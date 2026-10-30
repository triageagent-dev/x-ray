package memstore

import (
	"slices"
	"strings"
	"sync"
	"unicode/utf8"
)

// vocabIndex is a corpus-wide inverted index restricting rankAndLimit's
// per-candidate scan to items that can possibly match at least one query
// token, instead of visiting every active item on every search. Built
// incrementally at the same write-time call sites as kwIdx (trigramIndex):
// Upsert, Mirror, Supersede, Restore, and
// every delete/expire path.
//
// Words and joins are kept apart, not merged, because matchesKWTokenFields/
// fuzzyWordMatchFields apply DIFFERENT matching rules to a real word versus
// an adjacent-word-pair join (see candidateIDsForToken's doc comment for the
// full derivation) — collapsing them into one map would let a query token
// that only substring-matches a JOIN (never checked by the original code at
// all) incorrectly pull in that item.
type vocabIndex struct {
	Mu sync.RWMutex

	// ids/names intern item IDs to small integers so a posting list is a
	// sorted []uint32 (4 bytes per posting) instead of a map[string]struct{}
	// (a map header, bucket and 16-byte key per entry, which made every
	// one-item word or join cost ~200 bytes of map). An id keeps its interned
	// value for the life of the index, so names is bounded by the distinct ids
	// ever indexed, like trigramIndex.itemNames.
	ids   map[string]uint32
	names []string

	wordPostings map[string]*posting // real lower-cased word -> items containing it

	// joinPostings used to hold the adjacent-pair joins (word[i]+word[i+1], no
	// space) the same way. Nothing ever looked a join up by value, only by
	// length band, so the join posting lists live only in joinsByLen and a
	// join costs one map slot instead of two.

	// wordsByLen/joinsByLen are bucket VIEWS keyed by each entry's
	// utf8.RuneCountInString, computed exactly once when the entry is first
	// added to the vocabulary — never re-derived at query time. For words,
	// bucket[e] and wordPostings[e] are the SAME *posting, so there is no
	// duplicated id data, only a second key path into it:
	// candidateIDsForToken's fuzzy half walks only the length-banded buckets
	// here instead of every entry in the flat postings map with a
	// RuneCountInString call on each.
	wordsByLen map[int]map[string]*posting
	joinsByLen map[int]map[string]*posting

	byItemWords map[string][]string // item ID -> real words it currently contributes (for removal)
	byItemJoins map[string][]string // item ID -> joins it currently contributes (for removal)
}

// posting is the sorted set of interned item ids that contribute one
// vocabulary entry. It is shared by pointer between wordPostings and the
// length buckets.
type posting struct{ ids []uint32 }

func (p *posting) add(id uint32) {
	if i, found := slices.BinarySearch(p.ids, id); !found {
		p.ids = slices.Insert(p.ids, i, id)
	}
}

func (p *posting) remove(id uint32) {
	if i, found := slices.BinarySearch(p.ids, id); found {
		p.ids = slices.Delete(p.ids, i, i+1)
	}
}

func newVocabIndex() *vocabIndex {
	return &vocabIndex{
		ids:          make(map[string]uint32),
		wordPostings: make(map[string]*posting),
		wordsByLen:   make(map[int]map[string]*posting),
		joinsByLen:   make(map[int]map[string]*posting),
		byItemWords:  make(map[string][]string),
		byItemJoins:  make(map[string][]string),
	}
}

// internLocked returns id's interned value, minting one on first sight.
func (vi *vocabIndex) internLocked(id string) uint32 {
	if n, ok := vi.ids[id]; ok {
		return n
	}
	n := uint32(len(vi.names))
	vi.ids[id] = n
	vi.names = append(vi.names, id)
	return n
}

// compact drops the capacity slack the posting lists collected while they
// grew by insertion. A bulk build (Restore) calls it once at the end.
func (vi *vocabIndex) compact() {
	vi.Mu.Lock()
	defer vi.Mu.Unlock()
	for _, p := range vi.wordPostings {
		p.ids = slices.Clip(p.ids)
	}
	for _, bucket := range vi.joinsByLen {
		for _, p := range bucket {
			p.ids = slices.Clip(p.ids)
		}
	}
}

// vocabWordsForItem returns it's contribution to the vocabulary: its
// individual lower-cased title+content words (lowerFieldsFor — the exact
// same tokenization softMatchCoverageFields itself consumes, cached or
// freshly computed) as words, and each pair of ADJACENT words within title
// (separately within content) joined with no space as joins — mirroring
// fuzzyWordMatchFields' own adjacent-pair-join check exactly (word[i]+
// word[i+1], never crossing from title into content, never wrapping past
// the end of a field list). Both title and content are covered because
// matchesKWTokenFields checks title OR content — the vocabulary
// intentionally doesn't distinguish which, since that distinction is
// re-derived later by the real scoring functions on the surviving
// candidates.
func vocabWordsForItem(it Item) (words []string, joins []string) {
	_, ntf, _, ncf := lowerFieldsFor(it)
	words = make([]string, 0, len(ntf.words)+len(ncf.words))
	words = append(words, ntf.words...)
	words = append(words, ncf.words...)

	appendJoins := func(wt wordTokens) {
		for i := 0; i+1 < len(wt.words); i++ {
			joins = append(joins, wt.words[i]+wt.words[i+1])
		}
	}
	appendJoins(ntf)
	appendJoins(ncf)
	return words, joins
}

// index (re)computes id's contribution to the vocabulary from words/joins,
// replacing whatever it previously contributed. Callers pass
// vocabWordsForItem(it)'s two return values directly.
func (vi *vocabIndex) Index(id string, words, joins []string) {
	vi.Mu.Lock()
	defer vi.Mu.Unlock()
	vi.removeLocked(id)

	n := vi.internLocked(id)
	vi.byItemWords[id] = indexDedup(vi.wordPostings, vi.wordsByLen, n, words)
	vi.byItemJoins[id] = indexDedup(nil, vi.joinsByLen, n, joins)
}

// uniqueEntries returns entries without empties or repeats (an item's own
// word/content lists can repeat the same word many times), in a slice sized
// exactly to the result.
func uniqueEntries(entries []string) []string {
	uniq := make([]string, 0, len(entries))
	for _, e := range entries {
		if e != "" {
			uniq = append(uniq, e)
		}
	}
	slices.Sort(uniq)
	return slices.Clone(slices.Compact(uniq))
}

// postingFor returns e's posting list, from postings when the caller keeps
// one (words) and from the length bucket otherwise (joins).
func postingFor(postings map[string]*posting, byLen map[int]map[string]*posting, e string, rlen int) *posting {
	if postings != nil {
		return postings[e]
	}
	return byLen[rlen][e]
}

// indexDedup dedupes entries, records the interned item id n under each
// unique entry's posting list, and returns the deduped list for the caller's
// byItem*[id] bookkeeping.
//
// The first time an entry is ever seen, its rune length is computed once and
// its freshly-created posting is filed into byLen[rlen][entry] — and into
// postings[entry] when postings is non-nil — as the SAME *posting, so every
// later id added to (or removed from) that entry via either path is visible
// through both. An entry already present skips all of that: only its existing
// posting gets the id added.
func indexDedup(postings map[string]*posting, byLen map[int]map[string]*posting, n uint32, entries []string) []string {
	uniq := uniqueEntries(entries)
	for _, e := range uniq {
		rlen := utf8.RuneCountInString(e)
		p := postingFor(postings, byLen, e, rlen)
		if p == nil {
			p = &posting{}
			if postings != nil {
				postings[e] = p
			}
			bucket, ok := byLen[rlen]
			if !ok {
				bucket = make(map[string]*posting)
				byLen[rlen] = bucket
			}
			bucket[e] = p
		}
		p.add(n)
	}
	return uniq
}

// remove deletes id's contribution to the vocabulary (both words and joins).
func (vi *vocabIndex) Remove(id string) {
	vi.Mu.Lock()
	defer vi.Mu.Unlock()
	vi.removeLocked(id)
}

func (vi *vocabIndex) removeLocked(id string) {
	if n, ok := vi.ids[id]; ok {
		removeDedup(vi.wordPostings, vi.wordsByLen, n, vi.byItemWords[id])
		removeDedup(nil, vi.joinsByLen, n, vi.byItemJoins[id])
	}
	delete(vi.byItemWords, id)
	delete(vi.byItemJoins, id)
}

// removeDedup is indexDedup's inverse: for each entry the item no longer
// contributes, its id is dropped from that entry's posting, and if that was
// the last id (the entry has vanished from the vocabulary entirely), the
// entry is deleted from postings AND from its byLen bucket — recomputing
// the entry's rune length once, here, to find that bucket (this runs only
// for the entries one item is losing, never for the whole vocabulary, so it
// carries none of the per-query cost candidateIDsForToken used to pay).
func removeDedup(postings map[string]*posting, byLen map[int]map[string]*posting, n uint32, entries []string) {
	for _, e := range entries {
		rlen := utf8.RuneCountInString(e)
		p := postingFor(postings, byLen, e, rlen)
		if p == nil {
			continue
		}
		p.remove(n)
		if len(p.ids) == 0 {
			if postings != nil {
				delete(postings, e)
			}
			if bucket, ok := byLen[rlen]; ok {
				delete(bucket, e)
				if len(bucket) == 0 {
					delete(byLen, rlen)
				}
			}
		}
	}
}

// candidateIDsForToken resolves a single query token t against the
// vocabulary and returns the union of every matched entry's posting list —
// every item that COULD satisfy
// matchesKWTokenFields(nt, ntf, t) || matchesKWTokenFields(nc, ncf, t)
// for this token.
//
// Equivalence argument:
//
//  1. matchesKWTokenFields(h, hf, t) == strings.Contains(h, t) ||
//     fuzzyWordMatchFields(hf, t).
//
//  2. t is always whitespace-free: it comes from KwTokens, which is built by
//     strings.Fields (query tokenization). Because t contains no whitespace
//     byte, any span of h where t matches character-for-character cannot
//     include one of h's whitespace separators (t would need a whitespace
//     character at that offset too, and it has none) — so a Contains(h, t)
//     match always lies entirely within one of h's maximal whitespace-free
//     runs, i.e. one word of strings.Fields(h). Combined with the trivial
//     converse (t substring of a word W substring of h implies t substring
//     of h): strings.Contains(h, t) == ∃ w ∈ Fields(h), strings.Contains(w, t).
//     This is the wordPostings/Contains half below.
//
//  3. fuzzyWordMatchFields(hf, t), reproduced here:
//
//     tn := runes(t); tol := fuzzyTolerance(tn)
//     if tol == 0 { return false }                          // (*) short-circuits BOTH sub-checks below
//     for i, word := range hf {
//     if len-banded(word, tn, tol) && editDistance(t, word) <= tol { return true }
//     if i+1 < len(hf) {
//     join := word + hf[i+1]; jtol := fuzzyTolerance(len(join))
//     if len-banded(join, tn, jtol) && editDistance(t, join) <= jtol { return true }
//     }
//     }
//     return false
//
//     Two consequences that make wordPostings/joinPostings need DIFFERENT
//     rules, not one shared rule:
//     (a) the early return at (*) means NEITHER the single-word NOR the join
//     fuzzy check ever runs when the TOKEN's own tolerance is 0 (tn<=2) —
//     regardless of any word's or join's own length. A short token can
//     therefore NEVER fuzzy-match anything, join included.
//     (b) the join branch's tolerance (jtol) is scaled by the JOIN's OWN
//     length, not the token's — mirrored below via fuzzyTolerance(jn)
//     evaluated per join bucket length, not via the outer tol.
//     (c) — critically — there is NO Contains-style exact check on a join
//     anywhere in the original code. A join is only ever tested by bounded
//     edit distance. Applying strings.Contains to a join entry (as an
//     earlier version of this function did) manufactures matches the
//     original algorithm could never produce: caught live by
//     TestVocabIndex_CandidateIDs_MatchesPerItemScan/db, where "db"
//     (tol==0, so no fuzzy check of ANY kind should ever fire) was matching
//     items purely because "db" happened to appear as a substring straddling
//     two joined words — a match position that cannot exist in the original
//     nt/nc strings at all, since a real word boundary always has actual
//     whitespace between the two words (see point 2), and t contains none.
//
// So: wordPostings entries are tested by Contains(word, t) [always] OR
// length-banded editDistanceWithin(t, word, tol) [tol from the token, gated
// on tol>0] — reproducing point 2 and the single-word half of point 3.
// joinPostings entries are tested ONLY by length-banded
// editDistanceWithin(t, join, jtol) [jtol from the JOIN's own length, the
// whole check gated on the token's own tol>0 per (a)] — reproducing the join
// half of point 3, with no Contains check per (c).
//
// Resolving t against the corpus-wide DEDUPLICATED union of every item's
// words/joins (this function) and unioning the matched entries' posting
// lists therefore yields EXACTLY the same item set as checking every item
// individually — see TestVocabIndex_CandidateIDs_MatchesPerItemScan for the
// exhaustive cross-check against the brute-force per-item reference.
//
// A CPU profile of the lexical search phase attributed 45% of it to this
// function, almost entirely RuneCountInString calls made once per vocabulary
// entry, on every query token, against the ENTIRE wordPostings/joinPostings
// maps (see BenchmarkVocabIndex_CandidateIDsForTokens). fuzzyTolerance is 0
// for a rune length <=2 and 1 otherwise (see fuzzyTolerance), so the length
// band a fuzzy check can ever admit is at most [tn-1, tn+1] — a query token
// can only fuzzy-match entries whose rune length is within 1 of its own.
// wordsByLen/joinsByLen let the fuzzy half of this function visit exactly
// those length buckets (2-3 map lookups for words; every existing join
// length, checked against its own fuzzyTolerance, for joins — jtol depends
// on the JOIN's length, not tn, so unlike the word band it isn't a fixed
// [tn-1, tn+1] range) instead of the whole vocabulary, with every entry's
// rune length already known from the bucket key it lives under rather than
// recomputed here. The Contains half is unaffected — a substring match has
// no length relationship to t beyond word being at least as long, which is
// a byte-length (not rune-count) precondition strings.Contains already
// implies; checking it explicitly with len() before calling Contains skips
// the call for words shorter than t without touching rune counts at all.
// None of this changes which entries match, only how many are visited to
// find out — the equivalence argument above is unchanged.
//
// Because this is used purely as a pre-filter (rankAndLimit still runs the
// exact, unmodified softMatchCoverageFields/lexScoreFields on every
// surviving candidate), any slack in the equivalence could only ever cause
// OVER-inclusion (a performance cost, corrected by the real scoring pass),
// never a dropped result — but the argument above establishes it is exact,
// not merely safe, and the exhaustive test enforces exactness, not just
// safety, so a regression here is caught even though it could never produce
// a wrong final answer.
func (vi *vocabIndex) CandidateIDsForToken(t string) map[string]struct{} {
	tn := utf8.RuneCountInString(t)
	tol := fuzzyTolerance(tn)
	out := make(map[string]struct{})

	vi.Mu.RLock()
	defer vi.Mu.RUnlock()

	add := func(p *posting) {
		for _, n := range p.ids {
			out[vi.names[n]] = struct{}{}
		}
	}

	for word, p := range vi.wordPostings {
		if len(word) < len(t) { // strings.Contains(word, t)'s own byte-length precondition — see doc comment
			continue
		}
		if !strings.Contains(word, t) {
			continue
		}
		add(p)
	}

	if tol > 0 { // fuzzyWordMatchFields' outer short-circuit — see (a) above
		for wn := tn - tol; wn <= tn+tol; wn++ {
			bucket, ok := vi.wordsByLen[wn]
			if !ok {
				continue
			}
			for word, p := range bucket {
				if !editDistanceWithin(t, word, tol) {
					continue
				}
				add(p)
			}
		}

		for jn, bucket := range vi.joinsByLen {
			jtol := fuzzyTolerance(jn)
			if absDiff(jn, tn) > jtol {
				continue
			}
			for join, p := range bucket {
				if !editDistanceWithin(t, join, jtol) {
					continue
				}
				add(p)
			}
		}
	}

	return out
}

// wordDocFreq returns the number of active items whose vocabulary contribution
// includes the exact lower-cased word t: the literal, word-level document
// frequency that the distinctiveness weighting (tokenWeights, idfWeight) reads
// at query time. It is not a new index: wordPostings already holds this posting
// list for candidateIDsForToken, so this is a plain len() over it. It returns 0
// for a token no active item has as a literal word, the usual case for
// conversational filler against a corpus of templated text; idfWeight
// deliberately does not treat that as "maximally rare".
func (vi *vocabIndex) wordDocFreq(t string) int {
	vi.Mu.RLock()
	defer vi.Mu.RUnlock()
	if p := vi.wordPostings[t]; p != nil {
		return len(p.ids)
	}
	return 0
}

// candidateIDsForTokens is the union of candidateIDsForToken across every
// token in toks — the safe restriction keywordSearchWithTrigramHit applies
// to its candidate set before calling rankAndLimit. Any item outside this
// union scores matched=0 against EVERY token (candidateIDsForToken is exact,
// per its own doc comment, so "outside the union" means "matches no token at
// all"); softMatchCoverage's coverage requirement (need) is always >= 1 for
// a non-empty token list (1-2 tokens: need == len(toks) >= 1; 3+ tokens:
// need == len(toks)-1 >= 2), so matched=0 always fails the gate regardless
// of query length. Excluding such items from the scan therefore changes zero
// results; TestLexicalSearch_GoldenEquivalence_VocabRestricted checks it.
//
// Returns nil for an empty token list (caller's "no tokens — everything
// passes" branch in rankAndLimit already handles that case without needing
// a restriction).
func (vi *vocabIndex) CandidateIDsForTokens(toks []string) map[string]struct{} {
	if len(toks) == 0 {
		return nil
	}
	out := make(map[string]struct{})
	for _, t := range toks {
		for id := range vi.CandidateIDsForToken(t) {
			out[id] = struct{}{}
		}
	}
	return out
}
