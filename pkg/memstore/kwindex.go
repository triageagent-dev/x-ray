package memstore

import (
	"slices"
	"strings"
	"sync"
)

// trigramIndex is an in-memory inverted index that maps 3-char grams of
// individual words to item IDs. It accelerates keyword search by letting a
// query token match a haystack word when they share enough trigrams — catching
// no-space variants ("WidgetREC" → "Widget REC") and voice mishears.
//
// Each item is indexed per-word, not as a concatenated blob, so "goroutine"
// does not accidentally match "coroutines" just because the letters overlap.
//
// Grams and item IDs are interned to small integers (gramIDs/itemIDs) instead
// of being used as map keys directly: a Go map keyed by string pays a header,
// bucket, and per-entry allocation for every distinct key, which dominates
// resident memory when the corpus has far more distinct (word, item) pairs
// than distinct bytes of source text. Interning is exact, not a hash — with
// tens of thousands of distinct grams a 32-bit hash has a non-negligible
// birthday-collision probability, and a collision would silently merge two
// different grams' postings and change search results.
//
// Per-item data lives in two flat structures keyed by the interned integer
// item ID instead of one map entry per word:
//   - wordGramFlat/wordOffsets hold every word's gram IDs concatenated, each
//     word's segment SORTED and DEDUPED (word i occupies
//     wordGramFlat[id][wordOffsets[id][i]:wordOffsets[id][i+1]]) — a set,
//     same as the per-word grams it replaces, just represented as a sorted
//     slice with binary-search membership instead of a map.
//   - gramPosting, indexed by gram ID, holds every (item, word) pair
//     containing that gram as a SORTED slice of packed
//     uint64(itemIdx)<<32|wordIdx values (see wordRef), maintained
//     incrementally at the same write-time call sites (index(), via
//     clearItemPostingsLocked/removeLocked) so a query never walks an item's
//     word list — it looks up its own grams' posting lists instead (see
//     bestOverlapInto). Because wordRef packs itemIdx into the high bits and
//     the list is sorted, one item's entries always form a single contiguous
//     range within a gram's posting list — clearItemPostingsLocked and
//     index() update that whole range with one slices.Delete/slices.Insert
//     per distinct gram touched, never one per posting.
//
// An item keeps its interned index for the rest of the process's life once
// minted, across both re-indexing and remove(): index() reuses the same
// itemIdx for an id it has already interned, and removeLocked clears the
// item's postings/wordGramFlat/wordOffsets but leaves the itemIDs entry (and
// itemNames[idx]) in place, so a remove()-then-index() cycle on the same id
// — Expire followed by a later upsert, or an inactive item that becomes
// active again — reuses the
// existing slot instead of minting a fresh one. itemIDs/itemNames therefore
// grow with the number of DISTINCT ids ever indexed in this process, not
// with how many index()/remove() calls happen. A removed item scores zero
// regardless: its postings are gone, so every scoring path (which reads only
// gramPosting and wordGramFlat, never itemIDs presence) sees no overlap.
type trigramIndex struct {
	Mu sync.RWMutex

	gramIDs    map[string]uint32 // gram -> interned ID, exact (never hashed)
	nextGramID uint32

	itemIDs   map[string]uint32 // item ID -> interned index; entry kept across remove() so a later re-index reuses the slot
	itemNames []string          // interned index -> item ID; bounded by distinct ids ever indexed in this process, not by live item count

	// wordGramFlat[itemIdx] concatenates every word's (sorted, deduped) gram
	// IDs; wordOffsets[itemIdx][i]:wordOffsets[itemIdx][i+1] bounds word i's
	// segment. Computed once at index() time and consulted (never
	// recomputed) by every scoring path below.
	wordGramFlat map[uint32][]uint32
	wordOffsets  map[uint32][]int32

	// gramPosting[gramID] is the sorted list of every (item, word) pair
	// containing that gram, packed via wordRef. Grown by one nil entry each
	// time internGramLocked mints a new gram ID, so gramPosting is always
	// indexable by every ID gramIDs can return.
	gramPosting [][]uint64

	// groupHead/groupTail, indexed by gram ID like gramPosting, are scratch
	// used only inside index() to group the item currently being indexed's
	// occurrences of one gram into a single ascending chain — index() reads
	// wordGramFlat and gramPosting alike, so no unrelated logic notices this
	// mid-call state, and index() resets every entry it touches back to -1
	// before returning. Grown by one -1 entry alongside gramPosting so both
	// stay indexable by every ID gramIDs can return. Kept on the struct
	// (not allocated fresh per call) so their size tracks distinct grams
	// interned, not the current item's word count.
	groupHead []int32
	groupTail []int32
}

// wordRef packs one (item, word) reference into gramPosting's element type.
// Injective for realistic item/word counts (well under 2^32 each).
func wordRef(itemIdx, wordIdx uint32) uint64 {
	return uint64(itemIdx)<<32 | uint64(wordIdx)
}

// wordRefItem extracts the item index a wordRef was packed from.
func wordRefItem(ref uint64) uint32 {
	return uint32(ref >> 32)
}

func NewTrigramIndex() *trigramIndex {
	return &trigramIndex{
		gramIDs:      make(map[string]uint32),
		itemIDs:      make(map[string]uint32),
		wordGramFlat: make(map[uint32][]uint32),
		wordOffsets:  make(map[uint32][]int32),
	}
}

// trigrams returns all 3-char substrings of s (or s itself if shorter).
func trigrams(s string) []string {
	r := []rune(s)
	if len(r) < 3 {
		return []string{s}
	}
	out := make([]string, 0, len(r)-2)
	for i := 0; i <= len(r)-3; i++ {
		out = append(out, string(r[i:i+3]))
	}
	return out
}

// internItemLocked returns id's interned index, minting one (and recording
// the reverse mapping) only if id has never been seen. A previously-interned
// id keeps the same index across every later call, so re-indexing an
// existing id never grows itemNames. Caller must hold idx.Mu (write lock).
func (idx *trigramIndex) internItemLocked(id string) uint32 {
	if itemIdx, ok := idx.itemIDs[id]; ok {
		return itemIdx
	}
	itemIdx := uint32(len(idx.itemNames))
	idx.itemIDs[id] = itemIdx
	idx.itemNames = append(idx.itemNames, id)
	return itemIdx
}

// internGramLocked returns g's interned ID, minting one (and growing
// gramPosting to match) if g has never been seen. Caller must hold idx.Mu
// (write lock). Only the write path (index()) calls this — query-time
// lookups read idx.gramIDs directly, so an unfamiliar query gram can never
// grow the table.
func (idx *trigramIndex) internGramLocked(g string) uint32 {
	if gid, ok := idx.gramIDs[g]; ok {
		return gid
	}
	gid := idx.nextGramID
	idx.nextGramID++
	idx.gramIDs[g] = gid
	idx.gramPosting = append(idx.gramPosting, nil)
	idx.groupHead = append(idx.groupHead, -1)
	idx.groupTail = append(idx.groupTail, -1)
	return gid
}

// itemPostingRangeLocked returns the [lo, hi) bounds of itemIdx's own refs
// within gid's posting list. Every ref for one item sorts contiguously,
// because wordRef packs itemIdx into a uint64's high 32 bits and the list is
// sorted, so the item's whole run is bounded by the positions of
// wordRef(itemIdx, 0) and the first ref belonging to itemIdx+1 — the latter
// computed in uint64 (not uint32) so itemIdx == math.MaxUint32 cannot wrap.
// Caller must hold idx.Mu.
func (idx *trigramIndex) itemPostingRangeLocked(gid, itemIdx uint32) (lo, hi int) {
	list := idx.gramPosting[gid]
	lo, _ = slices.BinarySearch(list, wordRef(itemIdx, 0))
	hi, _ = slices.BinarySearch(list, (uint64(itemIdx)+1)<<32)
	return lo, hi
}

// index adds or replaces an item. Words from title and content are indexed
// individually so long-word trigrams only match the same word, not adjacent
// words. Re-indexing an id already present reuses its interned index — see
// the struct doc comment.
//
// Posting-list updates are batched per gram, not per (gram, word): one
// slices.Insert per distinct gram touched, instead of one per posting, turns
// what would otherwise be one memmove per posting into one per distinct
// gram — the difference matters because a common gram's posting list can run
// to hundreds of KB, and this runs under the store's write lock on every
// upsert. The grouping itself must stay
// cheap on a bulk build too, where every item is freshly interned and its
// refs already land at each gram's posting-list tail (cheap even
// ungrouped): a comparison sort of the item's (gram, word) occurrences costs
// O(total log total) and was measured to cost a full 1,300-item bulk build
// noticeably more than one memmove per posting saves; a map keyed by gram ID
// costs even more, one bucket-table allocation per index() call on top of
// the same hashing. groupHead/groupTail (struct fields, grown alongside
// gramPosting) instead chain each gram's occurrences into a singly linked
// list in O(total) — no sort, no hashing, no per-gram allocation — with
// insertion order preserving ascending word order for free, since words are
// visited in increasing i and each occurrence is appended to its own gram's
// chain tail.
func (idx *trigramIndex) Index(id, title, content string) {
	words := lexFields(strings.ToLower(title + " " + content))

	idx.Mu.Lock()
	defer idx.Mu.Unlock()

	itemIdx := idx.internItemLocked(id)
	idx.clearItemPostingsLocked(itemIdx)

	perWord := make([][]uint32, len(words))
	total := 0
	for i, w := range words {
		g := trigrams(w)
		gids := make([]uint32, len(g))
		for j, tg := range g {
			gids[j] = idx.internGramLocked(tg)
		}
		slices.Sort(gids)
		gids = slices.Compact(gids)
		perWord[i] = gids
		total += len(gids)
	}

	offsets := make([]int32, len(words)+1)
	flat := make([]uint32, 0, total)
	// chainNext/chainWord are per-occurrence scratch (index k = the k-th
	// (word, gram) occurrence in visitation order): chainNext[k] is the
	// next occurrence of the SAME gram, or -1; chainWord[k] is the word
	// index that occurrence came from. touchedGids lists each gram the
	// first time one of its occurrences is seen, so only grams this item
	// actually touches get walked (and groupHead/groupTail reset) below.
	chainNext := make([]int32, 0, total)
	chainWord := make([]int32, 0, total)
	touchedGids := make([]uint32, 0, total)
	for i, gids := range perWord {
		offsets[i] = int32(len(flat))
		flat = append(flat, gids...)
		for _, gid := range gids {
			k := int32(len(chainWord))
			chainWord = append(chainWord, int32(i))
			chainNext = append(chainNext, -1)
			if idx.groupHead[gid] < 0 {
				idx.groupHead[gid] = k
				touchedGids = append(touchedGids, gid)
			} else {
				chainNext[idx.groupTail[gid]] = k
			}
			idx.groupTail[gid] = k
		}
	}
	offsets[len(words)] = int32(len(flat))

	// refs holds every touched gram's batch back to back (bounds written as
	// each chain is walked), so the slices.Insert below reuses a slice of
	// this one array as its variadic argument instead of allocating one
	// batch slice per gram. clearItemPostingsLocked above already removed
	// every trace of itemIdx from gramPosting, so the insertion point for
	// wordRef(itemIdx, 0) is the correct insertion point for the whole
	// batch — nothing of this item can be interleaved with it.
	refs := make([]uint64, total)
	pos := 0
	for _, gid := range touchedGids {
		start := pos
		for k := idx.groupHead[gid]; k != -1; k = chainNext[k] {
			refs[pos] = wordRef(itemIdx, uint32(chainWord[k]))
			pos++
		}
		idx.groupHead[gid] = -1
		idx.groupTail[gid] = -1

		list := idx.gramPosting[gid]
		lo, _ := slices.BinarySearch(list, wordRef(itemIdx, 0))
		idx.gramPosting[gid] = slices.Insert(list, lo, refs[start:pos]...)
	}

	idx.wordGramFlat[itemIdx] = flat
	idx.wordOffsets[itemIdx] = offsets
}

// shrunk returns list re-allocated to its length when it carries spare
// capacity (slices.Clip would only hide the slack, not free it).
func shrunk[E any](list []E) []E {
	if cap(list) == len(list) {
		return list
	}
	return slices.Clone(list)
}

// compact drops the capacity slack the posting lists collected while they
// grew by insertion (slices.Insert grows like append, up to twice the length).
// A bulk build (Restore) calls it once at the end.
func (idx *trigramIndex) compact() {
	idx.Mu.Lock()
	defer idx.Mu.Unlock()
	for gid, list := range idx.gramPosting {
		idx.gramPosting[gid] = shrunk(list)
	}
}

// remove deletes an item from the index.
func (idx *trigramIndex) Remove(id string) {
	idx.Mu.Lock()
	defer idx.Mu.Unlock()
	idx.removeLocked(id)
}

// clearItemPostingsLocked drops itemIdx's posting-list entries and its
// wordGramFlat/wordOffsets segments, without touching itemIDs/itemNames —
// used both by removeLocked (id is genuinely gone) and by index() when
// replacing an already-interned item's content under the same itemIdx.
//
// Deletes one contiguous range per distinct gram itemIdx touches (via
// itemPostingRangeLocked), not one element per (gram, word) posting: flat
// can repeat a gram ID across different words' segments, so the distinct
// gram set is flat deduped (copy, sort, compact — flat/offsets themselves
// must stay as index() left them, since bulk build relies on offsets
// bounding exactly one word each). One slices.Delete per distinct gram
// replaces what would otherwise be one memmove per posting.
//
// Caller must hold idx.Mu (write lock). No-op if itemIdx was never indexed.
func (idx *trigramIndex) clearItemPostingsLocked(itemIdx uint32) {
	flat, ok := idx.wordGramFlat[itemIdx]
	if !ok {
		return
	}
	gids := slices.Clone(flat)
	slices.Sort(gids)
	gids = slices.Compact(gids)
	for _, gid := range gids {
		lo, hi := idx.itemPostingRangeLocked(gid, itemIdx)
		if lo < hi {
			idx.gramPosting[gid] = slices.Delete(idx.gramPosting[gid], lo, hi)
		}
	}
	delete(idx.wordGramFlat, itemIdx)
	delete(idx.wordOffsets, itemIdx)
}

// removeLocked clears id's contribution to the index via
// clearItemPostingsLocked. It deliberately leaves id's itemIDs entry in
// place — a re-index of the same id later (index()) reuses the interned slot
// via internItemLocked rather than minting a new one, so a remove()+index()
// cycle on one id costs no itemNames growth (see the struct doc comment and
// TestTrigramIndex_RemoveReaddReusesSlot). id scores zero in the meantime
// regardless, since every scoring path reads gramPosting/wordGramFlat —
// both cleared — not itemIDs presence. Caller must hold idx.Mu (write
// lock). No-op if id was never indexed.
func (idx *trigramIndex) removeLocked(id string) {
	itemIdx, ok := idx.itemIDs[id]
	if !ok {
		return
	}
	idx.clearItemPostingsLocked(itemIdx)
}

// tokOverlapLocked returns the best trigram-overlap fraction (0..1) for tok
// against any single word in the item's indexed word list. Caller must hold
// idx.Mu.
//
// Retained as a frozen reference implementation (O(item's own words) per
// call, no posting lists) — still exact, just not on the hot path. Used
// directly by kwindex_test.go and as the equivalence oracle the golden
// test's reference ranking calls through itemScoreGrams below.
// Production scoring goes through queryOverlapScores/tokenBestOverlapLocked
// instead (see rankAndLimit).
func (idx *trigramIndex) tokOverlapLocked(id, tok string) float64 {
	return idx.tokOverlapGramsLocked(id, trigrams(tok))
}

// tokOverlapGramsLocked is tokOverlapLocked with the token's trigrams already
// computed, so a caller scoring many items against the same token pays for
// trigrams(tok) once instead of once per item. Looks up each query gram's
// interned ID once (never minting), then binary-searches each word's sorted
// gram segment — the same set-membership test a map would give, without a
// map per word. Caller must hold idx.Mu.
func (idx *trigramIndex) tokOverlapGramsLocked(id string, tgrams []string) float64 {
	if len(tgrams) == 0 {
		return 0
	}
	itemIdx, ok := idx.itemIDs[id]
	if !ok {
		return 0
	}

	// A gram absent from gramIDs cannot appear in any word's sorted
	// segment, so it contributes zero occurrences below.
	gids := make([]uint32, len(tgrams))
	known := make([]bool, len(tgrams))
	for i, g := range tgrams {
		if gid, ok := idx.gramIDs[g]; ok {
			gids[i] = gid
			known[i] = true
		}
	}

	flat := idx.wordGramFlat[itemIdx]
	offsets := idx.wordOffsets[itemIdx]
	best := 0.0
	for w := 0; w+1 < len(offsets); w++ {
		seg := flat[offsets[w]:offsets[w+1]]
		n := 0
		for i, gid := range gids {
			if !known[i] {
				continue
			}
			if _, found := slices.BinarySearch(seg, gid); found {
				n++
			}
		}
		if frac := float64(n) / float64(len(tgrams)); frac > best {
			best = frac
			if best >= 1.0 {
				break
			}
		}
	}
	return best
}

// itemScore returns the mean per-token trigram overlap (0..1) for the item
// across all query tokens — the continuous relevance signal used by lexScore.
// Acquires the read lock once for the whole token set.
//
// Retained as a single-item, single-query convenience API for tests and the
// golden-equivalence reference path; the production hot path (rankAndLimit)
// computes the same values for every candidate at once via
// queryOverlapScores instead of calling this once per candidate — see that
// function's doc comment for why the difference matters.
func (idx *trigramIndex) ItemScore(id string, toks []string) float64 {
	if len(toks) == 0 {
		return 0
	}
	return idx.ItemScoreGrams(id, TokenGrams(toks))
}

// TokenGrams precomputes each token's trigrams once for a whole query, so a
// scan over N candidate items does not recompute them N times. Callers that
// score many items against one token set should hoist this out of their loop
// (rankAndLimit does, via queryOverlapScores).
func TokenGrams(toks []string) [][]string {
	out := make([][]string, len(toks))
	for i, t := range toks {
		out[i] = trigrams(t)
	}
	return out
}

// itemScoreGrams is itemScore with the query's token trigrams already computed.
func (idx *trigramIndex) ItemScoreGrams(id string, tokGrams [][]string) float64 {
	if len(tokGrams) == 0 {
		return 0
	}
	idx.Mu.RLock()
	defer idx.Mu.RUnlock()
	sum := 0.0
	for _, tg := range tokGrams {
		sum += idx.tokOverlapGramsLocked(id, tg)
	}
	return sum / float64(len(tokGrams))
}

// bestOverlapInto is the posting-list-merge equivalent of calling
// tokOverlapGramsLocked once per candidate item for a single query token's
// trigrams: it merges gramPosting's entries for exactly the grams in tgrams
// into the caller-provided (already-cleared) `counts` scratch map, then
// reduces that into a per-item best-per-word overlap fraction, ADDED into
// the caller-provided `dst` map (via "if frac > dst[id]", so dst can be
// pre-seeded — queryOverlapScores relies on that to fold each token's
// contribution into a running best before moving to the next token). Cost is
// proportional to how many (item, word) pairs actually share one of tgrams'
// grams, never to corpus or vocabulary size. An item with zero overlap for
// this token simply gets no entry touched in dst.
//
// `counts` and `dst` are caller-owned and reused across every token of a
// query (queryOverlapScores/search clear() and refill them once per token)
// rather than being allocated fresh here: clear() empties a map without
// releasing its bucket storage, so a query's later tokens reuse the same
// backing allocation instead of growing a fresh one from zero.
//
// Equivalence with tokOverlapGramsLocked, for a fixed id (holding dst's
// pre-seeded value fixed — this only concerns the max-update dst receives
// from THIS call):
//
//  1. tokOverlapGramsLocked(id, tgrams) takes, over every word w of id, n_w
//     := |{occurrences g in tgrams : g is in w's sorted gram segment}|
//     (counted WITH multiplicity — a repeated gram in tgrams, e.g. from a
//     token like "aaaa", can increment n_w more than once for the same
//     word), and returns max_w(n_w / len(tgrams)).
//
//  2. Here, counts[wordRef(itemIdx,w)] increments once per occurrence g in
//     tgrams such that gramPosting[gramIDs[g]] contains wordRef(itemIdx,w)
//     — and it does exactly when g is in w's sorted gram segment, because
//     index()/clearItemPostingsLocked keep gramPosting and wordGramFlat in
//     lockstep at every index()/removeLocked() call, batched per gram rather
//     than per posting but touching the identical set of refs either way. So
//     counts[wordRef(itemIdx,w)] == n_w exactly, occurrence-for-occurrence,
//     for every word w id has.
//
//  3. dst[id] after this call is max(dst[id]_before, max over the SAME
//     value set {n_w/len(tgrams) : w}) via repeated "if frac > dst[id]"
//     updates — a plain max reduction, whose result does not depend on
//     visitation order (unlike a sum, there is no order-sensitive rounding:
//     max over an identical multiset of float64 values, computed by the
//     identical n_w/len(tgrams) division, yields the identical bit pattern
//     regardless of which order the words/candidates are visited in). So
//     when dst starts this call at its zero value (single-token callers,
//     search()'s first token), dst[id] == tokOverlapGramsLocked's return
//     value, bit for bit.
//
// A query gram absent from gramIDs is looked up (never minted) and skipped:
// it cannot be in any word's segment, so treating it as zero occurrences
// matches what a present-but-empty posting list would do.
//
// See TestTrigramIndex_QueryOverlapScores_MatchesItemScore and
// TestTrigramIndex_QueryOverlapScores_MatchesGoldenCorpus for the exhaustive cross-checks, and
// BenchmarkTrigramItemScore_Search_Scale for the cost comparison. Caller
// must hold idx.Mu (read lock); this only reads gramIDs and gramPosting.
func (idx *trigramIndex) bestOverlapInto(tgrams []string, counts map[uint64]int32, dst map[string]float64) {
	for _, g := range tgrams {
		gid, ok := idx.gramIDs[g]
		if !ok {
			continue
		}
		for _, ref := range idx.gramPosting[gid] {
			counts[ref]++
		}
	}
	if len(counts) == 0 {
		return
	}
	n := float64(len(tgrams))
	for ref, c := range counts {
		id := idx.itemNames[wordRefItem(ref)]
		if frac := float64(c) / n; frac > dst[id] {
			dst[id] = frac
		}
	}
}

// queryOverlapScores is itemScoreGrams computed for every candidate item AT
// ONCE instead of once per candidate — the production replacement for
// rankAndLimit's old per-candidate `idx.ItemScoreGrams(it.ID, tokGrams)`
// call. Calling itemScoreGrams per candidate meant every surviving candidate
// re-walked its own full word list against every query token (the O(corpus)
// walker this round removes); calling this once per query and then doing a
// plain map lookup per candidate turns that into ONE posting-list-merge pass
// (bestOverlapInto, per token, over two scratch maps reused — not
// reallocated — across every token) whose cost is bounded by how much of
// the index the query's own grams actually touch, plus a cheap per-item
// lookup thereafter.
//
// Returns, for every item with nonzero overlap on at least one token, the
// same mean-overlap value itemScoreGrams(id, tokGrams) would return for that
// id — an item absent from the result (or the result being nil) means 0,
// exactly like itemScoreGrams's zero-value behavior for an unindexed/
// non-overlapping id, and a nil-map lookup in Go already returns the zero
// value, so callers do not need a nil check before indexing the result.
//
// Lock discipline: idx.Mu is held only across the per-token posting-list
// merges (the map reads gramPosting requires); the final division by
// len(tokGrams) happens on the caller-owned `sums` map after RUnlock, since
// it touches no shared state.
func (idx *trigramIndex) QueryOverlapScores(tokGrams [][]string) map[string]float64 {
	if len(tokGrams) == 0 {
		return nil
	}
	sums := make(map[string]float64)

	idx.Mu.RLock()
	counts := make(map[uint64]int32)
	tokBest := make(map[string]float64)
	for _, tg := range tokGrams {
		if len(tg) == 0 {
			continue
		}
		clear(counts)
		clear(tokBest)
		idx.bestOverlapInto(tg, counts, tokBest)
		for id, frac := range tokBest {
			sums[id] += frac
		}
	}
	idx.Mu.RUnlock()

	n := float64(len(tokGrams))
	for id := range sums {
		sums[id] /= n
	}
	return sums
}

// tokThreshold returns the overlap fraction a token must clear to count as a
// trigram match, scaled by token length:
//   - short (≤4 chars, 1-2 trigrams): 1.0 — must match exactly (token IS its trigram)
//   - medium (5-6 chars, 3-4 trigrams): 0.75 — catches "widges"→"widget" (3/4), rejects distant words
//   - long (≥7 chars, 5+ trigrams): 0.90 — tight; prevents "goroutine"↔"coroutines" false positives
func tokThreshold(nTrigrams int) float64 {
	switch {
	case nTrigrams <= 2:
		return 1.0
	case nTrigrams <= 4:
		return 0.75
	default:
		return 0.90
	}
}

// tokMatchesItem returns true if tok clears its length-scaled overlap
// threshold against any single word in the item — the boolean gate built on
// tokOverlapLocked.
//
// Retained for direct tests and as a readable reference predicate; search()
// below no longer calls it — it re-derives trigrams(tok) and tokThreshold on
// every (id, tok) pair. Caller must hold idx.Mu.
func (idx *trigramIndex) TokMatchesItem(id, tok string) bool {
	return idx.tokOverlapLocked(id, tok) >= tokThreshold(len(trigrams(tok)))
}

// search returns item IDs where every query token trigram-matches at least
// one word in the item. Used as a pre-filter before exact/fuzzy matching, and
// its non-empty-result-that-contributed status feeds the KWTrigramHits
// counter (see rankAndLimit).
//
// Built on tokenBestOverlapLocked rather than an item-level posting seeded
// from the first token alone plus a per-(id,token) tokMatchesItem filter.
// Equivalence: a deliberately loose prefilter seeded from "item shares ANY
// trigram with the first token, in ANY word" is looser than the per-token
// threshold check every token (including the first) then has to pass anyway.
// Since every tokThreshold value is > 0, passing a token's threshold always
// implies sharing at least one gram with it, so such a seed step could never
// exclude an id the threshold loop would otherwise keep: the final set is
// always exactly "passes every token's threshold check", regardless of how
// the candidate pool is seeded. Computing that directly — intersect, token
// by token, the per-token best-overlap maps against their thresholds —
// yields the identical result set without visiting anything that shares zero
// grams with any token. See TestTrigramIndex_Search_MatchesTokMatchesItemBruteForce
// and TestTrigramIndex_Search_MatchesGoldenCorpus.
func (idx *trigramIndex) Search(query string) []string {
	toks := KwTokens(NormalizeQuery(query))
	if len(toks) == 0 {
		return nil
	}

	tokGrams := TokenGrams(toks)
	thresholds := make([]float64, len(tokGrams))
	for i, tg := range tokGrams {
		thresholds[i] = tokThreshold(len(tg))
	}

	idx.Mu.RLock()
	defer idx.Mu.RUnlock()

	// counts/best are scratch maps reused (clear()ed, not reallocated) across
	// every token — same discipline as queryOverlapScores, see
	// bestOverlapInto's doc comment for why that matters.
	counts := make(map[uint64]int32)
	best := make(map[string]float64)

	idx.bestOverlapInto(tokGrams[0], counts, best)
	candidates := make(map[string]struct{}, len(best))
	for id, frac := range best {
		if frac >= thresholds[0] {
			candidates[id] = struct{}{}
		}
	}

	for i := 1; i < len(tokGrams) && len(candidates) > 0; i++ {
		clear(counts)
		clear(best)
		idx.bestOverlapInto(tokGrams[i], counts, best)
		for id := range candidates {
			if best[id] < thresholds[i] { // missing key reads as 0, always < threshold
				delete(candidates, id)
			}
		}
	}

	out := make([]string, 0, len(candidates))
	for id := range candidates {
		out = append(out, id)
	}
	return out
}

// HasItem reports whether id currently has live postings in the index. An
// item's entry in itemIDs is deliberately kept across remove(), so presence
// there is not enough: the postings are the signal.
func (idx *trigramIndex) HasItem(id string) bool {
	idx.Mu.RLock()
	defer idx.Mu.RUnlock()
	itemIdx, ok := idx.itemIDs[id]
	if !ok {
		return false
	}
	_, ok = idx.wordGramFlat[itemIdx]
	return ok
}
