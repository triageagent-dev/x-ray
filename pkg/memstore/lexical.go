package memstore

import (
	"math"
	"sort"
	"strings"
	"unicode/utf8"
)

// Lexical search machinery: query normalisation, fuzzy and trigram matching,
// distinctiveness-weighted scoring, and rank fusion. It runs against the
// store's resident items.

// NormalizeQuery lowercases, splits CamelCase and PascalCase boundaries
// ("FooBAR" becomes "foo bar"), and turns separators and punctuation into single
// spaces, so "what is payment-api?" reduces to "what is payment api". Without
// the punctuation step a token keeps the mark glued to it ("api?") and misses an
// exact match against "api".
func NormalizeQuery(q string) string {
	// Insert a space before an uppercase letter that follows a lowercase one.
	var sb strings.Builder
	runes := []rune(q)
	for i, r := range runes {
		if i > 0 && r >= 'A' && r <= 'Z' {
			prev := runes[i-1]
			if prev >= 'a' && prev <= 'z' {
				sb.WriteByte(' ')
			}
		}
		sb.WriteRune(r)
	}
	q = strings.ToLower(sb.String())
	return strings.Join(lexFields(q), " ")
}

// lexFields splits text into tokens on whitespace, the separators _ and -, and
// the punctuation that carries no lexical signal, so "`pip install vllm`."
// yields "pip", "install", "vllm". It is the one tokenisation shared by the
// query, the indexed text and the vocabulary, so a word is the same word on
// both sides.
func lexFields(s string) []string {
	return strings.FieldsFunc(s, isLexSep)
}

// isLexSep reports whether r separates lexical tokens — the split lexFields
// and lexWordTokens share.
func isLexSep(r rune) bool {
	switch r {
	case ' ', '\t', '\n', '\r', '_', '-', '.', ',', ';', ':', '!', '?', '"', '\'',
		'`', '(', ')', '[', ']', '{', '}', '<', '>', '|', '/', '\\', '*', '#', '=':
		return true
	}
	return false
}

// queryStopwords are tokens that carry no retrieval signal. A conversational
// question ("what is the status of auth?") is mostly these; kept in the token
// list they let an unrelated item that merely contains the filler clear the
// coverage gate, or decide the confidence of a lexical answer.
var queryStopwords = map[string]bool{
	"a": true, "the": true, "an": true, "and": true, "or": true, "but": true,
	"is": true, "are": true, "was": true, "were": true, "be": true, "been": true, "being": true,
	"do": true, "does": true, "did": true, "doing": true, "done": true,
	"i": true, "you": true, "we": true, "it": true, "he": true, "she": true, "they": true,
	"my": true, "your": true, "our": true, "its": true, "their": true,
	"to": true, "of": true, "in": true, "on": true, "at": true, "by": true, "for": true,
	"with": true, "from": true, "into": true, "about": true, "as": true,
	"what": true, "when": true, "where": true, "which": true, "who": true, "whom": true,
	"why": true, "how": true, "that": true, "this": true, "these": true, "those": true,
	"can": true, "could": true, "should": true, "would": true, "will": true, "shall": true,
	"may": true, "might": true, "must": true, "need": true, "want": true,
	"have": true, "has": true, "had": true, "get": true, "got": true,
	"if": true, "then": true, "else": true, "so": true, "just": true, "only": true,
	"any": true, "all": true, "some": true, "no": true, "not": true, "there": true, "here": true,
	"me": true, "am": true, "run": true, "use": true, "used": true, "using": true,
}

// KwTokens splits a normalized query into its content-bearing tokens: stopwords
// and single characters are dropped, except that a single character with a digit
// ("q3" is two characters; "3" is one) stays. A query made only of stopwords
// keeps all its words, since filtering everything would leave the lexical
// channel with nothing to say.
func KwTokens(q string) []string {
	all := lexFields(q)
	out := make([]string, 0, len(all))
	for _, t := range all {
		if queryStopwords[t] {
			continue
		}
		if utf8.RuneCountInString(t) < 2 && !strings.ContainsAny(t, "0123456789") {
			continue
		}
		out = append(out, t)
	}
	if len(out) == 0 {
		return all
	}
	return out
}

func min3(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

// asciiRuneSelf mirrors utf8.RuneSelf (0x80) — the first byte value that can
// only appear as part of a multibyte UTF-8 sequence, never as a standalone
// ASCII byte.
const asciiRuneSelf = 0x80

// asciiOnly reports whether s contains only single-byte (ASCII) runes — true
// for the overwhelming majority of words in this corpus (service names, log
// terms, English prose). When true, indexing s by byte IS indexing it by
// rune (each byte is exactly one rune), so a DP can operate directly on the
// string's bytes with no []rune conversion — see editDistanceWithin.
func asciiOnly(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= asciiRuneSelf {
			return false
		}
	}
	return true
}

// editDistanceWithin reports whether the Levenshtein distance between a and
// b is at most max — equivalent to editDistance(a, b) <= max for every input,
// including non-ASCII, but avoiding editDistance's unconditional []rune
// conversion for the common ASCII case (a and b are runtime words/query
// tokens; profiling showed the []rune conversions — RuneCountInString +
// runtime.stringtoslicerune under the hood — as a large share of
// editDistance's own cost) and bailing out of the DP as soon as the answer
// can no longer be within max.
//
// The early exit is safe because of a standard Levenshtein-DP property: in
// the row-by-row DP below, every cell of row i is bounded below by row i-1's
// minimum value (curr[j+1] = min3(curr[j]+1, prev[j+1]+1, prev[j]+cost), and
// every one of those three candidates is >= rowMin(prev) — prev[j]+cost >=
// rowMin(prev) since cost >= 0, prev[j+1]+1 >= rowMin(prev)+1, and by
// induction curr[j]+1 >= rowMin(prev)+1 too). So row minimums are
// non-decreasing: once one exceeds max, so does the final distance, and the
// remaining rows need not be computed. TestEditDistanceWithin_MatchesEditDistance
// checks this against the reference editDistance function directly.
func editDistanceWithin(a, b string, max int) bool {
	if a == b {
		return true // distance 0; every real caller passes max >= 0
	}
	if max <= 0 {
		return false // a != b, so distance >= 1 > max
	}
	if asciiOnly(a) && asciiOnly(b) {
		if absDiff(len(a), len(b)) > max {
			return false
		}
		return editDistanceWithinASCII(a, b, max)
	}
	ra, rb := []rune(a), []rune(b)
	if absDiff(len(ra), len(rb)) > max {
		return false
	}
	return editDistanceWithinRunes(ra, rb, max)
}

// editDistanceWithinASCII is editDistanceWithin's DP for two ASCII-only
// strings, indexed directly by byte (byte index == rune index for ASCII).
// Mirrors editDistance's DP shape exactly (same recurrence, same min3), plus
// the row-minimum early exit described on editDistanceWithin.
func editDistanceWithinASCII(a, b string, max int) bool {
	la, lb := len(a), len(b)
	prev := make([]int, lb+1)
	curr := make([]int, lb+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 0; i < la; i++ {
		curr[0] = i + 1
		rowMin := curr[0]
		for j := 0; j < lb; j++ {
			cost := 1
			if a[i] == b[j] {
				cost = 0
			}
			v := min3(curr[j]+1, prev[j+1]+1, prev[j]+cost)
			curr[j+1] = v
			if v < rowMin {
				rowMin = v
			}
		}
		if rowMin > max {
			return false
		}
		prev, curr = curr, prev
	}
	return prev[lb] <= max
}

// editDistanceWithinRunes is editDistanceWithin's DP for the non-ASCII
// fallback, indexed by rune — identical shape to editDistanceWithinASCII,
// operating on []rune instead of string bytes.
func editDistanceWithinRunes(a, b []rune, max int) bool {
	la, lb := len(a), len(b)
	prev := make([]int, lb+1)
	curr := make([]int, lb+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 0; i < la; i++ {
		curr[0] = i + 1
		rowMin := curr[0]
		for j := 0; j < lb; j++ {
			cost := 1
			if a[i] == b[j] {
				cost = 0
			}
			v := min3(curr[j]+1, prev[j+1]+1, prev[j]+cost)
			curr[j+1] = v
			if v < rowMin {
				rowMin = v
			}
		}
		if rowMin > max {
			return false
		}
		prev, curr = curr, prev
	}
	return prev[lb] <= max
}

// wordTokens is a haystack's words (as lexFields would split it) paired with
// each word's precomputed rune length. fuzzyWordMatchFields' length banding
// (below) needs every word's rune count to compare against a query token's;
// computing it once per item at index time (see lexicalCache/
// buildLexicalCache) means a query never re-derives it, instead of
// utf8.RuneCountInString running once per (haystack word, query token) pair
// on every single search the way it used to.
//
// The words are kept as spans into text, not as a []string plus a []int: a
// string header is 16 bytes and an int 8, against 12 bytes per span, and
// every word of every item is held four times over (lower-cased and
// normalised, title and content). Word(i) is a substring of text, so reading
// a word allocates nothing.
type wordTokens struct {
	text  string
	spans []wordSpan
}

// wordSpan locates one word in wordTokens.text and records its rune count.
type wordSpan struct {
	off, n, runes uint32
}

// Len is the number of words.
func (w wordTokens) Len() int { return len(w.spans) }

// Word is the i-th word.
func (w wordTokens) Word(i int) string {
	sp := w.spans[i]
	return w.text[sp.off : sp.off+sp.n]
}

// RuneLen is the i-th word's rune count.
func (w wordTokens) RuneLen(i int) int { return int(w.spans[i].runes) }

// Words is the word list as a fresh slice.
func (w wordTokens) Words() []string {
	out := make([]string, len(w.spans))
	for i := range out {
		out[i] = w.Word(i)
	}
	return out
}

// lexWordTokens splits s exactly as lexFields does and records each word's
// span and rune count, without building the intermediate []string.
func lexWordTokens(s string) wordTokens {
	var spans []wordSpan
	start := -1
	runes := uint32(0)
	for i, r := range s {
		if isLexSep(r) {
			if start >= 0 {
				spans = append(spans, wordSpan{uint32(start), uint32(i - start), runes})
				start = -1
			}
			continue
		}
		if start < 0 {
			start, runes = i, 0
		}
		runes++
	}
	if start >= 0 {
		spans = append(spans, wordSpan{uint32(start), uint32(len(s) - start), runes})
	}
	return wordTokens{text: s, spans: shrunk(spans)}
}

// newWordTokens builds a wordTokens from an already-split word list. The words
// need not be substrings of one string, so they are joined into a new one;
// production code splits with lexWordTokens instead, which shares the source
// text.
func newWordTokens(words []string) wordTokens {
	text := strings.Join(words, " ")
	spans := make([]wordSpan, len(words))
	off := 0
	for i, w := range words {
		spans[i] = wordSpan{uint32(off), uint32(len(w)), uint32(utf8.RuneCountInString(w))}
		off += len(w) + 1
	}
	return wordTokens{text: text, spans: spans}
}

// matchesKWTokenFields returns true when a single token matches the
// haystack — exact substring first, then a length-scaled fuzzy word match.
// The haystack is pre-split into words — see fuzzyWordMatchFields.
func matchesKWTokenFields(h string, hf wordTokens, t string) bool {
	if strings.Contains(h, t) {
		return true
	}
	return fuzzyWordMatchFields(hf, t)
}

// Lexical scoring weights. lexScore folds the whole-query title
// bonuses, graded per-token match quality, and a small trigram tie-breaker into
// one ranking score. Token coverage (matchesKWTokenFields) remains the
// *inclusion* gate — see softMatchCoverage.
const (
	wExactTitle   = 1000 // title equals the whole normalized query
	wTitleContain = 600  // title contains the whole query as a substring
	wTokExactTle  = 100  // per-token: exact substring in title
	wTokFuzzyTle  = 60   // per-token: 1-edit fuzzy match in title
	wTokExactCnt  = 40   // per-token: exact substring in content
	wTokFuzzyCnt  = 25   // per-token: 1-edit fuzzy match in content
	wTrigram      = 20   // multiplied by mean trigram-overlap fraction (0..1) — tie-breaker only
	wMissPenalty  = 200  // per missing token — keeps full-coverage above partial
)

// ── Distinctiveness weighting ───────────────────────────────────────────────
//
// A conversational query ("what is wrong with the big query") holds a few
// tokens that name the thing asked about and several that are ordinary English.
// If every token is weighed only by whether and how it matched, a templated
// item whose boilerplate happens to contain the filler words scores as well as,
// or better than, an item that names the entity but shares no filler with the
// query.
//
// Each query token is therefore weighted by a corpus-derived distinctiveness
// score (idfWeight, a smoothed inverse document frequency) before it is folded
// into lexScoreFields: a token nearly every item contains contributes almost
// nothing whether matched or missed; a token only a few items contain dominates
// both the match bonus and the miss penalty. tokenWeights derives the weights
// from the vocabulary index's word postings alone, once per query.

// idfWeight computes the distinctiveness weight for one token: log(1 + N/df),
// the smoothed inverse document frequency. N is the size of the candidate set
// the search is scoped to; df is the token's literal word-level document
// frequency.
//
//   - df == N (every item contains the token): the formula's minimum, log(2),
//     as uninformative as a token can be.
//   - df small relative to N: the weight grows as df shrinks toward 1.
//   - df <= 0 (no item contains the token as a literal word, the usual case for
//     conversational filler against a corpus of templated text): floored to N,
//     not to 1. Add-one smoothing would floor df to 1 and give an unseen token
//     the largest weight, which is backwards: a token the corpus knows nothing
//     about cannot be its distinguishing feature, every candidate misses it
//     alike, and several such tokens would outweigh one rare entity token a
//     candidate does match. Flooring to N gives an absent token the weight of
//     the most ubiquitous real word: no information, assume unremarkable.
func idfWeight(df, n int) float64 {
	if n <= 0 {
		return 1
	}
	if df <= 0 {
		df = n
	}
	return math.Log1p(float64(n) / float64(df))
}

// tokenWeights computes idfWeight for every distinct token in toks against the
// vocabulary vocab describes and the candidate-set size n. A nil vocab treats
// every token's df as 0, so every weight floors to the minimum alike, which
// ranks the same as a uniform weight. A repeated token computes its weight once.
// It returns nil for an empty token list.
func tokenWeights(toks []string, vocab *vocabIndex, n int) map[string]float64 {
	if len(toks) == 0 {
		return nil
	}
	w := make(map[string]float64, len(toks))
	for _, t := range toks {
		if _, ok := w[t]; ok {
			continue
		}
		df := 0
		if vocab != nil {
			df = vocab.wordDocFreq(t)
		}
		w[t] = idfWeight(df, n)
	}
	return w
}

// tokenWeight looks up t's weight in weights, defaulting to 1.0 when weights is
// nil or has no entry for t. With a nil map every weight-scaled term of
// lexScoreFields reduces to its unweighted form, which is what callers without
// a vocabulary (benchmarks, reference implementations) get.
func tokenWeight(weights map[string]float64, t string) float64 {
	if weights == nil {
		return 1.0
	}
	if w, ok := weights[t]; ok {
		return w
	}
	return 1.0
}

// wCoverageMassThreshold is rankAndLimit's fallback admission bar
// (weightedMatchFraction) for an item that fails the strict count-based
// coverage gate of softMatchCoverageFields. It only ever admits more candidates:
// an item that clears the count gate is admitted exactly as before. 0.35 is low
// enough that one dominant entity token (the entity in "what is wrong with
// <entity>", against four filler tokens) clears it, and high enough that an
// item matching nothing distinctive does not.
const wCoverageMassThreshold = 0.35

// tokenMatchQualityFields returns the best per-token score against
// title/content, graded exact-title > fuzzy-title > exact-content >
// fuzzy-content > none. Title and content are pre-split into words — see
// fuzzyWordMatchFields.
func tokenMatchQualityFields(nt string, ntf wordTokens, nc string, ncf wordTokens, t string) int {
	if strings.Contains(nt, t) {
		return wTokExactTle
	}
	if fuzzyWordMatchFields(ntf, t) {
		return wTokFuzzyTle
	}
	if strings.Contains(nc, t) {
		return wTokExactCnt
	}
	if fuzzyWordMatchFields(ncf, t) {
		return wTokFuzzyCnt
	}
	return 0
}

// fuzzyTolerance is the allowed Levenshtein distance for a fuzzy word match,
// scaled by token length. 1-2 char tokens demand exactness; tokens of 3+ chars
// allow 1 edit — catches both short mishears ("rek"→"rec") and long ones
// ("lunchflow"→"launchflow").
func fuzzyTolerance(n int) int {
	if n <= 2 {
		return 0
	}
	return 1
}

// fuzzyWordMatchFields is fuzzyWordMatch with the haystack already split into
// words (and each word's rune length precomputed — see wordTokens). The split
// is invariant across a query's tokens, so a caller testing many tokens
// against the same title/content splits once instead of once per token — the
// same hoist as TokenGrams in rankAndLimit; the rune lengths are invariant
// across every query against the same item, so a cached wordTokens (see
// lexicalCache) hoists them further still, across every search, not just
// every token of one search.
func fuzzyWordMatchFields(wt wordTokens, t string) bool {
	tn := utf8.RuneCountInString(t)
	tol := fuzzyTolerance(tn)
	if tol == 0 {
		return false
	}
	n := wt.Len()
	for i := 0; i < n; i++ {
		word := wt.Word(i)
		wn := wt.RuneLen(i)
		// Levenshtein distance is at least the difference in length, so a word
		// whose length gap already exceeds the tolerance can never match. The
		// check is exact (no behaviour change) and skips the DP — which
		// allocates two rune slices and two int rows per call — for the large
		// majority of words. This is the hot path of the whole lexical scan:
		// fuzzyWordMatch runs for every query token that is not a literal
		// substring, against every word of every candidate item, four times
		// over (softMatchCoverage checks title and content,
		// tokenMatchQualityFields checks both again). Measured at 500
		// items × 8 tokens it was 838k allocations and 86 ms per scan
		// before this guard (BenchmarkLexicalScan_PerItem).
		if absDiff(wn, tn) <= tol && editDistanceWithin(t, word, tol) {
			return true
		}
		if i+1 < n {
			// Adjacent-pair join catches "Foo BAR" → "FooBAR". Build the
			// concatenation only when its length could possibly match.
			jn := wn + wt.RuneLen(i+1)
			jtol := fuzzyTolerance(jn)
			if absDiff(jn, tn) <= jtol && editDistanceWithin(t, word+wt.Word(i+1), jtol) {
				return true
			}
		}
	}
	return false
}

func absDiff(a, b int) int {
	if a > b {
		return a - b
	}
	return b - a
}

// idHaystack widens an item's content with its own ID for lexical matching: a
// query equal to, or a substring of, a cell's id should find the cell, because
// the id is what a caller copy-pasting from another view would type. It only
// widens the string handed to the matching functions and the trigram index; it
// never changes the item's Content or what is rendered back.
func idHaystack(it Item) string {
	if it.ID == "" {
		return it.Content
	}
	return it.Content + " " + it.ID
}

// softMatchCoverageFields is softMatchCoverage with title/content already
// lower-cased and split into wordTokens by the caller — the exact same nt/nc
// softMatchCoverage computes internally, just precomputed once (at index
// time, when sourced from lexicalCache) instead of on every call.
func softMatchCoverageFields(nt string, ntf wordTokens, nc string, ncf wordTokens, toks []string) (matched int, ok bool) {
	if len(toks) == 0 {
		return 0, true
	}
	for _, t := range toks {
		if matchesKWTokenFields(nt, ntf, t) || matchesKWTokenFields(nc, ncf, t) {
			matched++
		}
	}
	need := len(toks)
	if len(toks) >= 3 {
		need = len(toks) - 1
	}
	return matched, matched >= need
}

// lexScoreFields scores an item against a query. Title and content arrive
// already normalised and split into wordTokens (precomputed at index time when
// they come from the lexical cache).
//
// The per-token match quality (the 100/60/40/25 tiers of tokenMatchQualityFields)
// is scaled by that token's weight relative to the query's mean weight, then the
// sum is divided by the same mean. A query whose tokens all weigh alike keeps
// the plain tier sum, while within one query a high-weight token's match
// outweighs a low-weight token's. The miss penalty scales with the weighted
// fraction of the query left unmatched instead of the count: missing a filler
// word costs almost nothing, missing the token that carries most of the weight
// costs close to len(toks)*wMissPenalty.
//
// The exact-phrase title bonus and the trigram tie-breaker are unweighted.
func lexScoreFields(nt string, ntf wordTokens, nc string, ncf wordTokens, q string, toks []string, weights map[string]float64, trigramFrac float64) int {
	score := 0
	if q != "" && nt == q {
		score += wExactTitle
	} else if q != "" && strings.Contains(nt, q) {
		score += wTitleContain
	}

	var matchedWeight, totalWeight float64
	for _, t := range toks {
		w := tokenWeight(weights, t)
		totalWeight += w
		tq := tokenMatchQualityFields(nt, ntf, nc, ncf, t)
		if tq > 0 {
			matchedWeight += w
		}
		score += int(float64(tq) * w)
	}
	if len(toks) > 0 && totalWeight > 0 {
		mean := totalWeight / float64(len(toks))
		score = int(float64(score) / mean)
	}

	score += int(trigramFrac * wTrigram)

	if len(toks) > 0 && totalWeight > 0 && matchedWeight < totalWeight {
		score -= int(((totalWeight - matchedWeight) / totalWeight) * float64(len(toks)) * wMissPenalty)
	}
	return score
}

// weightedMatchFraction is rankAndLimit's fallback coverage signal: the share of
// the query's weight, not its token count, that the matched tokens account for:
// sum(weight(t) : t matched) / sum(weight(t) : t in toks). It uses the same
// per-token predicate as softMatchCoverageFields (matchesKWTokenFields), so the
// two always agree on which tokens matched and differ only in how a match counts.
//
// rankAndLimit uses it as an additive fallback: an item that clears the strict
// count-based bar is admitted without consulting it, and an item that fails the
// bar gets one more chance when its matched tokens carry enough of the query's
// weight (wCoverageMassThreshold).
func weightedMatchFraction(nt string, ntf wordTokens, nc string, ncf wordTokens, toks []string, weights map[string]float64) float64 {
	if len(toks) == 0 {
		return 1
	}
	var matchedWeight, totalWeight float64
	for _, t := range toks {
		w := tokenWeight(weights, t)
		totalWeight += w
		if matchesKWTokenFields(nt, ntf, t) || matchesKWTokenFields(nc, ncf, t) {
			matchedWeight += w
		}
	}
	if totalWeight <= 0 {
		return 0
	}
	return matchedWeight / totalWeight
}

// lexicalCache holds precomputed, item-derived tokenization state for the
// lexical search hot path. Every field is a pure function of Title, Content and
// ID, exactly what a per-query ToLower, NormalizeQuery and Fields call would
// produce, computed once by buildLexicalCache whenever those fields may have
// changed (Upsert, Mirror, Supersede's new item, Restore) instead of for every
// candidate of every search. A write that only changes Status, Links or
// UpdatedAt leaves the cache valid.
//
// built tells a computed cache from the zero value, so an item that reaches
// search without going through a write path falls back to computing the fields
// on the fly instead of scoring against empty ones.
type lexicalCache struct {
	built bool

	titleLower      string
	titleLowerWT    wordTokens
	haystackLower   string
	haystackLowerWT wordTokens

	titleNorm      string
	titleNormWT    wordTokens
	haystackNorm   string
	haystackNormWT wordTokens
}

// buildLexicalCache (re)computes item.lex from its current Title, Content and
// ID. Every write path that can change any of the three calls it before the item
// is stored. It takes no lock itself.
func buildLexicalCache(item *Item) { buildLexicalCacheText(item, idHaystack(*item)) }

// buildLexicalCacheText is buildLexicalCache with the haystack the host chose.
func buildLexicalCacheText(item *Item, haystack string) {
	tl := strings.ToLower(item.Title)
	hl := strings.ToLower(haystack)
	tn := NormalizeQuery(item.Title)
	hn := NormalizeQuery(haystack)
	item.lex = lexicalCache{
		built:           true,
		titleLower:      tl,
		titleLowerWT:    lexWordTokens(tl),
		haystackLower:   hl,
		haystackLowerWT: lexWordTokens(hl),
		titleNorm:       tn,
		titleNormWT:     lexWordTokens(tn),
		haystackNorm:    hn,
		haystackNormWT:  lexWordTokens(hn),
	}
}

// lowerFieldsFor returns the lower-cased title/haystack and their wordTokens
// split for it — the exact values softMatchCoverage computes internally
// (strings.ToLower, no CamelCase splitting; haystack is idHaystack(it), i.e.
// Content widened with ID). Uses it.lex when built; recomputes on the fly
// otherwise (item reached search without going through a write path that
// calls buildLexicalCache), so scoring is identical either way, just without
// the cache's speedup in the fallback case.
func lowerFieldsFor(it Item) (nt string, ntf wordTokens, nc string, ncf wordTokens) {
	if it.lex.built {
		return it.lex.titleLower, it.lex.titleLowerWT, it.lex.haystackLower, it.lex.haystackLowerWT
	}
	nt = strings.ToLower(it.Title)
	nc = strings.ToLower(idHaystack(it))
	return nt, lexWordTokens(nt), nc, lexWordTokens(nc)
}

// normFieldsFor is lowerFieldsFor for lexScore's normalized (CamelCase-split,
// underscore/dash-folded) fields instead of softMatchCoverage's plain
// lower-cased ones — see NormalizeQuery.
func normFieldsFor(it Item) (nt string, ntf wordTokens, nc string, ncf wordTokens) {
	if it.lex.built {
		return it.lex.titleNorm, it.lex.titleNormWT, it.lex.haystackNorm, it.lex.haystackNormWT
	}
	nt = NormalizeQuery(it.Title)
	nc = NormalizeQuery(idHaystack(it))
	return nt, lexWordTokens(nt), nc, lexWordTokens(nc)
}

func rankAndLimit(q string, toks []string, cands []Item, idx *trigramIndex, trigramCands map[string]struct{}, weights map[string]float64, limit int) ([]Item, bool) {
	return rankAndLimitPerKind(q, toks, cands, idx, trigramCands, weights, limit, 0)
}

// rankAndLimitPerKind is rankAndLimit with a per-kind quota: when perKind is
// positive, at most that many results of any one kind are returned, so a
// dominant kind cannot fill the limit and starve the smaller ones. Relevance
// order within a kind is kept and limit still caps the total.
func rankAndLimitPerKind(q string, toks []string, cands []Item, idx *trigramIndex, trigramCands map[string]struct{}, weights map[string]float64, limit, perKind int) ([]Item, bool) {
	type scored struct {
		it      Item
		score   int
		matched int
		// viaFallback marks an item admitted only through the
		// weightedMatchFraction fallback: it failed the strict count-based gate.
		// Such an item can have an arbitrarily low matched count (one
		// high-weight entity token, every filler token missed), so it is exempt
		// from the coverage cutoff below, which enforces count-based coverage
		// among items admitted by count.
		viaFallback bool
	}
	var hits []scored
	trigramContributed := false
	// Token trigrams are query-scoped: computing them once here instead of once
	// per candidate keeps the allocations at O(tokens) per search.
	//
	// fracByID is computed once for the whole candidate set by
	// QueryOverlapScores (one posting-list merge per query token) instead of per
	// candidate. A nil map read (no index, or no overlap for an id) returns the
	// zero value, so the per-candidate lookup needs no nil check.
	var fracByID map[string]float64
	if idx != nil && len(toks) > 0 {
		fracByID = idx.QueryOverlapScores(TokenGrams(toks))
	}
	for _, it := range cands {
		if len(toks) > 0 {
			// lowerFieldsFor/normFieldsFor read it.lex, built once at index time,
			// instead of redoing ToLower, NormalizeQuery and Fields on this
			// candidate's title and content on every search.
			nt, ntf, nc, ncf := lowerFieldsFor(it)
			matched, ok := softMatchCoverageFields(nt, ntf, nc, ncf, toks)
			viaFallback := false
			if !ok {
				// The strict count-based gate says too many tokens are missing, but
				// if the tokens this item did match carry enough of the query's
				// distinctiveness mass, admit it anyway. Purely additive: an item
				// that passed above never reaches this branch.
				if weightedMatchFraction(nt, ntf, nc, ncf, toks, weights) < wCoverageMassThreshold {
					continue
				}
				viaFallback = true
			}
			frac := fracByID[it.ID]
			ntN, ntfN, ncN, ncfN := normFieldsFor(it)
			sc := lexScoreFields(ntN, ntfN, ncN, ncfN, q, toks, weights, frac)
			if _, ok := trigramCands[it.ID]; ok {
				trigramContributed = true
			}
			hits = append(hits, scored{it, sc, matched, viaFallback})
		} else {
			hits = append(hits, scored{it, 0, 0, false})
		}
	}

	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		if !hits[i].it.UpdatedAt.Equal(&hits[j].it.UpdatedAt) {
			return hits[i].it.UpdatedAt.After(hits[j].it.UpdatedAt.Time)
		}
		return hits[i].it.ID < hits[j].it.ID
	})

	// Coverage cutoff: drop tail items that cover strictly fewer query tokens
	// than the top hit, so a single shared token doesn't let a long tail clear
	// the coverage gate. topCov is computed only from STRICT-gate hits (never
	// from a viaFallback item, which can have an arbitrarily low matched
	// count by design — see the scored struct's doc comment); a viaFallback
	// hit is likewise never subject to this cutoff itself, since it was
	// already vetted by weightedMatchFraction, a different bar than raw
	// token count. An item whose matched tokens carry at least
	// wCoverageMassThreshold of the query's weight is spared as well (see
	// carriesMass). When no strict-gate hit exists at all, topCov stays 0 and
	// this step is a no-op (every hit got here via the mass fallback).
	if len(hits) > 1 {
		topCov := 0
		for _, h := range hits {
			if !h.viaFallback && h.matched > topCov {
				topCov = h.matched
			}
		}
		kept := hits[:0]
		for _, h := range hits {
			if h.viaFallback || h.matched >= topCov || carriesMass(h.it, toks, weights) {
				kept = append(kept, h)
			}
		}
		hits = kept
	}

	if limit <= 0 {
		limit = 10
	}
	out := make([]Item, 0, min(limit, len(hits)))
	var perKindCount map[string]int
	if perKind > 0 {
		perKindCount = make(map[string]int, 16)
	}
	for _, h := range hits {
		if len(out) >= limit {
			break
		}
		if perKind > 0 {
			if perKindCount[h.it.Kind] >= perKind {
				continue
			}
			perKindCount[h.it.Kind]++
		}
		out = append(out, h.it)
	}
	return out, trigramContributed
}

// carriesMass reports whether an item's matched tokens account for at least
// wCoverageMassThreshold of the query's weight. The coverage cutoff spares such
// an item: a raw token count would drop it because another item happens to
// contain every filler word of the query.
func carriesMass(it Item, toks []string, weights map[string]float64) bool {
	nt, ntf, nc, ncf := lowerFieldsFor(it)
	return weightedMatchFraction(nt, ntf, nc, ncf, toks, weights) >= wCoverageMassThreshold
}

// ── Reciprocal Rank Fusion ────────────────────────────────────────────────

const rrfK = 60

// fuseRRF combines the lexical, semantic and exact-match rankings by Reciprocal
// Rank Fusion: each item's fused score is the sum of 1/(rrfK+rank) across the
// lists it appears in, ranks 0-based in input order (every input arrives sorted
// best first). RRF is used rather than a score blend because the channels'
// scores are not on comparable scales; only their ranks combine. An item found
// by two channels outranks one found by either alone.
//
// exact holds the items whose title or service name is the whole query. An
// exact match is normally lexical rank 0 as well, so it collects two rank-0
// votes, which beat the best a rival can hold (semantic 0 plus lexical 1).
//
// Equal fused scores are ordered deterministically: first the item found by the
// channel the caller trusts (the lexical one when the lexical answer was
// confident, the semantic one otherwise), then the more recently updated, then
// the smaller ID. Ordering by ID alone would put an arbitrary item first.
func fuseRRF(lex, sem, exact []Item, limit int, preferLex bool) []Item {
	fused := make(map[string]float64)
	byID := make(map[string]Item)
	inLex := make(map[string]bool, len(lex)+len(exact))
	inSem := make(map[string]bool, len(sem))
	add := func(items []Item, seen map[string]bool) {
		for rank, it := range items {
			fused[it.ID] += 1.0 / float64(rrfK+rank)
			seen[it.ID] = true
			byID[it.ID] = it
		}
	}
	add(lex, inLex)
	add(sem, inSem)
	add(exact, inLex)

	ids := make([]string, 0, len(fused))
	for id := range fused {
		ids = append(ids, id)
	}
	trusted := func(id string) bool {
		if preferLex {
			return inLex[id]
		}
		return inSem[id]
	}
	sort.SliceStable(ids, func(i, j int) bool {
		a, b := ids[i], ids[j]
		if fused[a] != fused[b] {
			return fused[a] > fused[b]
		}
		if ta, tb := trusted(a), trusted(b); ta != tb {
			return ta
		}
		ua, ub := byID[a].UpdatedAt, byID[b].UpdatedAt
		if !ua.Equal(&ub) {
			return ua.After(ub.Time)
		}
		return a < b
	})

	if limit <= 0 {
		limit = 10
	}
	out := make([]Item, 0, limit)
	for _, id := range ids {
		if len(out) >= limit {
			break
		}
		out = append(out, byID[id])
	}
	return out
}
