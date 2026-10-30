package memstore

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeQuery_StripsPunctuationAtTokenBoundaries(t *testing.T) {
	cases := map[string]string{
		"what is wrong with payment-api?": "what is wrong with payment api",
		"db?":                             "db",
		"ui, db.":                         "ui db",
		"say \"hello\" (world)":           "say hello world",
		"`pip install vllm`.":             "pip install vllm",
		"a/b|c":                           "a b c",
		"  spaced   out  ":                "spaced out",
		"WidgetREC":                       "widget rec",
	}
	for in, want := range cases {
		assert.Equal(t, want, NormalizeQuery(in), in)
	}
}

func TestKwTokens_HaveNoPunctuation(t *testing.T) {
	assert.Equal(t, []string{"wrong", "payment", "api"}, KwTokens(NormalizeQuery("what is wrong with payment-api?")))
	assert.Equal(t, []string{"db"}, KwTokens("db?"))
}

func TestLexicalCache_WordsCarryNoTrailingPunctuation(t *testing.T) {
	it := Item{}
	it.ID, it.Title, it.Content = "a", "Incident review.", "The bigquery, job: failed (twice)."
	buildLexicalCache(&it)
	assert.Equal(t, []string{"incident", "review"}, it.lex.titleLowerWT.Words())
	assert.Contains(t, it.lex.haystackLowerWT.Words(), "bigquery")
	assert.Contains(t, it.lex.haystackLowerWT.Words(), "job")
	assert.Contains(t, it.lex.haystackLowerWT.Words(), "twice")
	assert.NotContains(t, it.lex.haystackLowerWT.Words(), "bigquery,")
}

func TestVocabulary_DocumentFrequencyCountsAWordFollowedByPunctuation(t *testing.T) {
	s := NewStore()
	upsert(t, s, mk("a", "note", "t", "We use bigquery. It is fast."))
	upsert(t, s, mk("b", "note", "t", "Costs of bigquery, again, matter."))
	upsert(t, s, mk("c", "note", "t", "Nothing relevant here at all."))
	assert.Equal(t, 2, s.vocab.wordDocFreq("bigquery"), "a trailing period or comma does not hide the word from the document frequency")
}

func TestKeywordSearch_ATrailingQuestionMarkStillMatchesExactly(t *testing.T) {
	s := NewStore()
	upsert(t, s, mk("a", "note", "payment-api", "the payment service was down for ten minutes"))
	q := NormalizeQuery("payment-api?")
	toks := KwTokens(q)
	require.Equal(t, []string{"payment", "api"}, toks)

	items := s.KeywordSearch("payment-api?", "", "", 5)
	require.Len(t, items, 1)
	nt, ntf, nc, ncf := normFieldsFor(items[0])
	assert.Equal(t, wTokExactTle, tokenMatchQualityFields(nt, ntf, nc, ncf, toks[1]), "api? no longer degrades to a fuzzy match")
	assert.GreaterOrEqual(t, lexScoreFields(nt, ntf, nc, ncf, q, toks, nil, 0), wExactTitle, "the whole-query title bonus is not lost to the question mark")
}

func TestTrigramIndex_IndexesWordsWithoutPunctuation(t *testing.T) {
	idx := NewTrigramIndex()
	idx.Index("a", "Review.", "the bigquery, job")
	assert.Equal(t, 1.0, idx.ItemScore("a", []string{"bigquery"}))
	assert.Equal(t, 1.0, idx.ItemScore("a", []string{"review"}))
	assert.Equal(t, []string{"a"}, idx.Search("bigquery job?"))
}

// TestLexWordTokens_MatchesLexFields proves the span representation yields the
// words lexFields does, with the rune count of each.
func TestLexWordTokens_MatchesLexFields(t *testing.T) {
	for _, in := range []string{
		"",
		"   ",
		"plain words here",
		"  leading and trailing  ",
		"snake_case-and.dotted;list,of:things!? `code` (paren) [br] {cu} <an> a|b/c\\d*e#f=g",
		"Ünïcödé wörds 日本語 テスト mixed café",
		"bad \xff\xfe bytes \xc3 here",
		"one",
		strings.Repeat("long", 5000) + " tail",
	} {
		want := lexFields(in)
		got := lexWordTokens(in)
		assert.Equal(t, len(want), got.Len(), "%q", in)
		assert.Equal(t, want, append([]string{}, got.Words()...), "%q", in)
		for i, w := range want {
			assert.Equal(t, utf8.RuneCountInString(w), got.RuneLen(i), "%q word %d", in, i)
		}
	}
}
