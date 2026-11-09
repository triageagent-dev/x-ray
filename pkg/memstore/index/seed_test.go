package index

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	memoryv1 "github.com/triageagent-dev/x-ray/v2/api/v1"
)

func cellWith(embedding string) *memoryv1.MemoryCell {
	return &memoryv1.MemoryCell{Spec: memoryv1.MemoryCellSpec{ID: "c", Kind: "note", Title: "t", Content: "body", Embedding: embedding}}
}

// fingerprinted marks c's embedding as made from doc's text by s.
func fingerprinted(c *memoryv1.MemoryCell, s *CellStore, doc IndexEntry) *memoryv1.MemoryCell {
	c.Annotations = map[string]string{FingerprintAnnotation: s.EmbeddingFingerprint(doc, c.Spec.Embedding)}
	return c
}

// failingGateway is an embeddings server that fails every request, behind an
// embedder of dims dimensions (0: the model's own, so the store learns it from
// the first vector).
func failingGateway(t *testing.T, dims int) *Embedder {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "embedder down", http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	e, err := NewEmbedder(context.Background(), srv.URL, "m", "", "", dims, 0, zerolog.Nop())
	require.NoError(t, err)
	return e
}

func TestSeedOrEmbed_ACellThatCarriesAUsableEmbeddingIsSeededAndNothingNeedsWriting(t *testing.T) {
	emb := textGateway(t, map[string][]float32{})
	s := NewCellStore(emb, zerolog.Nop())
	doc := IndexEntry{ID: "c"}
	got, err := s.SeedOrEmbed(context.Background(), fingerprinted(cellWith(EncodeEmbedding(vec(1, 0, 0))), s, doc), doc)
	require.NoError(t, err)
	assert.Empty(t, got, "the cell already holds its vector")
	assert.EqualValues(t, 1, s.CollectionSize())
}

func TestSeedOrEmbed_ACellWithoutAnEmbeddingIsEmbeddedAndTheVectorReturnedToPersist(t *testing.T) {
	doc := IndexEntry{ID: "c", Kind: "note", Title: "t", Content: "body"}
	emb := textGateway(t, map[string][]float32{cellEmbedText(doc): vec(0, 1, 0)})
	s := NewCellStore(emb, zerolog.Nop())
	got, err := s.SeedOrEmbed(context.Background(), cellWith(""), doc)
	require.NoError(t, err)
	require.NotEmpty(t, got)
	v, err := DecodeEmbedding(got)
	require.NoError(t, err)
	assert.Equal(t, vec(0, 1, 0), v)
	assert.EqualValues(t, 1, s.CollectionSize())
}

func TestSeedOrEmbed_AWrongDimensionOrUndecodableEmbeddingIsReplaced(t *testing.T) {
	doc := IndexEntry{ID: "c", Kind: "note", Title: "t", Content: "body"}
	emb := textGateway(t, map[string][]float32{cellEmbedText(doc): vec(0, 1, 0)})
	s := NewCellStore(emb, zerolog.Nop())
	require.True(t, s.SeedFromEmbedding(IndexEntry{ID: "first"}, vec(1, 0, 0))) // fixes the dimension at four

	// Both carry a matching fingerprint, so the guard under test is what fails.
	wrong := EncodeEmbedding([]float32{1, 0})
	got, err := s.SeedOrEmbed(context.Background(), fingerprinted(cellWith(wrong), s, doc), doc)
	require.NoError(t, err)
	assert.NotEmpty(t, got, "the stored vector disagrees with the index dimension, so the cell is re-embedded")
	assert.NotEqual(t, wrong, got)

	got, err = s.SeedOrEmbed(context.Background(), fingerprinted(cellWith("!!! not base64 !!!"), s, doc), doc)
	require.NoError(t, err)
	assert.NotEmpty(t, got, "an embedding that cannot be decoded is replaced too")
}

func TestSeedOrEmbed_AFailedEmbedIsAnError(t *testing.T) {
	s := NewCellStore(textGateway(t, map[string][]float32{}), zerolog.Nop())
	_, err := s.SeedOrEmbed(context.Background(), cellWith(""), IndexEntry{ID: "c", Title: "unknown"})
	assert.Error(t, err)
}

func TestSeedOrEmbed_AStoredEmbeddingOfAnotherTextOrWithoutAFingerprintIsReEmbedded(t *testing.T) {
	doc := IndexEntry{ID: "c", Kind: "note", Title: "t", Content: "new body"}
	emb := textGateway(t, map[string][]float32{cellEmbedText(doc): vec(0, 1, 0)})
	s := NewCellStore(emb, zerolog.Nop())
	stored := EncodeEmbedding(vec(1, 0, 0))

	old := doc
	old.Content = "old body"
	got, err := s.SeedOrEmbed(context.Background(), fingerprinted(cellWith(stored), s, old), doc)
	require.NoError(t, err)
	require.NotEmpty(t, got, "the stored vector was made from the old text")
	v, err := DecodeEmbedding(got)
	require.NoError(t, err)
	assert.Equal(t, vec(0, 1, 0), v, "the new text's vector is returned to persist")

	got, err = s.SeedOrEmbed(context.Background(), cellWith(stored), doc)
	require.NoError(t, err)
	assert.NotEmpty(t, got, "an embedding written before fingerprints is re-embedded once")
	assert.EqualValues(t, 1, s.CollectionSize())
}

func TestEmbeddingFingerprint_FollowsTheTextTheModelTheDimensionAndTheVector(t *testing.T) {
	newStore := func(model string, dims int) *CellStore {
		e, err := NewEmbedder(context.Background(), "http://unused", model, "", "", dims, 0, zerolog.Nop())
		require.NoError(t, err)
		return NewCellStore(e, zerolog.Nop())
	}
	doc := IndexEntry{ID: "c", Kind: "note", Scope: "public", Subject: "s", Title: "t", Content: "body"}
	const emb = "AAAA"
	s := newStore("m", 8)
	fp := s.EmbeddingFingerprint(doc, emb)
	assert.Regexp(t, `^[0-9a-f]{64}\.[0-9a-f]{16}$`, fp)
	assert.Equal(t, fp, newStore("m", 8).EmbeddingFingerprint(doc, emb), "the same text, model, dimension and vector")

	changed := doc
	changed.Content = "other body"
	assert.NotEqual(t, fp, s.EmbeddingFingerprint(changed, emb), "a changed text")
	assert.NotEqual(t, fp, newStore("m2", 8).EmbeddingFingerprint(doc, emb), "another model")
	assert.NotEqual(t, fp, newStore("m", 16).EmbeddingFingerprint(doc, emb), "another dimension")
	other := s.EmbeddingFingerprint(doc, "BBBB")
	assert.NotEqual(t, fp, other, "another stored vector")
	assert.Equal(t, fp[:64], other[:64], "the text part is the same")

	unrelated := doc
	unrelated.Tags, unrelated.Confidence = []string{"x"}, "high"
	assert.Equal(t, fp, s.EmbeddingFingerprint(unrelated, emb), "fields the embedder is not sent do not count")

	long := doc
	long.Content = strings.Repeat("a", 4000)
	pastCap := long
	pastCap.Content += " only past the 1800-byte cap"
	assert.Equal(t, s.EmbeddingFingerprint(long, emb), s.EmbeddingFingerprint(pastCap, emb), "text the embedder is never sent does not count")
}

func TestSeedOrEmbed_AVectorReplacedUnderItsAnnotationIsReEmbedded(t *testing.T) {
	doc := IndexEntry{ID: "c", Kind: "note", Title: "t", Content: "body"}
	emb := textGateway(t, map[string][]float32{cellEmbedText(doc): vec(0, 1, 0)})
	s := NewCellStore(emb, zerolog.Nop())
	c := fingerprinted(cellWith(EncodeEmbedding(vec(1, 0, 0))), s, doc)

	// Another writer swaps the vector and leaves the annotation as it was.
	c.Spec.Embedding = EncodeEmbedding(vec(0, 0, 1))
	got, err := s.SeedOrEmbed(context.Background(), c, doc)
	require.NoError(t, err)
	require.NotEmpty(t, got, "the annotation does not vouch for this vector")
	v, err := DecodeEmbedding(got)
	require.NoError(t, err)
	assert.Equal(t, vec(0, 1, 0), v)
}

func TestSeedOrEmbed_AFailedEmbedSeedsAStaleStoredVectorAndReportsTheError(t *testing.T) {
	doc := IndexEntry{ID: "c", Kind: "note", Title: "t", Content: "body"}
	ctx := context.Background()

	s := NewCellStore(failingGateway(t, 4), zerolog.Nop())
	got, err := s.SeedOrEmbed(ctx, cellWith(EncodeEmbedding(vec(1, 0, 0))), doc)
	require.Error(t, err, "the caller retries")
	assert.Empty(t, got, "nothing to write")
	assert.EqualValues(t, 1, s.CollectionSize(), "the cell stays searchable by its stored vector")

	wrongDim := NewCellStore(failingGateway(t, 4), zerolog.Nop())
	_, err = wrongDim.SeedOrEmbed(ctx, cellWith(EncodeEmbedding([]float32{1, 0, 0, 0, 0, 0, 0, 0})), doc)
	require.Error(t, err)
	assert.Zero(t, wrongDim.CollectionSize(), "a vector of another dimension is not seeded")

	unknownDim := NewCellStore(failingGateway(t, 0), zerolog.Nop())
	_, err = unknownDim.SeedOrEmbed(ctx, cellWith(EncodeEmbedding(vec(1, 0, 0))), doc)
	require.Error(t, err)
	assert.Zero(t, unknownDim.CollectionSize(), "a stale vector never sets the store's dimension")
}

func TestSubjectIndex_ANaNVectorReSeededAddsNoNodeAndNeedsNoRebuild(t *testing.T) {
	idx := newSubjectIndex()
	nan := vec(float32(math.NaN()), 1, 0)
	for range 3 {
		idx.insert(IndexEntry{ID: "n", Kind: "note"}, nan)
	}
	assert.Equal(t, 1, idx.index.Size())
	assert.False(t, idx.dirty, "the same bits are the same vector")
}

func TestCellStore_ACellWhoseSubjectChangedLeavesItsOldSubject(t *testing.T) {
	s := NewCellStore(nil, zerolog.Nop())
	doc := IndexEntry{ID: "a", Subject: "old", Kind: "note"}
	require.True(t, s.SeedFromEmbedding(doc, vec(1, 0, 0)))
	doc.Subject = "new"
	require.True(t, s.SeedFromEmbedding(doc, vec(1, 0, 0)))
	assert.EqualValues(t, 1, s.CollectionSize(), "one entry, in the new subject")
	got := s.searchAll(vec(1, 0, 0), 10, "")
	require.Len(t, got, 1, "returned once")
	assert.Equal(t, "new", got[0].Document.Subject)
}

// Every reconcile seeds its cell again, and the graph has no key dedupe: it
// folds an equal vector into the node it finds, but a zero-norm vector has no
// finite distance and is never found, so each Insert of it adds a node. A
// re-seed of an unchanged vector must add none, or the graph grows by one node
// per such cell per reconcile and search returns the cell once per node.
func TestSubjectIndex_ReSeedingAnUnchangedCellAddsNoGraphNode(t *testing.T) {
	s := NewCellStore(nil, zerolog.Nop())
	doc := IndexEntry{ID: "a", Subject: "s", Kind: "note", Title: "t"}
	zero := IndexEntry{ID: "zero", Subject: "s", Kind: "note", Title: "z"}
	for range 5 {
		require.True(t, s.SeedFromEmbedding(doc, vec(1, 0, 0)))
		require.True(t, s.SeedFromEmbedding(zero, vec(0, 0, 0)))
	}
	idx := s.getOrCreate("s")
	assert.Equal(t, 2, idx.index.Size(), "one node per cell however often it is seeded")
	assert.False(t, idx.dirty, "nothing to rebuild")
	assert.Len(t, idx.search(vec(1, 0, 0), 10, ""), 2, "each cell is returned once")
	assert.Zero(t, idx.rebuilds)

	// A changed vector still adds a node and marks the graph dirty; the next
	// read rebuilds it down to one node per cell.
	require.True(t, s.SeedFromEmbedding(doc, vec(0, 1, 0)))
	assert.True(t, idx.dirty)
	idx.search(vec(0, 1, 0), 10, "")
	assert.Equal(t, 2, idx.index.Size())
}

func TestSubjectIndex_ReSeedWithChangedFieldsAndTheSameVectorRefreshesTheEntryOnly(t *testing.T) {
	idx := newSubjectIndex()
	idx.insert(IndexEntry{ID: "a", Kind: "note", Title: "old title"}, vec(1, 0, 0))
	idx.insert(IndexEntry{ID: "a", Kind: "note", Title: "new title", Tags: []string{"x"}}, vec(1, 0, 0))
	assert.Equal(t, 1, idx.index.Size(), "no new node for the same vector")
	assert.False(t, idx.dirty)
	got := idx.search(vec(1, 0, 0), 10, "")
	require.Len(t, got, 1)
	assert.Equal(t, "new title", got[0].Document.Title, "the entry carries the new fields")
	assert.Equal(t, []string{"x"}, got[0].Document.Tags)
}
