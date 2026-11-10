package xray

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A recall through MCP is a hit event: it is what lights the recalled cells and
// their region in a connected UI.
func TestMCPSearchMemory_PublishesAHitForTheSharedItemsItReturns(t *testing.T) {
	store := newTestStore()
	ctx := context.Background()
	_, msg := store.Upsert(ctx, "pub-1", "operational_pattern", "public", "Retry policy", "payments retries failures three times", "me", "", "", nil, []string{"area:payments"})
	require.Empty(t, msg)
	_, msg = store.Upsert(ctx, "priv-1", "note", "private", "My retry note", "payments retries are my own business", "me", "", "", nil, nil)
	require.Empty(t, msg)

	ts := httptest.NewServer(NewMCPServer(store, zerolog.Nop(), func() string { return "tok" }, nil, nil, "", nil, 0, 0, 0))
	defer ts.Close()
	c, err := client.NewStreamableHttpClient(ts.URL+"/mcp", transport.WithHTTPHeaders(map[string]string{"Authorization": "Bearer tok"}))
	require.NoError(t, err)
	require.NoError(t, c.Start(ctx))
	defer c.Close()
	init := mcp.InitializeRequest{}
	init.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	init.Params.ClientInfo = mcp.Implementation{Name: "t", Version: "1"}
	_, err = c.Initialize(ctx, init)
	require.NoError(t, err)

	events, stopEvents := store.hub.Subscribe("")
	defer stopEvents()
	var req mcp.CallToolRequest
	req.Params.Name = "search_memory"
	req.Params.Arguments = map[string]any{"query": "payments retries", "subject": "me"}
	res, err := c.CallTool(ctx, req)
	require.NoError(t, err)
	assert.Contains(t, res.Content[0].(mcp.TextContent).Text, "priv-1", "the owner still gets the private item back")

	select {
	case f := <-events:
		var hit MemoryHitEvent
		require.NoError(t, json.Unmarshal(f.Data, &hit))
		assert.Equal(t, []string{"pub-1"}, hit.ItemIDs, "the private item is returned to its owner but never named on the stream")
		assert.Equal(t, []string{"operational_pattern"}, hit.Kinds)
		assert.Equal(t, [][]string{{"area:payments"}}, hit.Tags, "tags place the hit in a category split off its kind by tag")
		assert.Empty(t, hit.Query, "the stream reaches every client and does not carry what was asked")
	case <-time.After(3 * time.Second):
		t.Fatal("no hit event for a recall")
	}
}

func TestMemoryStore_PublishHit_TagsOnlyForSharedItems(t *testing.T) {
	store := newTestStore()
	ctx := context.Background()
	_, msg := store.Upsert(ctx, "pub-1", "policy", "public", "Shared", "shared body", "me", "", "", nil, []string{"area:a"})
	require.Empty(t, msg)
	_, msg = store.Upsert(ctx, "priv-1", "note", "private", "Mine", "private body", "me", "", "", nil, []string{"secret-topic"})
	require.Empty(t, msg)

	events, stop := store.hub.Subscribe("")
	defer stop()
	// Voice's pinned fetch names its caller's own private items too; their tags stay off the shared stream.
	store.PublishHit([]string{"pub-1", "priv-1", "gone"}, "", []string{"policy", "note", "policy"})

	select {
	case f := <-events:
		var hit MemoryHitEvent
		require.NoError(t, json.Unmarshal(f.Data, &hit))
		assert.Equal(t, [][]string{{"area:a"}, nil, nil}, hit.Tags)
	case <-time.After(3 * time.Second):
		t.Fatal("no hit event")
	}
}

func TestMemoryStore_PublishHit_NoTagsLeavesTheEventAsItWas(t *testing.T) {
	store := newTestStore()
	ctx := context.Background()
	_, msg := store.Upsert(ctx, "plain-1", "policy", "public", "Plain", "no tags", "me", "", "", nil, nil)
	require.Empty(t, msg)

	events, stop := store.hub.Subscribe("")
	defer stop()
	store.PublishHit([]string{"plain-1"}, "", []string{"policy"})

	select {
	case f := <-events:
		assert.NotContains(t, string(f.Data), `"tags"`, "a store without tags sends the event without the field")
	case <-time.After(3 * time.Second):
		t.Fatal("no hit event")
	}
}
