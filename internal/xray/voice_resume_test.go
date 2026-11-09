package xray

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genai"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
)

// ── Session resumption and teardown (ADR-039 capabilities 1 and 2) ───────────

// fakeLive is a Gemini Live session the test drives: Receive hands out what was
// pushed, in order, and the sends are counted.
type fakeLive struct {
	recv      chan fakeRecv
	closed    chan struct{}
	closeOnce sync.Once
	sendErr   atomic.Bool  // when set, every send fails as on a dropped connection
	sends     atomic.Int32 // send attempts, failed ones included
	audio     atomic.Int32 // audio frames accepted
	mu        sync.Mutex
	got       []string // accepted messages in order: "audio:<data>", "end", "content", "tool"
}

type fakeRecv struct {
	msg *genai.LiveServerMessage
	err error
}

func newFakeLive() *fakeLive {
	return &fakeLive{recv: make(chan fakeRecv, 16), closed: make(chan struct{})}
}

func (f *fakeLive) push(msg *genai.LiveServerMessage) { f.recv <- fakeRecv{msg: msg} }
func (f *fakeLive) pushErr(err error)                 { f.recv <- fakeRecv{err: err} }

func (f *fakeLive) isClosed() bool {
	select {
	case <-f.closed:
		return true
	default:
		return false
	}
}

func (f *fakeLive) Receive() (*genai.LiveServerMessage, error) {
	select {
	case r := <-f.recv:
		return r.msg, r.err
	case <-f.closed:
		return nil, errors.New("fake live: use of closed connection")
	}
}

func (f *fakeLive) Close() error {
	f.closeOnce.Do(func() { close(f.closed) })
	return nil
}

func (f *fakeLive) send() error {
	f.sends.Add(1)
	if f.isClosed() || f.sendErr.Load() {
		return errors.New("fake live: broken pipe")
	}
	return nil
}

func (f *fakeLive) record(m string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, m)
}

// received returns the messages the session accepted, in order.
func (f *fakeLive) received() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.got...)
}

func (f *fakeLive) SendClientContent(genai.LiveClientContentInput) error {
	if err := f.send(); err != nil {
		return err
	}
	f.record("content")
	return nil
}

func (f *fakeLive) SendToolResponse(genai.LiveToolResponseInput) error {
	if err := f.send(); err != nil {
		return err
	}
	f.record("tool")
	return nil
}

func (f *fakeLive) SendRealtimeInput(in genai.LiveRealtimeInput) error {
	if err := f.send(); err != nil {
		return err
	}
	if in.Audio != nil {
		f.audio.Add(1)
		f.record("audio:" + string(in.Audio.Data))
	}
	if in.AudioStreamEnd {
		f.record("end")
	}
	return nil
}

// fakeConnector stands in for Live.Connect: it records the resumption handle of
// every call and returns the queued sessions in order, then fails.
type fakeConnector struct {
	mu      sync.Mutex
	next    []*fakeLive
	handles []string
	configs []*genai.LiveConnectConfig
}

func (c *fakeConnector) connect(_ context.Context, cfg *genai.LiveConnectConfig) (liveConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h := "<none>"
	if cfg.SessionResumption != nil {
		h = cfg.SessionResumption.Handle
	}
	c.handles = append(c.handles, h)
	c.configs = append(c.configs, cfg)
	if len(c.next) == 0 {
		return nil, errors.New("fake connect: handle rejected")
	}
	l := c.next[0]
	c.next = c.next[1:]
	return l, nil
}

func (c *fakeConnector) config(i int) *genai.LiveConnectConfig {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.configs[i]
}

func (c *fakeConnector) seen() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.handles...)
}

// browserPair returns both ends of a WebSocket: the server end goes to the
// session, the client end plays the browser.
func browserPair(t *testing.T) (server, client *websocket.Conn) {
	t.Helper()
	up := websocket.Upgrader{}
	ch := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		ch <- c
	}))
	t.Cleanup(srv.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.NoError(t, err)
	server = <-ch
	t.Cleanup(func() { client.Close(); server.Close() })
	return server, client
}

var testBaseLiveConfig = &genai.LiveConnectConfig{
	ResponseModalities: []genai.Modality{genai.ModalityAudio},
	SystemInstruction:  genai.NewContentFromText("you are a test", "user"),
}

// startResumeSession runs a voice session on a fake Live session and returns it
// with the browser end of its WebSocket. opts adjust the session before it runs.
func startResumeSession(t *testing.T, first *fakeLive, c *fakeConnector, resumption bool, opts ...func(*VoiceSession)) (*VoiceSession, *websocket.Conn) {
	t.Helper()
	server, client := browserPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	s := &VoiceSession{
		id:               "vs-test",
		logger:           zerolog.Nop(),
		browser:          server,
		live:             first,
		connect:          c.connect,
		liveConfig:       testBaseLiveConfig,
		resumption:       resumption,
		cancel:           cancel,
		done:             make(chan struct{}),
		hangup:           make(chan struct{}),
		sessionStartTime: time.Now(),
	}
	for _, o := range opts {
		o(s)
	}
	go s.run(ctx)
	// The browser end closes first, so a session that fails to end its own
	// browser read still ends here and the test fails instead of hanging.
	t.Cleanup(func() { client.Close(); s.Close() })
	return s, client
}

// readUntil reads browser messages until one matches, failing after 5 s.
func readUntil(t *testing.T, browser *websocket.Conn, match func(VoiceServerMessage) bool) VoiceServerMessage {
	t.Helper()
	require.NoError(t, browser.SetReadDeadline(time.Now().Add(5*time.Second)))
	for {
		var m VoiceServerMessage
		err := browser.ReadJSON(&m)
		require.NoError(t, err, "browser read")
		if match(m) {
			return m
		}
	}
}

// expectClose reads until the server closes the socket and returns whether a
// Done message came first and the close code (0 when no close frame arrived).
func expectClose(t *testing.T, browser *websocket.Conn) (sawDone bool, code int) {
	t.Helper()
	require.NoError(t, browser.SetReadDeadline(time.Now().Add(5*time.Second)))
	for {
		var m VoiceServerMessage
		err := browser.ReadJSON(&m)
		if err != nil {
			var ce *websocket.CloseError
			if errors.As(err, &ce) {
				return sawDone, ce.Code
			}
			return sawDone, 0
		}
		if m.Done {
			sawDone = true
		}
	}
}

// blockBrowserRead has the browser send one frame and waits until the session
// has relayed it, so readFromBrowser is back in its blocking read, where a
// live session spends its time between frames.
func blockBrowserRead(t *testing.T, browser *websocket.Conn, live *fakeLive) {
	t.Helper()
	require.NoError(t, browser.WriteJSON(VoiceBrowserMessage{Audio: []byte{1}}))
	require.Eventually(t, func() bool { return live.audio.Load() == 1 }, 5*time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
}

func waitDone(t *testing.T, s *VoiceSession) {
	t.Helper()
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("voice session did not end")
	}
}

func assertRunning(t *testing.T, s *VoiceSession) {
	t.Helper()
	select {
	case <-s.done:
		t.Fatal("voice session ended; it should have resumed")
	default:
	}
}

func resumable(handle string) *genai.LiveServerMessage {
	return &genai.LiveServerMessage{SessionResumptionUpdate: &genai.LiveServerSessionResumptionUpdate{NewHandle: handle, Resumable: true}}
}

// resumableAt is a resumable handle whose state covers the client messages up to
// last (LastConsumedClientMessageIndex, sent when transparent is on).
func resumableAt(handle string, last int64) *genai.LiveServerMessage {
	m := resumable(handle)
	m.SessionResumptionUpdate.LastConsumedClientMessageIndex = last
	return m
}

func turnComplete() *genai.LiveServerMessage {
	return &genai.LiveServerMessage{ServerContent: &genai.LiveServerContent{TurnComplete: true}}
}

func modelAudio() *genai.LiveServerMessage {
	return &genai.LiveServerMessage{ServerContent: &genai.LiveServerContent{ModelTurn: &genai.Content{Parts: []*genai.Part{{InlineData: &genai.Blob{Data: []byte{1, 2}}}}}}}
}

func transcript(text string) *genai.LiveServerMessage {
	return &genai.LiveServerMessage{ServerContent: &genai.LiveServerContent{OutputTranscription: &genai.Transcription{Text: text}}}
}

func hasTranscript(text string) func(VoiceServerMessage) bool {
	return func(m VoiceServerMessage) bool { return m.Transcript == text }
}

// Gemini Live recycles its connection about every 10 minutes and says so with a
// GoAway 30-60 s ahead. With a resumable handle held, resumption on and the model
// idle, the session reconnects at once with that handle, and the browser never
// notices.
func TestVoiceSession_GoAwayResumesWithHeldHandle(t *testing.T) {
	live1, live2 := newFakeLive(), newFakeLive()
	c := &fakeConnector{next: []*fakeLive{live2}}
	s, browser := startResumeSession(t, live1, c, true)

	live1.push(resumable("h1"))
	// A point the server cannot resume from leaves the held handle alone.
	live1.push(&genai.LiveServerMessage{SessionResumptionUpdate: &genai.LiveServerSessionResumptionUpdate{Resumable: false}})
	live1.push(&genai.LiveServerMessage{GoAway: &genai.LiveServerGoAway{TimeLeft: 50 * time.Second}})
	live2.push(transcript("after resume"))

	readUntil(t, browser, hasTranscript("after resume"))
	assert.Equal(t, []string{"h1"}, c.seen(), "reconnects once, at once, with the held handle")
	assert.True(t, live1.isClosed(), "the recycled session is closed")
	assert.Same(t, testBaseLiveConfig.SystemInstruction, c.config(0).SystemInstruction, "the resumed session keeps the session's config")
	assertRunning(t, s)
}

// A GoAway while the model is speaking waits for the end of that turn, so the
// answer is not cut off; the reconnect follows the TurnComplete, on the handle
// that covers the answer.
func TestVoiceSession_GoAwayMidTurnResumesAtTurnComplete(t *testing.T) {
	live1, live2 := newFakeLive(), newFakeLive()
	c := &fakeConnector{next: []*fakeLive{live2}}
	s, browser := startResumeSession(t, live1, c, true)

	live1.push(resumable("h1"))
	live1.push(&genai.LiveServerMessage{ServerContent: &genai.LiveServerContent{ModelTurn: &genai.Content{Parts: []*genai.Part{{InlineData: &genai.Blob{Data: []byte{1, 2}}}}}}})
	live1.push(&genai.LiveServerMessage{GoAway: &genai.LiveServerGoAway{TimeLeft: 50 * time.Second}})
	live1.push(transcript("rest of the answer"))
	readUntil(t, browser, hasTranscript("rest of the answer"))
	assert.Empty(t, c.seen(), "no reconnect while the model is mid-answer")

	live1.push(&genai.LiveServerMessage{ServerContent: &genai.LiveServerContent{TurnComplete: true}})
	live1.push(resumable("h2"))
	live2.push(transcript("after resume"))
	readUntil(t, browser, hasTranscript("after resume"))
	assert.Equal(t, []string{"h2"}, c.seen())
	assertRunning(t, s)
}

// A GoAway deferred to the end of the answer is spent when the connection drops
// first and is resumed then: the next TurnComplete does not reconnect again.
func TestVoiceSession_GoAwayMidTurnThenDropResumesOnce(t *testing.T) {
	live1, live2 := newFakeLive(), newFakeLive()
	c := &fakeConnector{next: []*fakeLive{live2, newFakeLive()}}
	s, browser := startResumeSession(t, live1, c, true)

	live1.push(resumable("h1"))
	live1.push(&genai.LiveServerMessage{ServerContent: &genai.LiveServerContent{ModelTurn: &genai.Content{Parts: []*genai.Part{{InlineData: &genai.Blob{Data: []byte{1, 2}}}}}}})
	live1.push(&genai.LiveServerMessage{GoAway: &genai.LiveServerGoAway{TimeLeft: 50 * time.Second}})
	live1.pushErr(&websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "GoAway deadline exceeded"})
	live2.push(&genai.LiveServerMessage{ServerContent: &genai.LiveServerContent{TurnComplete: true}})
	live2.push(transcript("after resume"))

	readUntil(t, browser, hasTranscript("after resume"))
	assert.Equal(t, []string{"h1"}, c.seen(), "one reconnect for one GoAway")
	assertRunning(t, s)
}

// An upstream drop without a close frame (1006, unexpected EOF) is resumed too,
// when a handle is held.
func TestVoiceSession_ReceiveErrorResumesWithHeldHandle(t *testing.T) {
	live1, live2 := newFakeLive(), newFakeLive()
	c := &fakeConnector{next: []*fakeLive{live2}}
	s, browser := startResumeSession(t, live1, c, true)

	live1.push(resumable("h1"))
	live1.push(resumable("h2")) // the newest handle wins
	live1.pushErr(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"})
	live2.push(transcript("after resume"))

	readUntil(t, browser, hasTranscript("after resume"))
	assert.Equal(t, []string{"h2"}, c.seen())
	assertRunning(t, s)
}

// A browser frame sent onto the dropped connection fails; while the session can
// resume, that frame is dropped instead of ending the session.
func TestVoiceSession_SendFailureWhileResumableKeepsSession(t *testing.T) {
	live1, live2 := newFakeLive(), newFakeLive()
	c := &fakeConnector{next: []*fakeLive{live2}}
	s, browser := startResumeSession(t, live1, c, true)

	live1.push(resumable("h1"))
	live1.push(transcript("ready"))
	readUntil(t, browser, hasTranscript("ready"))

	live1.sendErr.Store(true)
	require.NoError(t, browser.WriteJSON(VoiceBrowserMessage{Audio: []byte{1}}))
	require.Eventually(t, func() bool { return live1.sends.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	live1.pushErr(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"})
	live2.push(transcript("after resume"))
	readUntil(t, browser, hasTranscript("after resume"))
	require.NoError(t, browser.WriteJSON(VoiceBrowserMessage{Audio: []byte{2}}))
	require.Eventually(t, func() bool { return live2.audio.Load() == 1 }, 5*time.Second, 5*time.Millisecond, "audio reaches the resumed session")
	assertRunning(t, s)
}

// With resumption off, a receive error ends the session as it always has.
func TestVoiceSession_ResumptionOffEndsOnReceiveError(t *testing.T) {
	live1 := newFakeLive()
	c := &fakeConnector{next: []*fakeLive{newFakeLive()}}
	s, browser := startResumeSession(t, live1, c, false)

	live1.push(resumable("h1"))
	live1.push(&genai.LiveServerMessage{GoAway: &genai.LiveServerGoAway{TimeLeft: 50 * time.Second}})
	live1.pushErr(&websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "GoAway deadline exceeded"})

	sawDone, code := expectClose(t, browser)
	waitDone(t, s)
	assert.Empty(t, c.seen(), "no reconnect with resumption off")
	assert.True(t, sawDone)
	assert.Equal(t, websocket.CloseInternalServerErr, code)
}

// Without a resumable handle there is nothing to resume from: the session ends.
func TestVoiceSession_NoHandleEndsOnReceiveError(t *testing.T) {
	live1 := newFakeLive()
	c := &fakeConnector{next: []*fakeLive{newFakeLive()}}
	s, browser := startResumeSession(t, live1, c, true)

	live1.push(&genai.LiveServerMessage{SessionResumptionUpdate: &genai.LiveServerSessionResumptionUpdate{Resumable: false}})
	live1.push(&genai.LiveServerMessage{GoAway: &genai.LiveServerGoAway{TimeLeft: 50 * time.Second}})
	live1.pushErr(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"})

	_, code := expectClose(t, browser)
	waitDone(t, s)
	assert.Empty(t, c.seen())
	assert.Equal(t, websocket.CloseInternalServerErr, code)
}

// One reconnect attempt per drop: a rejected handle, or a resumed session that
// drops before it delivers anything, ends the session instead of looping.
func TestVoiceSession_ResumeAttemptsAreBounded(t *testing.T) {
	t.Run("connect fails", func(t *testing.T) {
		live1 := newFakeLive()
		c := &fakeConnector{}
		s, browser := startResumeSession(t, live1, c, true)

		live1.push(resumable("h1"))
		live1.pushErr(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"})

		_, code := expectClose(t, browser)
		waitDone(t, s)
		assert.Equal(t, []string{"h1"}, c.seen())
		assert.Equal(t, websocket.CloseInternalServerErr, code)
	})
	t.Run("resumed session drops at once", func(t *testing.T) {
		live1, live2 := newFakeLive(), newFakeLive()
		c := &fakeConnector{next: []*fakeLive{live2, newFakeLive()}}
		s, browser := startResumeSession(t, live1, c, true)

		live1.push(resumable("h1"))
		live1.pushErr(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"})
		live2.push(&genai.LiveServerMessage{SetupComplete: &genai.LiveServerSetupComplete{}})
		live2.pushErr(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"})

		_, code := expectClose(t, browser)
		waitDone(t, s)
		assert.Equal(t, []string{"h1"}, c.seen(), "no second attempt without a delivered message")
		assert.Equal(t, websocket.CloseInternalServerErr, code)
	})
	t.Run("a resumption update alone is not a delivered message", func(t *testing.T) {
		live1, live2 := newFakeLive(), newFakeLive()
		c := &fakeConnector{next: []*fakeLive{live2, newFakeLive()}}
		s, browser := startResumeSession(t, live1, c, true)

		live1.push(resumable("h1"))
		live1.pushErr(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"})
		live2.push(resumable("h2"))
		live2.pushErr(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"})

		_, code := expectClose(t, browser)
		waitDone(t, s)
		assert.Equal(t, []string{"h1"}, c.seen(), "a connection that fails right after an update is not retried forever")
		assert.Equal(t, websocket.CloseInternalServerErr, code)
	})
	t.Run("a delivered message allows the next drop one attempt", func(t *testing.T) {
		live1, live2, live3 := newFakeLive(), newFakeLive(), newFakeLive()
		c := &fakeConnector{next: []*fakeLive{live2, live3}}
		s, browser := startResumeSession(t, live1, c, true)

		live1.push(resumable("h1"))
		live1.pushErr(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"})
		live2.push(transcript("answer"))
		live2.push(resumable("h2"))
		live2.pushErr(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"})
		live3.push(transcript("second resume"))

		readUntil(t, browser, hasTranscript("second resume"))
		assert.Equal(t, []string{"h1", "h2"}, c.seen())
		assertRunning(t, s)
	})
}

// When the session ends, the browser gets Done and a close frame at once (1000
// for a normal end, 1011 for an upstream failure), even while it sends nothing;
// it used to wait for its next frame and then see 1006.
func TestVoiceSession_TeardownSendsCloseFrame(t *testing.T) {
	t.Run("hangup closes 1000", func(t *testing.T) {
		live1 := newFakeLive()
		s, browser := startResumeSession(t, live1, &fakeConnector{}, true)
		blockBrowserRead(t, browser, live1)
		s.closeHangup()

		sawDone, code := expectClose(t, browser)
		waitDone(t, s)
		assert.True(t, sawDone, "Done precedes the close frame")
		assert.Equal(t, websocket.CloseNormalClosure, code)
	})
	t.Run("upstream error closes 1011", func(t *testing.T) {
		live1 := newFakeLive()
		s, browser := startResumeSession(t, live1, &fakeConnector{}, false)
		blockBrowserRead(t, browser, live1)
		live1.pushErr(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"})

		sawDone, code := expectClose(t, browser)
		waitDone(t, s)
		assert.True(t, sawDone)
		assert.Equal(t, websocket.CloseInternalServerErr, code)
	})
}

// With resumption off the Live config is exactly the session's config; with it
// on, the server is asked for resumption updates, and a reconnect carries the
// handle. The session's own config is never changed.
func TestVoiceSession_LiveConfigAsksForResumptionOnlyWhenOn(t *testing.T) {
	base := &genai.LiveConnectConfig{SystemInstruction: genai.NewContentFromText("p", "user")}

	off := (&VoiceSession{liveConfig: base}).liveConfigFor("")
	assert.Nil(t, off.SessionResumption)
	assert.Equal(t, *base, *off)

	on := &VoiceSession{liveConfig: base, resumption: true}
	require.NotNil(t, on.liveConfigFor("").SessionResumption)
	assert.Equal(t, "", on.liveConfigFor("").SessionResumption.Handle)
	assert.Equal(t, "h1", on.liveConfigFor("h1").SessionResumption.Handle)
	assert.Nil(t, base.SessionResumption, "the base config is not mutated")
}

// The CR's sessionResumption wins over the --voice-session-resumption flag when set.
func TestVoiceManager_SessionResumptionCROverridesFlag(t *testing.T) {
	vm := newVoiceManagerWithConfig(&VoiceSessionConfig{})
	vm.sessionResumption = true
	assert.True(t, vm.effectiveSessionResumption(), "flag default when the CR leaves it unset")

	vm = newVoiceManagerWithConfig(&VoiceSessionConfig{SessionResumption: genai.Ptr(false)})
	vm.sessionResumption = true
	assert.False(t, vm.effectiveSessionResumption(), "CR false wins over flag true")

	vm = newVoiceManagerWithConfig(&VoiceSessionConfig{SessionResumption: genai.Ptr(true)})
	assert.True(t, vm.effectiveSessionResumption(), "CR true wins over flag false")
}

func TestConfigWatcher_ParsesSessionResumption(t *testing.T) {
	w := &ConfigWatcher{}
	cell := &mcell.MemoryCell{}
	cell.Name = SystemConfigCRName
	cell.Spec.Kind = "system_configuration"
	cell.Spec.VoiceConfig = &mcell.VoiceSessionConfig{SessionResumption: genai.Ptr(true)}
	w.update(cell)
	require.NotNil(t, w.Get().SessionResumption)
	assert.True(t, *w.Get().SessionResumption)

	cell.Spec.VoiceConfig = &mcell.VoiceSessionConfig{}
	w.update(cell)
	assert.Nil(t, w.Get().SessionResumption, "unset in the CR stays nil so the flag applies")
}

// ── Transparent replay (ADR-039, as built 2026-11-09) ─────────────────────────

func withTransparent(s *VoiceSession) { s.transparent = true }

// say has the browser send one audio frame per chunk.
func say(t *testing.T, browser *websocket.Conn, chunks ...string) {
	t.Helper()
	for _, c := range chunks {
		require.NoError(t, browser.WriteJSON(VoiceBrowserMessage{Audio: []byte(c)}))
	}
}

// waitReceived waits up to 5 s for l to have accepted n messages and fails
// listing what it did accept.
func waitReceived(t *testing.T, l *fakeLive, n int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); len(l.received()) < n && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	require.Len(t, l.received(), n, "messages accepted by the fake session")
}

// processed waits until the session has handled everything pushed so far. The
// marker is queued on every session given, so a check that follows runs (and
// fails on its own assertion) even if a regression has moved the session to
// another one; a marker left on an unread session is a harmless transcript.
func processed(t *testing.T, browser *websocket.Conn, marker string, sessions ...*fakeLive) {
	t.Helper()
	for _, l := range sessions {
		l.push(transcript(marker))
	}
	readUntil(t, browser, hasTranscript(marker))
}

func audioFrames(chunks ...string) []string {
	out := make([]string, len(chunks))
	for i, c := range chunks {
		out[i] = "audio:" + c
	}
	return out
}

// The 2026-11-09 live tests. First: a GoAway arrived while the user was asking a
// question, the session reconnected at once, and the part of the question
// streamed to the old connection was lost. Now the session stays on the old
// connection while the user speaks, until the turn has ended and the handle that
// covers it has arrived, then sends the new one what that handle has not
// consumed: every chunk reaches the model once, in order.
func TestVoiceSession_GoAwayWhileUserSpeaksResumesAfterTurnWithoutLoss(t *testing.T) {
	live1, live2 := newFakeLive(), newFakeLive()
	c := &fakeConnector{next: []*fakeLive{live2}}
	s, browser := startResumeSession(t, live1, c, true, withTransparent)

	live1.push(resumable("h1")) // the previous turn's handle
	processed(t, browser, "h1 held", live1)

	say(t, browser, "a1", "a2", "a3") // the user starts a question
	waitReceived(t, live1, 3)
	live1.push(&genai.LiveServerMessage{GoAway: &genai.LiveServerGoAway{TimeLeft: 30 * time.Second}})
	processed(t, browser, "goaway handled", live1, live2)
	assert.Empty(t, c.seen(), "no reconnect while the user speaks")

	say(t, browser, "a4", "a5") // the rest of the question
	waitReceived(t, live1, 5)
	live1.push(modelAudio())
	live1.push(turnComplete())
	processed(t, browser, "answered", live1, live2)
	assert.Empty(t, c.seen(), "no reconnect on the handle that predates the answer")

	say(t, browser, "a6") // microphone audio after the answer
	waitReceived(t, live1, 6)
	live1.push(resumableAt("h2", 5)) // the answer's handle: a1..a5 are in its state, a6 is not
	live2.push(transcript("after resume"))
	readUntil(t, browser, hasTranscript("after resume"))
	say(t, browser, "a7")
	waitReceived(t, live2, 2)

	assert.Equal(t, []string{"h2"}, c.seen())
	assert.True(t, c.config(0).SessionResumption.Transparent, "the reconnect asks for consumed indices")
	assert.Equal(t, audioFrames("a1", "a2", "a3", "a4", "a5", "a6"), live1.received())
	assert.Equal(t, audioFrames("a6", "a7"), live2.received(), "the unconsumed chunk is replayed before the new one")
	assert.Equal(t, audioFrames("a1", "a2", "a3", "a4", "a5", "a6", "a7"), append(live1.received()[:5], live2.received()...),
		"what h2 consumed from the old session plus what the new one got: every chunk once, in order")
	assertRunning(t, s)
}

// Second and third: the GoAway came after minutes of silence, and the reconnect
// sent buffered silence in one burst (65 s, then 10 s); Vertex ended the new
// stream with 429 ("1011 Resource has been exhausted") and the session ended. Now
// it reconnects at once and replays none of that silence.
func TestVoiceSession_GoAwayAfterLongSilenceResumesAtOnceWithoutReplay(t *testing.T) {
	live1, live2 := newFakeLive(), newFakeLive()
	c := &fakeConnector{next: []*fakeLive{live2}}
	s, browser := startResumeSession(t, live1, c, true, withTransparent)

	live1.push(resumable("h1"))
	processed(t, browser, "h1 held", live1)
	// 65 s of browser frames (8 ms, 256 bytes each), as in the live test. They
	// arrive here at once, so paceLocked sends the first audioLead of them and
	// drops the rest; the pinned update after them marks the reader done.
	frame := make([]byte, 256)
	const frames = 65 * 125
	for range frames {
		require.NoError(t, browser.WriteJSON(VoiceBrowserMessage{Audio: frame}))
	}
	require.NoError(t, browser.WriteJSON(VoiceBrowserMessage{Pinned: []string{"read"}}))
	require.Eventually(t, func() bool { return len(s.getPinnedIDs()) == 1 }, 5*time.Second, time.Millisecond)
	require.NotEmpty(t, live1.received())

	live1.push(&genai.LiveServerMessage{GoAway: &genai.LiveServerGoAway{TimeLeft: 30 * time.Second}})
	live2.push(transcript("after resume"))
	readUntil(t, browser, hasTranscript("after resume"))

	assert.Equal(t, []string{"h1"}, c.seen(), "no waiting for a turn in silence")
	assert.Empty(t, live2.received(), "silence is not replayed")
	assertRunning(t, s)
}

// A user who speaks over the answer stops it (Interrupted): the model is idle
// again, so a GoAway then reconnects at once.
func TestVoiceSession_GoAwayAfterInterruptionResumesAtOnce(t *testing.T) {
	live1, live2 := newFakeLive(), newFakeLive()
	c := &fakeConnector{next: []*fakeLive{live2}}
	s, browser := startResumeSession(t, live1, c, true)

	live1.push(resumable("h1"))
	live1.push(modelAudio())
	live1.push(&genai.LiveServerMessage{ServerContent: &genai.LiveServerContent{Interrupted: true}})
	live1.push(&genai.LiveServerMessage{GoAway: &genai.LiveServerGoAway{TimeLeft: 30 * time.Second}})
	live2.push(transcript("after resume"))
	readUntil(t, browser, hasTranscript("after resume"))

	assert.Equal(t, []string{"h1"}, c.seen())
	assertRunning(t, s)
}

// A drop without a GoAway (1006) in the middle of an utterance: the new session
// gets every client message after the held handle's index, in order (audio,
// audioStreamEnd), before anything sent after the reconnect. The greeting is
// client message 1, so the indices count every kind of message.
func TestVoiceSession_DropMidUtteranceReplaysAfterConsumedIndex(t *testing.T) {
	live1, live2 := newFakeLive(), newFakeLive()
	c := &fakeConnector{next: []*fakeLive{live2}}
	s, browser := startResumeSession(t, live1, c, true, withTransparent, func(s *VoiceSession) { s.greeting = "hello" })
	waitReceived(t, live1, 1)

	say(t, browser, "a1", "a2", "a3")
	waitReceived(t, live1, 4)
	live1.push(resumableAt("h1", 3)) // covers the greeting, a1 and a2
	processed(t, browser, "h1 held", live1)
	say(t, browser, "a4")
	require.NoError(t, browser.WriteJSON(VoiceBrowserMessage{AudioStreamEnd: true}))
	waitReceived(t, live1, 6)

	live1.pushErr(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"})
	live2.push(transcript("after resume"))
	readUntil(t, browser, hasTranscript("after resume"))
	say(t, browser, "a5")
	waitReceived(t, live2, 4)

	assert.Equal(t, []string{"h1"}, c.seen())
	assert.Equal(t, []string{"audio:a3", "audio:a4", "end", "audio:a5"}, live2.received(), "no greeting again, nothing lost")
	assert.Equal(t, []string{"content", "audio:a1", "audio:a2", "audio:a3", "audio:a4", "end", "audio:a5"}, append(live1.received()[:3], live2.received()...))
	assertRunning(t, s)
}

// A resumable handle drops the kept messages up to its consumed index. A
// non-resumable update leaves them, and one without an index (the server
// ignored transparent) covers them all, so nothing the handle holds is resent.
func TestVoiceSession_ReplayBufferTrimmedByConsumedIndex(t *testing.T) {
	s := &VoiceSession{logger: zerolog.Nop(), live: newFakeLive(), transparent: true}
	for range 5 {
		require.NoError(t, s.sendLive(10, func(l liveConn) error {
			return l.SendRealtimeInput(genai.LiveRealtimeInput{Audio: &genai.Blob{Data: []byte{1}}})
		}))
	}
	update := func(u *genai.LiveServerSessionResumptionUpdate) {
		require.NoError(t, s.handleGeminiMessage(context.Background(), &genai.LiveServerMessage{SessionResumptionUpdate: u}))
	}
	kept := func() (idx []int64, bytes int) {
		s.liveMu.Lock()
		defer s.liveMu.Unlock()
		for _, m := range s.replay {
			idx = append(idx, m.idx)
		}
		return idx, s.replayBytes
	}

	update(&genai.LiveServerSessionResumptionUpdate{Resumable: false, LastConsumedClientMessageIndex: 4})
	idx, bytes := kept()
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, idx, "a point the server cannot resume from covers nothing")
	assert.Equal(t, 50, bytes)

	update(&genai.LiveServerSessionResumptionUpdate{NewHandle: "h1", Resumable: true, LastConsumedClientMessageIndex: 3})
	idx, bytes = kept()
	assert.Equal(t, []int64{4, 5}, idx)
	assert.Equal(t, 20, bytes)

	update(&genai.LiveServerSessionResumptionUpdate{NewHandle: "h2", Resumable: true})
	idx, bytes = kept()
	assert.Empty(t, idx, "an update without an index covers everything sent")
	assert.Zero(t, bytes)
}

// The buffer keeps at most replayMaxMessages and replayMaxBytes of microphone
// frames, the newest ones, and counts what it dropped. Other messages (text,
// tool responses) are not dropped for the bound.
func TestVoiceSession_ReplayBufferIsBounded(t *testing.T) {
	frame := func(size int) sentMsg {
		return sentMsg{bytes: size, audio: time.Microsecond, send: func(l liveConn) error {
			return l.SendRealtimeInput(genai.LiveRealtimeInput{Audio: &genai.Blob{Data: []byte{1}}})
		}}
	}
	send := func(s *VoiceSession, n, size int) {
		s.liveMu.Lock()
		defer s.liveMu.Unlock()
		for range n {
			require.NoError(t, s.sendMsgLocked(context.Background(), frame(size)))
		}
	}
	t.Run("messages", func(t *testing.T) {
		s := &VoiceSession{logger: zerolog.Nop(), live: newFakeLive(), transparent: true}
		send(s, replayMaxMessages+10, 1)
		s.liveMu.Lock()
		defer s.liveMu.Unlock()
		require.Len(t, s.replay, replayMaxMessages)
		assert.Equal(t, int64(11), s.replay[0].idx, "the oldest ten are dropped")
		assert.Equal(t, int64(replayMaxMessages+10), s.replay[len(s.replay)-1].idx)
		assert.Equal(t, replayMaxMessages, s.replayBytes)
		assert.Equal(t, 10, s.replayLost, "a reconnect now reports the ten")
	})
	t.Run("bytes", func(t *testing.T) {
		s := &VoiceSession{logger: zerolog.Nop(), live: newFakeLive(), transparent: true}
		send(s, 6, replayMaxBytes/4)
		s.liveMu.Lock()
		defer s.liveMu.Unlock()
		require.Len(t, s.replay, 4)
		assert.Equal(t, int64(3), s.replay[0].idx)
		assert.Equal(t, replayMaxBytes, s.replayBytes)
	})
	t.Run("a tool response outlives the audio after it", func(t *testing.T) {
		s := &VoiceSession{logger: zerolog.Nop(), live: newFakeLive(), transparent: true}
		require.NoError(t, s.sendLive(100, func(l liveConn) error {
			return l.SendToolResponse(genai.LiveToolResponseInput{})
		}))
		send(s, replayMaxBytes/256+50, 256)
		s.liveMu.Lock()
		defer s.liveMu.Unlock()
		require.NotEmpty(t, s.replay)
		assert.Equal(t, int64(1), s.replay[0].idx, "the tool response is still kept")
		assert.Zero(t, s.replay[0].audio)
		assert.Equal(t, 50, s.replayLost, "only microphone frames were dropped")
	})
	t.Run("other messages have their own hard cap", func(t *testing.T) {
		s := &VoiceSession{logger: zerolog.Nop(), live: newFakeLive(), transparent: true}
		for range replayMaxContent + 5 {
			require.NoError(t, s.sendLive(10, func(l liveConn) error {
				return l.SendClientContent(genai.LiveSendClientContentParameters{})
			}))
		}
		s.liveMu.Lock()
		defer s.liveMu.Unlock()
		require.Len(t, s.replay, replayMaxContent, "a client cannot grow the buffer without limit")
		assert.Equal(t, int64(6), s.replay[0].idx, "the oldest five went")
		assert.Equal(t, 5, s.replayLost)
		assert.Equal(t, replayMaxContent, s.contentCount)
		assert.Equal(t, replayMaxContent*10, s.contentBytes)
	})
}

// Speech the server has transcribed since the last TurnComplete means the user's
// turn is open even in a pause: a GoAway then waits for the turn to end.
func TestVoiceSession_GoAwayAfterTranscribedSpeechWaitsForTheTurn(t *testing.T) {
	live1, live2 := newFakeLive(), newFakeLive()
	c := &fakeConnector{next: []*fakeLive{live2}}
	s, browser := startResumeSession(t, live1, c, true)

	live1.push(resumable("h1"))
	live1.push(&genai.LiveServerMessage{ServerContent: &genai.LiveServerContent{InputTranscription: &genai.Transcription{Text: "what does the policy"}}})
	live1.push(&genai.LiveServerMessage{GoAway: &genai.LiveServerGoAway{TimeLeft: 30 * time.Second}})
	processed(t, browser, "goaway handled", live1, live2)
	assert.Empty(t, c.seen(), "no reconnect in the middle of the user's turn")

	live1.push(modelAudio())
	live1.push(turnComplete())
	live1.push(resumable("h2"))
	live2.push(transcript("after resume"))
	readUntil(t, browser, hasTranscript("after resume"))
	assert.Equal(t, []string{"h2"}, c.seen())
	assertRunning(t, s)
}

// A replay skips the silent microphone frames before the first one with speech
// and sends the speech at the pace it was spoken. The browser frames that queued
// behind it are paced too: silent ones are dropped while behind, so the backlog
// drains instead of going out as a burst.
func TestVoiceSession_ReplaySkipsLeadingSilenceAndKeepsThePace(t *testing.T) {
	live := newFakeLive()
	s := &VoiceSession{logger: zerolog.Nop(), live: live, transparent: true}
	quiet, voiced := make([]byte, 256), make([]byte, 256)
	for i := 0; i < len(voiced); i += 2 {
		voiced[i+1] = 0x10 // 4096
	}
	// Kept as if sent earlier on the old connection.
	for range 30 {
		s.keepLocked(audioMsg(quiet))
	}
	for range 100 {
		s.keepLocked(audioMsg(voiced))
	}
	for range 5 {
		s.keepLocked(audioMsg(quiet))
	}

	s.live = newFakeLive()
	start := time.Now()
	msgs, bytes, skipped, err := s.replayLocked(context.Background())
	took := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, 30, skipped, "the leading silence")
	assert.Equal(t, 100, msgs, "the speech; the silence after it is behind the clock and dropped")
	assert.Equal(t, 100*256, bytes)
	assert.GreaterOrEqual(t, took, 800*time.Millisecond-audioLead-20*time.Millisecond, "100 frames of 8 ms at real-time pace")
	assert.Len(t, s.replay, 100, "kept again on the new connection until a handle covers them")

	// The backlog: browser frames that queued behind the replay arrive at once.
	for range 50 {
		require.NoError(t, s.sendMsgLocked(context.Background(), audioMsg(quiet)))
	}
	assert.Len(t, s.replay, 100, "silent frames behind the clock are dropped, not sent")
}

// A frame waiting for its pace gives up when the session ends.
func TestVoiceSession_PacedSendStopsOnCancel(t *testing.T) {
	s := &VoiceSession{logger: zerolog.Nop(), live: newFakeLive(), transparent: true}
	voiced := []byte{0, 0x10, 0, 0x10}
	s.audioClock = time.Now().Add(10 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	start := time.Now()
	err := s.sendMsgLocked(ctx, audioMsg(voiced))
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), 2*time.Second, "does not wait out the clock")
	assert.Empty(t, s.replay, "nothing sent, nothing kept")
}

func TestIsQuiet(t *testing.T) {
	frame := func(v int16) []byte { return []byte{byte(uint16(v)), byte(uint16(v) >> 8), 0, 0} }
	assert.True(t, isQuiet(make([]byte, 256)))
	assert.True(t, isQuiet(frame(quietPeak-1)))
	assert.True(t, isQuiet(frame(-quietPeak+1)))
	assert.False(t, isQuiet(frame(quietPeak)))
	assert.False(t, isQuiet(frame(-quietPeak)))
	assert.False(t, isQuiet([]byte("a1")), "the test chunks used above count as speech")
}
