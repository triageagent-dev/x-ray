package xray

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
	"github.com/triageagent-dev/x-ray/v2/pkg/mesh"
	"github.com/triageagent-dev/x-ray/v2/pkg/meshsync"
)

// The state that must be the same on every replica and is not in a shared store
// at the moment a request needs it rides the mesh (ADR-068): the counters and the
// daily token spend, the logins, the OAuth client registrations and the
// authorization codes. The cell still holds what survives a restart; the mesh
// only closes the gap until the next flush.
const (
	frameLedger      = "ledger"
	frameSessions    = "sessions"
	frameClients     = "oauth_clients"
	frameCode        = "auth_code"
	frameCodeRedeem  = "auth_code_redeemed"
	frameCodeClaim   = "auth_code_claim" // a request: the issuing replica decides who redeems a code
	frameLoginClaim  = "login_state"     // a request: the replica that started a sign-in completes it
	codeShareTimeout = time.Second
)

type sessionsFrame struct {
	Rows []mcell.UserSession `json:"rows,omitempty"`
	Drop []string            `json:"drop,omitempty"`
}

type codeFrame struct {
	Code        string   `json:"code"`
	Challenge   string   `json:"challenge"`
	ClientID    string   `json:"clientId"`
	RedirectURI string   `json:"redirectUri"`
	Resource    string   `json:"resource"`
	Sub         string   `json:"sub"`
	Scopes      []string `json:"scopes"`
	ExpiresAt   int64    `json:"expiresAt"` // unix seconds
	// MAC proves the frame came from a replica that holds the OAuth signing secret,
	// not only the mesh certificate: a code carries the subject and scopes of the
	// token it will be exchanged for, so a peer that could mint one could mint a
	// token for anyone.
	MAC string `json:"mac"`
}

// codeMACKey is derived from the signing key every replica shares (ADR-050), under
// its own label so it is not a key anything else uses.
func (a *mcpAuthorizer) codeMACKey() []byte {
	h := hmac.New(sha256.New, a.key.Seed())
	h.Write([]byte("xray-mesh-auth-code-v1"))
	return h.Sum(nil)
}

func (a *mcpAuthorizer) codeMAC(f codeFrame) string {
	h := hmac.New(sha256.New, a.codeMACKey())
	for _, part := range []string{f.Code, f.Challenge, f.ClientID, f.RedirectURI, f.Resource, f.Sub, strings.Join(f.Scopes, " "), strconv.FormatInt(f.ExpiresAt, 10)} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// JoinMesh shares this replica's counters, token spend, logins and OAuth client
// registrations with its peers as self. Call before the mesh starts and before
// the replica serves.
func (m *MemoryStore) JoinMesh(mm *mesh.Mesh, self string, logger zerolog.Logger) {
	m.joinFleet(self, func(recs []meshsync.Record) { mm.Broadcast(frameLedger, recs) })

	mm.Handle(frameLedger, func(origin string, data json.RawMessage) {
		var recs []meshsync.Record
		if json.Unmarshal(data, &recs) != nil {
			return
		}
		for i := range recs {
			recs[i].Origin = origin
		}
		m.applyLedger(recs, time.Now())
	})

	sessions.mu.Lock()
	sessions.onPublish = func(row mcell.UserSession) {
		mm.Broadcast(frameSessions, sessionsFrame{Rows: []mcell.UserSession{row}})
	}
	sessions.onDrop = func(h string) { mm.Broadcast(frameSessions, sessionsFrame{Drop: []string{h}}) }
	sessions.mu.Unlock()
	mm.Handle(frameSessions, func(_ string, data json.RawMessage) {
		var f sessionsFrame
		if json.Unmarshal(data, &f) != nil {
			return
		}
		if len(f.Rows) > 0 {
			if r := sessions.restoreSessions(f.Rows); r.undecryptable > 0 {
				logger.Warn().Int("undecryptable", r.undecryptable).Msg("mesh: a peer's session rows could not be decrypted (key mismatch)")
			}
		}
		for _, h := range f.Drop {
			sessions.dropHMAC(h)
		}
		m.markStatsDirty()
	})

	oauthClients.mu.Lock()
	oauthClients.onAdd = func(r mcell.OAuthClientReg) { mm.Broadcast(frameClients, []mcell.OAuthClientReg{r}) }
	oauthClients.mu.Unlock()
	mm.Handle(frameClients, func(_ string, data json.RawMessage) {
		var rows []mcell.OAuthClientReg
		if json.Unmarshal(data, &rows) != nil {
			return
		}
		if oauthClients.restore(rows) > 0 {
			m.markStatsDirty()
		}
	})

	// A peer that connects, or reconnects after a restart, is sent the level
	// state it may have missed.
	mm.OnConnect(func(send func(string, any)) {
		send(frameLedger, m.ledgerState(time.Now()))
		send(frameSessions, sessionsFrame{Rows: sessions.snapshotSessions()})
		send(frameClients, oauthClients.snapshot())
	})
}

// RunMesh sends the counters and resends this replica's totals until ctx ends:
// the counters every few seconds, the totals every meshsync.ResendInterval so a
// counter that went quiet, or a peer that missed a frame, converges.
func (m *MemoryStore) RunMesh(ctx context.Context, mm *mesh.Mesh) {
	pub := time.NewTicker(counterPublishEvery)
	resend := time.NewTicker(meshsync.ResendInterval)
	defer pub.Stop()
	defer resend.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-pub.C:
			m.publishCounters()
		case <-resend.C:
			if recs := m.ledgerState(time.Now()); len(recs) > 0 {
				mm.Broadcast(frameLedger, recs)
			}
		}
	}
}

// JoinMesh lets an authorization code issued here be redeemed on any replica.
// /authorize answers only once the peers have the code, because the client's
// next call is /token and the service may route it to any of them.
func (a *mcpAuthorizer) JoinMesh(mm *mesh.Mesh) {
	a.shareCode = func(code string, c authCode) {
		ctx, cancel := context.WithTimeout(context.Background(), codeShareTimeout)
		defer cancel()
		f := codeFrame{
			Code: code, Challenge: c.challenge, ClientID: c.clientID, RedirectURI: c.redirectURI,
			Resource: c.resource, Sub: c.sub, Scopes: c.scopes, ExpiresAt: c.expiresAt.Unix(),
		}
		f.MAC = a.codeMAC(f)
		acked, sent := mm.BroadcastWait(ctx, frameCode, f)
		if acked < sent {
			a.logger.Warn().Int("acked", acked).Int("peers", sent).Msg("mesh: an authorization code did not reach every peer; a /token call routed to one may fail")
		}
	}
	a.dropCode = func(code string) { mm.Broadcast(frameCodeRedeem, code) }
	a.claimCode = func(code, owner string) (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), codeShareTimeout*2)
		defer cancel()
		var granted bool
		if err := mm.Request(ctx, owner, frameCodeClaim, code, &granted); err != nil {
			return false, err
		}
		return granted, nil
	}
	mm.HandleRequest(frameCodeClaim, func(_ string, data json.RawMessage) any {
		var code string
		if json.Unmarshal(data, &code) != nil {
			return false
		}
		granted := a.claim(code)
		if granted {
			mm.Broadcast(frameCodeRedeem, code) // the other copies go
		}
		return granted
	})
	mm.Handle(frameCode, func(origin string, data json.RawMessage) {
		var f codeFrame
		if json.Unmarshal(data, &f) != nil || f.Code == "" {
			return
		}
		if !hmac.Equal([]byte(f.MAC), []byte(a.codeMAC(f))) {
			a.logger.Warn().Str("origin", origin).Msg("mesh: an authorization code from a peer failed its MAC; dropped")
			return
		}
		a.putCode(f.Code, authCode{
			challenge: f.Challenge, clientID: f.ClientID, redirectURI: f.RedirectURI,
			resource: f.Resource, sub: f.Sub, scopes: f.Scopes, expiresAt: time.Unix(f.ExpiresAt, 0),
			owner: origin,
		})
	})
	mm.Handle(frameCodeRedeem, func(_ string, data json.RawMessage) {
		var code string
		if json.Unmarshal(data, &code) == nil {
			a.forgetCode(code)
		}
	})
}

type loginClaim struct {
	Provider string `json:"provider"`
	State    string `json:"state"`
}

type loginClaimReply struct {
	OK        bool   `json:"ok"`
	UIOrigin  string `json:"uiOrigin,omitempty"`
	ExpiresAt int64  `json:"expiresAt,omitempty"` // unix seconds
}

// JoinMesh lets a sign-in started on this replica complete on another: the
// provider's callback may reach any replica, which asks the one the state names.
// Call before the mesh starts.
func (s *Server) JoinMesh(mm *mesh.Mesh, self string) {
	joinLoginMesh(mm, self, s.logger, s.google, s.github, s.azure)
}

func joinLoginMesh(mm *mesh.Mesh, self string, logger zerolog.Logger, g *googleAuth, gh *githubAuth, az *azureAuth) {
	consume := map[string]func(string) (loginState, bool){"google": g.consumeState, "github": gh.consumeState, "entra": az.consumeState}
	mm.HandleRequest(frameLoginClaim, func(_ string, data json.RawMessage) any {
		var c loginClaim
		if json.Unmarshal(data, &c) != nil || consume[c.Provider] == nil {
			return loginClaimReply{}
		}
		st, ok := consume[c.Provider](c.State) // single use: the first replica to ask gets it
		return loginClaimReply{OK: ok, UIOrigin: st.uiOrigin, ExpiresAt: st.expiresAt.Unix()}
	})
	claim := func(provider string) func(owner, state string) (loginState, bool) {
		return func(owner, state string) (loginState, bool) {
			ctx, cancel := context.WithTimeout(context.Background(), codeShareTimeout*2)
			defer cancel()
			var r loginClaimReply
			if err := mm.Request(ctx, owner, frameLoginClaim, loginClaim{Provider: provider, State: state}, &r); err != nil || !r.OK {
				if err != nil {
					logger.Warn().Err(err).Str("owner", owner).Msg("mesh: the replica that started a sign-in could not be asked; the user signs in again")
				}
				return loginState{}, false // the owner is gone or refused: sign in again
			}
			return loginState{expiresAt: time.Unix(r.ExpiresAt, 0), uiOrigin: r.UIOrigin}, true
		}
	}
	g.self, g.claim = self, claim("google")
	gh.self, gh.claim = self, claim("github")
	az.self, az.claim = self, claim("entra")
}
