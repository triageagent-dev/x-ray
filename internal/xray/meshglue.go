package xray

import (
	"context"
	"encoding/json"
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
		acked, sent := mm.BroadcastWait(ctx, frameCode, codeFrame{
			Code: code, Challenge: c.challenge, ClientID: c.clientID, RedirectURI: c.redirectURI,
			Resource: c.resource, Sub: c.sub, Scopes: c.scopes, ExpiresAt: c.expiresAt.Unix(),
		})
		if acked < sent {
			a.logger.Warn().Int("acked", acked).Int("peers", sent).Msg("mesh: an authorization code did not reach every peer; a /token call routed to one may fail")
		}
	}
	a.dropCode = func(code string) { mm.Broadcast(frameCodeRedeem, code) }
	mm.Handle(frameCode, func(_ string, data json.RawMessage) {
		var f codeFrame
		if json.Unmarshal(data, &f) != nil || f.Code == "" {
			return
		}
		a.putCode(f.Code, authCode{
			challenge: f.Challenge, clientID: f.ClientID, redirectURI: f.RedirectURI,
			resource: f.Resource, sub: f.Sub, scopes: f.Scopes, expiresAt: time.Unix(f.ExpiresAt, 0),
		})
	})
	mm.Handle(frameCodeRedeem, func(_ string, data json.RawMessage) {
		var code string
		if json.Unmarshal(data, &code) == nil {
			a.forgetCode(code)
		}
	})
}
