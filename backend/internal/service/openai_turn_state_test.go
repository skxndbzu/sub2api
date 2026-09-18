package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

type otsTestStore struct {
	OpenAITurnStateStore
	records map[OpenAITurnStateKey]*OpenAITurnStateRecord
	reads   []OpenAITurnStateKey
}

func (s *otsTestStore) Read(_ context.Context, k OpenAITurnStateKey, _, _ string) (*OpenAITurnStateRecord, time.Duration, error) {
	s.reads = append(s.reads, k)
	r := s.records[k]
	if r == nil {
		return nil, 0, nil
	}
	return r, time.Until(r.ExpiresAt), nil
}
func (s *otsTestStore) LeaseValid(context.Context, *OpenAITurnStateLease) (bool, error) {
	return true, nil
}

type otsTestAccounts struct {
	AccountRepository
	account *Account
}

func (s *otsTestAccounts) GetByID(context.Context, int64) (*Account, error) { return s.account, nil }

type otsTestProxies struct {
	ProxyRepository
	nodes map[int64]*Proxy
}

func (s *otsTestProxies) GetByID(_ context.Context, id int64) (*Proxy, error) {
	return s.nodes[id], nil
}

func otsAccount(id int64) *Account {
	cfg := DefaultOpenAITurnStateConfig()
	cfg.Enabled = true
	cfg.ProxyIDs = []int64{1, 2, 3}
	cfg.Targets = []OpenAITurnStateTarget{{Model: "model-a", ServiceTiers: []string{"omitted", "priority"}}, {Model: "model-b", ServiceTiers: []string{"omitted"}}}
	return &Account{ID: id, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Credentials: map[string]any{"access_token": "test-only-oauth-token", "chatgpt_account_id": "test-account"}, Extra: map[string]any{OpenAITurnStateExtraKey: cfg}}
}

func TestOpenAITurnStateHTTPPriorityMappingAndAccountRetry(t *testing.T) {
	a := otsAccount(1)
	b := otsAccount(2)
	ctx := context.Background()
	key := OpenAITurnStateKey{1, "model-a", "priority"}
	value := otsTestToken(time.Now(), 'z')
	store := &otsTestStore{records: map[OpenAITurnStateKey]*OpenAITurnStateRecord{key: {State: value, StateLength: 292, StateDigest: turnStateDigest(value), ExpiresAt: time.Now().Add(time.Hour), Generation: "v1"}}}
	gateway := &OpenAIGatewayService{}
	svc := NewOpenAITurnStateService(store, &otsTestAccounts{account: a}, nil, gateway)
	defer svc.Stop()
	gateway.turnStates = svc
	c, _ := newTurnStateTestContext(t, 7, "client-session")
	c.Request.Header.Set(openAICodexTurnStateHeader, "client-history")
	body := []byte(`{"model":"model-a","service_tier":"priority","previous_response_id":"keep-this","input":[{"role":"user","content":"keep-content"}]}`)
	before := append([]byte(nil), body...)
	req, err := gateway.buildUpstreamRequest(ctx, c, a, body, "token", true, "", true)
	require.NoError(t, err)
	require.Equal(t, value, req.Header.Get(openAICodexTurnStateHeader))
	require.Equal(t, "model=model-a;tier=priority", req.Header.Get(openAICodexRoutingHintHeader))
	passthrough, err := gateway.buildUpstreamRequestOpenAIPassthrough(ctx, c, a, body, "token")
	require.NoError(t, err)
	require.Equal(t, value, passthrough.Header.Get(openAICodexTurnStateHeader))
	retry, err := gateway.buildUpstreamRequest(ctx, c, b, body, "token2", true, "", true)
	require.NoError(t, err)
	require.Equal(t, "client-history", retry.Header.Get(openAICodexTurnStateHeader))
	require.Equal(t, "client-history", c.Request.Header.Get(openAICodexTurnStateHeader))
	require.Equal(t, before, body)
	cfg := DefaultOpenAITurnStateConfig()
	a.Extra[OpenAITurnStateExtraKey] = cfg
	reads := len(store.reads)
	disabled, err := gateway.buildUpstreamRequest(ctx, c, a, body, "token", true, "", true)
	require.NoError(t, err)
	require.Equal(t, "client-history", disabled.Header.Get(openAICodexTurnStateHeader))
	require.Len(t, store.reads, reads)
}

type otsLocalUpstream struct {
	client *http.Client
	base   *url.URL
}

func (s *otsLocalUpstream) Do(req *http.Request, proxy string, _ int64, _ int) (*http.Response, error) {
	clone := req.Clone(req.Context())
	u := *req.URL
	u.Scheme = s.base.Scheme
	u.Host = s.base.Host
	clone.URL = &u
	clone.Host = ""
	clone.Header = req.Header.Clone()
	clone.Header.Set("X-Test-Proxy", proxy)
	return s.client.Do(clone)
}
func (s *otsLocalUpstream) DoWithTLS(r *http.Request, p string, a int64, c int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return s.Do(r, p, a, c)
}

func TestOpenAITurnStateProbeSelects292AndClosesStream(t *testing.T) {
	for name, rejected := range map[string]string{
		"wrong_length":      strings.Repeat("L", 312),
		"invalid_timestamp": strings.Repeat("Q", 292),
		"expired":           otsTestToken(time.Now().Add(-2*time.Hour), 'E'),
	} {
		t.Run(name, func(t *testing.T) {
			issuedAt := time.Now().UTC().Add(-20 * time.Minute).Truncate(time.Second)
			validToken := otsTestToken(issuedAt, 'Q')
			var mu sync.Mutex
			var bodies [][]byte
			var seenStates []string
			closed := make(chan struct{}, 3)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/cdn-cgi/trace" {
					if strings.Contains(r.Header.Get("X-Test-Proxy"), "node1") {
						_, _ = io.WriteString(w, "ip=203.0.113.1\n")
					} else {
						_, _ = io.WriteString(w, "ip=203.0.113.2\n")
					}
					return
				}
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				bodies = append(bodies, body)
				seenStates = append(seenStates, r.Header.Get(openAICodexTurnStateHeader))
				mu.Unlock()
				value := rejected
				if strings.Contains(r.Header.Get("X-Test-Proxy"), "node2") {
					value = validToken
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set(openAICodexTurnStateHeader, value)
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				closed <- struct{}{}
			}))
			defer server.Close()
			u, _ := url.Parse(server.URL)
			a := otsAccount(1)
			store := &otsTestStore{}
			repo := &otsTestAccounts{account: a}
			gateway := &OpenAIGatewayService{accountRepo: repo, httpUpstream: &otsLocalUpstream{server.Client(), u}, openAITokenProvider: NewOpenAITokenProvider(repo, nil, nil)}
			proxies := &otsTestProxies{nodes: map[int64]*Proxy{1: {ID: 1, Protocol: "http", Host: "node1", Port: 8080, Status: StatusActive}, 2: {ID: 2, Protocol: "http", Host: "node2", Port: 8080, Status: StatusActive}, 3: {ID: 3, Protocol: "http", Host: "node3", Port: 8080, Status: StatusActive}}}
			svc := NewOpenAITurnStateService(store, repo, proxies, gateway)
			defer svc.Stop()
			gateway.turnStates = svc
			cfg, err := ParseOpenAITurnStateConfig(a.Extra)
			require.NoError(t, err)
			key := OpenAITurnStateKey{1, "model-a", "omitted"}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			r, m := svc.probeTarget(ctx, a, cfg, key, &OpenAITurnStateLease{Key: key})
			require.NotNil(t, r)
			require.Equal(t, validToken, r.State)
			require.Equal(t, issuedAt, r.IssuedAt)
			require.Equal(t, issuedAt.Add(time.Hour), r.ExpiresAt)
			require.Equal(t, r.ExpiresAt.Add(-10*time.Minute), r.RefreshAt)
			require.Equal(t, 2, m.Attempts)
			require.Equal(t, int64(2), r.SourceProxyID)
			for i := 0; i < 2; i++ {
				select {
				case <-closed:
				case <-ctx.Done():
					t.Fatal("probe waited for the SSE body instead of closing the stream")
				}
			}
			mu.Lock()
			defer mu.Unlock()
			require.Len(t, bodies, 2)
			require.Equal(t, []string{"", ""}, seenStates)
			for _, body := range bodies {
				require.NotContains(t, string(body), "previous_response_id")
				require.NotContains(t, string(body), "client-history")
				var payload map[string]any
				require.NoError(t, json.Unmarshal(body, &payload))
				require.Equal(t, "model-a", payload["model"])
				require.Equal(t, true, payload["stream"])
			}
			require.Nil(t, a.ProxyID, "probing must not edit the business proxy")
		})
	}
}

func TestOpenAITurnStateWSPoolUsesHardStateAndModelCompatibility(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	pool := newOpenAIWSConnPool(cfg)
	defer pool.Close()
	dialer := &openAIWSCountingDialer{}
	pool.setClientDialerForTest(dialer)
	a := otsAccount(1)
	d := OpenAITurnStateDecision{Managed: true, Key: OpenAITurnStateKey{1, "model-a", "omitted"}, Source: "probe_cache", Generation: "v1", StateDigest: "a"}
	req := openAIWSAcquireRequest{Account: a, WSURL: "wss://example.com/responses", Headers: http.Header{}, TurnState: d}
	first, err := pool.Acquire(context.Background(), req)
	require.NoError(t, err)
	firstID := first.ConnID()
	first.Release()
	same, err := pool.Acquire(context.Background(), req)
	require.NoError(t, err)
	require.True(t, same.Reused())
	require.Equal(t, firstID, same.ConnID())
	same.Release()
	req.TurnState.Generation = "v2"
	req.TurnState.StateDigest = "b"
	next, err := pool.Acquire(context.Background(), req)
	require.NoError(t, err)
	require.NotEqual(t, firstID, next.ConnID())
	nextID := next.ConnID()
	next.Release()
	req.TurnState.Key.Model = "model-b"
	req.PreferredConnID = nextID
	req.ForcePreferredConn = true
	_, err = pool.Acquire(context.Background(), req)
	require.ErrorIs(t, err, errOpenAIWSPreferredConnUnavailable)
	req.ForcePreferredConn = false
	other, err := pool.Acquire(context.Background(), req)
	require.NoError(t, err)
	require.NotEqual(t, nextID, other.ConnID())
	other.Release()
}

func TestOpenAITurnStateConfigAndRawResponseLogging(t *testing.T) {
	a := otsAccount(1)
	before := openAITurnStateIdentity(a, a)
	a.Credentials["access_token"] = "rotated"
	a.Credentials["expires_at"] = time.Now().Add(time.Hour).Unix()
	require.Equal(t, before, openAITurnStateIdentity(a, a))
	a.Credentials["chatgpt_account_id"] = "different"
	require.NotEqual(t, before, openAITurnStateIdentity(a, a))
	merged := MergeOpenAITurnStateExtra(a.Extra, map[string]any{OpenAITurnStateExtraKey: map[string]any{"max_attempts": 3}})
	cfg, err := ParseOpenAITurnStateConfig(merged)
	require.NoError(t, err)
	require.Equal(t, 3, cfg.MaxAttempts)
	require.Len(t, cfg.Targets, 2)
	require.True(t, cfg.Enabled)
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)
	t.Setenv("OPENAI_TURN_STATE_LOG_VALUES", "")
	value := strings.Repeat("s", 292)
	headers := http.Header{}
	headers.Set(openAICodexTurnStateHeader, value)
	logOpenAITurnStateResponse(context.Background(), a, headers, "test")
	require.NotContains(t, output.String(), value)
	output.Reset()
	cfg.LogResponseValues = true
	a.Extra[OpenAITurnStateExtraKey] = cfg
	logOpenAITurnStateResponse(context.Background(), a, headers, "test")
	require.Contains(t, output.String(), `"x-codex-turn-state":"`+value+`"`)
	require.NotContains(t, output.String(), "rotated")
}

func TestOpenAITurnStateWSRejectsOnlyIncompatibleFrame(t *testing.T) {
	inner := &fakePassthroughFrameConn{reads: [][]byte{[]byte("different-route"), []byte("original-route")}}
	wrapped := &openAIWSPolicyEnforcingFrameConn{inner: inner, filter: func(_ coderws.MessageType, payload []byte) ([]byte, *OpenAIFastBlockedError, error) {
		if string(payload) == "different-route" {
			return nil, nil, errOpenAITurnStateWSRoute
		}
		return payload, nil, nil
	}}
	_, accepted, err := wrapped.ReadFrame(context.Background())
	require.NoError(t, err)
	require.Equal(t, "original-route", string(accepted))
	require.Len(t, inner.writes, 1)
	require.Contains(t, string(inner.writes[0]), "turn_state_route_changed")
	require.False(t, inner.closeOnce)
}

func TestOpenAITurnStateBusinessResponseDoesNotReplaceOrExtendCache(t *testing.T) {
	a := otsAccount(1)
	key := OpenAITurnStateKey{1, "model-a", "omitted"}
	value := otsTestToken(time.Now(), 'c')
	returned := strings.Repeat("r", 312)
	expires := time.Now().Add(time.Minute)
	store := &otsTestStore{records: map[OpenAITurnStateKey]*OpenAITurnStateRecord{key: {State: value, StateLength: 292, StateDigest: turnStateDigest(value), ExpiresAt: expires}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, value, r.Header.Get(openAICodexTurnStateHeader))
		w.Header().Set(openAICodexTurnStateHeader, returned)
		_, _ = io.WriteString(w, "{}")
	}))
	defer server.Close()
	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	gateway := &OpenAIGatewayService{httpUpstream: &otsLocalUpstream{client: server.Client(), base: u}}
	svc := NewOpenAITurnStateService(store, &otsTestAccounts{account: a}, nil, gateway)
	defer svc.Stop()
	gateway.turnStates = svc
	c, _ := newTurnStateTestContext(t, 1, "session")
	req, err := gateway.buildUpstreamRequest(context.Background(), c, a, []byte(`{"model":"model-a"}`), "token", true, "", true)
	require.NoError(t, err)
	resp, err := gateway.doOpenAIUpstream(req, "", a)
	require.NoError(t, err)
	defer resp.Body.Close()
	gateway.relayOpenAICodexTurnState(c, a, resp.Header)
	require.Equal(t, returned, c.Writer.Header().Get(openAICodexTurnStateHeader))
	require.Equal(t, value, store.records[key].State)
	require.Equal(t, expires, store.records[key].ExpiresAt)
	store.records[key].ExpiresAt = time.Now().Add(-time.Second)
	headers := http.Header{}
	headers.Set(openAICodexTurnStateHeader, "legacy")
	d := svc.Resolve(context.Background(), a, "model-a", "", headers)
	require.Equal(t, "legacy", d.Source)
	require.Equal(t, "legacy", headers.Get(openAICodexTurnStateHeader))
}

func TestOpenAITurnStateWSPoolInjectsCurrentValueAtDial(t *testing.T) {
	a := otsAccount(1)
	key := OpenAITurnStateKey{1, "model-a", "omitted"}
	value := otsTestToken(time.Now(), 'n')
	store := &otsTestStore{records: map[OpenAITurnStateKey]*OpenAITurnStateRecord{key: {State: value, StateLength: 292, StateDigest: turnStateDigest(value), Generation: "new", ExpiresAt: time.Now().Add(time.Hour)}}}
	svc := NewOpenAITurnStateService(store, &otsTestAccounts{account: a}, nil, nil)
	defer svc.Stop()
	pool := newOpenAIWSConnPool(&config.Config{})
	defer pool.Close()
	pool.turnStates = svc
	dialer := newOpenAIWSFirstDialBlockingCaptureDialer()
	close(dialer.releaseFirst)
	pool.setClientDialerForTest(dialer)
	headers := http.Header{}
	headers.Set(openAICodexTurnStateHeader, "old-cached-value")
	req := openAIWSAcquireRequest{Account: a, WSURL: "wss://example.com/responses", Headers: headers, TurnState: OpenAITurnStateDecision{Managed: true, Key: key, Source: "probe_cache", Generation: "old", OriginalValues: []string{"client-value"}}}
	lease, err := pool.Acquire(context.Background(), req)
	require.NoError(t, err)
	defer lease.Release()
	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	require.Len(t, dialer.headers, 1)
	require.Equal(t, value, dialer.headers[0].Get(openAICodexTurnStateHeader))
	require.Equal(t, "old-cached-value", headers.Get(openAICodexTurnStateHeader), "caller headers stay unchanged")
}
