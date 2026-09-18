package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type OpenAITurnStateService struct {
	store         OpenAITurnStateStore
	accounts      AccountRepository
	proxies       ProxyRepository
	gateway       *OpenAIGatewayService
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	startOnce     sync.Once
	queue         chan OpenAITurnStateKey
	pending       sync.Map
	nextReconcile time.Time
	// Tests replace network operations without ever calling a live upstream.
	probe func(context.Context, *Account, OpenAITurnStateConfig, OpenAITurnStateKey, *OpenAITurnStateLease) (*OpenAITurnStateRecord, OpenAITurnStateMeta)
}

func NewOpenAITurnStateService(store OpenAITurnStateStore, accounts AccountRepository, proxies ProxyRepository, gateway *OpenAIGatewayService) *OpenAITurnStateService {
	ctx, cancel := context.WithCancel(context.Background())
	s := &OpenAITurnStateService{store: store, accounts: accounts, proxies: proxies, gateway: gateway, ctx: ctx, cancel: cancel, queue: make(chan OpenAITurnStateKey, 256)}
	s.probe = s.probeTarget
	return s
}

func ProvideOpenAITurnStateService(store OpenAITurnStateStore, accounts AccountRepository, proxies ProxyRepository, gateway *OpenAIGatewayService) *OpenAITurnStateService {
	s := NewOpenAITurnStateService(store, accounts, proxies, gateway)
	gateway.turnStates = s
	if repo, ok := accounts.(interface {
		SetTurnStateLifecycle(AccountTurnStateLifecycle)
	}); ok {
		repo.SetTurnStateLifecycle(s)
	}
	s.Start()
	return s
}

func (s *OpenAITurnStateService) Start() {
	s.startOnce.Do(func() {
		for i := 0; i < 4; i++ {
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				for {
					select {
					case <-s.ctx.Done():
						return
					case key := <-s.queue:
						s.run(key)
						s.pending.Store(key, time.Now().Add(time.Second))
					}
				}
			}()
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.scan()
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-s.ctx.Done():
					return
				case <-ticker.C:
					s.scan()
				}
			}
		}()
	})
}
func (s *OpenAITurnStateService) Stop() {
	if s != nil {
		s.cancel()
		s.wg.Wait()
		if closer, ok := s.store.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
}

func (s *OpenAITurnStateService) enqueue(k OpenAITurnStateKey) {
	if s == nil || s.ctx.Err() != nil {
		return
	}
	if previous, loaded := s.pending.LoadOrStore(k, true); loaded {
		until, ready := previous.(time.Time)
		if !ready || time.Now().Before(until) || !s.pending.CompareAndSwap(k, previous, true) {
			return
		}
	}
	select {
	case s.queue <- k:
	default:
		s.pending.Delete(k)
		slog.Debug("openai_turn_state_queue_full", "account_id", k.AccountID)
	}
}

func (s *OpenAITurnStateService) identity(ctx context.Context, a *Account) (string, error) {
	source := a
	if a.IsCredentialShadow() {
		var err error
		source, err = resolveCredentialAccount(ctx, s.accounts, a)
		if err != nil {
			return "", err
		}
	}
	value := openAITurnStateIdentity(a, source)
	if s.gateway != nil && s.gateway.cfg != nil {
		value += fmt.Sprintf(":%t:%s", s.gateway.cfg.Gateway.DisableCodexIdentityEnforcement, CodexCanonicalUserAgent())
	}
	return turnStateDigest(value), nil
}

func (s *OpenAITurnStateService) syncAccount(ctx context.Context, a *Account, token string) (OpenAITurnStateConfig, OpenAITurnStateControl, error) {
	cfg, err := ParseOpenAITurnStateConfig(a.Extra)
	if err != nil {
		return cfg, OpenAITurnStateControl{}, err
	}
	id, err := s.identity(ctx, a)
	if err != nil {
		return cfg, OpenAITurnStateControl{}, err
	}
	enabled := cfg.Enabled && a.Platform == PlatformOpenAI && a.Type == AccountTypeOAuth && a.Status == StatusActive
	if a.ExpiresAt != nil && !a.ExpiresAt.After(time.Now()) {
		enabled = false
	}
	revision := int64(0)
	if !a.UpdatedAt.IsZero() {
		revision = a.UpdatedAt.UnixMicro()
	}
	control, err := s.store.SyncControl(ctx, a.ID, id, openAITurnStateConfigVersion(cfg), enabled, token, revision)
	if err == nil && (control.IdentityVersion != id || control.ConfigVersion != openAITurnStateConfigVersion(cfg) || control.Enabled != enabled) {
		err = errors.New("turn_state_stale_account_snapshot")
	}
	return cfg, control, err
}

func turnStateTargets(id int64, cfg OpenAITurnStateConfig) []OpenAITurnStateKey {
	var keys []OpenAITurnStateKey
	for _, target := range cfg.Targets {
		for _, tier := range target.ServiceTiers {
			keys = append(keys, OpenAITurnStateKey{id, target.Model, tier})
		}
	}
	return keys
}

func (s *OpenAITurnStateService) scan() {
	if !time.Now().Before(s.nextReconcile) {
		s.reconcile()
		s.nextReconcile = time.Now().Add(time.Minute)
	}
	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()
	keys, err := s.store.Due(ctx, 64)
	if err != nil {
		return
	}
	for _, key := range keys {
		s.enqueue(key)
	}
}

func (s *OpenAITurnStateService) reconcile() {
	s.pending.Range(func(key, value any) bool {
		if until, ok := value.(time.Time); ok && !time.Now().Before(until) {
			s.pending.CompareAndDelete(key, value)
		}
		return true
	})
	ctx, cancel := context.WithTimeout(s.ctx, 12*time.Second)
	defer cancel()
	accounts, err := s.accounts.ListByPlatform(ctx, PlatformOpenAI)
	if err != nil {
		return
	}
	for i := range accounts {
		a := &accounts[i]
		if _, exists := a.Extra[OpenAITurnStateExtraKey]; !exists {
			continue
		}
		cfg, control, err := s.syncAccount(ctx, a, "")
		if err != nil || !control.Enabled {
			continue
		}
		for _, key := range turnStateTargets(a.ID, cfg) {
			meta, err := s.store.Meta(ctx, key)
			if err != nil || meta.Result == "cleared" {
				continue
			}
			if meta.NextAttemptAt.After(time.Now()) {
				_ = s.store.Schedule(ctx, key, meta.NextAttemptAt)
				continue
			}
			r, ttl, err := s.store.Read(ctx, key, control.IdentityVersion, control.ConfigVersion)
			if err != nil {
				continue
			}
			if meta.ProbeStatus == "queued" || r == nil || ttl <= time.Duration(cfg.RefreshBeforeSeconds)*time.Second {
				_ = s.store.Schedule(ctx, key, time.Now())
			} else {
				_ = s.store.Schedule(ctx, key, time.Now().Add(ttl-time.Duration(cfg.RefreshBeforeSeconds)*time.Second))
			}
		}
	}
}

func (s *OpenAITurnStateService) run(key OpenAITurnStateKey) {
	ctx, cancel := context.WithTimeout(s.ctx, 12*time.Second)
	a, err := s.accounts.GetByID(ctx, key.AccountID)
	if err != nil {
		cancel()
		return
	}
	cfg, control, err := s.syncAccount(ctx, a, "")
	if err != nil || !control.Enabled || !cfg.allows(key.Model, key.ServiceTier) {
		cancel()
		return
	}
	meta, err := s.store.Meta(ctx, key)
	if err != nil || meta.NextAttemptAt.After(time.Now()) {
		cancel()
		return
	}
	r, ttl, err := s.store.Read(ctx, key, control.IdentityVersion, control.ConfigVersion)
	if err != nil || (r != nil && ttl > time.Duration(cfg.RefreshBeforeSeconds)*time.Second && meta.ProbeStatus != "queued") {
		cancel()
		return
	}
	budget := time.Duration(cfg.MaxAttempts*cfg.ProbeTimeoutSeconds+10) * time.Second
	lease, err := s.store.Acquire(ctx, key, budget+30*time.Second)
	cancel()
	if err != nil || lease == nil {
		if err == nil {
			scheduleCtx, stop := context.WithTimeout(s.ctx, time.Second)
			_ = s.store.Schedule(scheduleCtx, key, time.Now().Add(15*time.Second))
			stop()
		}
		return
	}
	defer func() {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), time.Second)
		defer releaseCancel()
		_ = s.store.Release(releaseCtx, lease)
	}()
	if !lease.Control.sameGeneration(control) {
		return
	}
	probeCtx, probeCancel := context.WithTimeout(s.ctx, budget)
	defer probeCancel()
	record, result := s.probe(probeCtx, a, cfg, key, lease)
	result.TaskID = lease.Token
	result.StartedAt = lease.Meta.StartedAt
	result.FinishedAt = time.Now().UTC()
	if record == nil {
		result.ProbeStatus = "failed"
		result.Failures = lease.Meta.Failures + 1
		cooldown := min(600, 60<<min(result.Failures-1, 4))
		if result.NextAttemptAt.Before(time.Now().Add(time.Duration(cooldown) * time.Second)) {
			result.NextAttemptAt = time.Now().Add(time.Duration(cooldown) * time.Second)
		}
		result.LastSuccessAt = lease.Meta.LastSuccessAt
		result.LastExpiresAt = lease.Meta.LastExpiresAt
	} else {
		result.ProbeStatus = "succeeded"
		result.Result = "matched"
		result.Failures = 0
		result.NextAttemptAt = time.Time{}
		result.LastSuccessAt = record.ProbedAt
		result.LastExpiresAt = record.ExpiresAt
	}
	commitCtx, commitCancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer commitCancel()
	// Recheck the authoritative identity before committing any sampled value.
	fresh, err := s.accounts.GetByID(commitCtx, key.AccountID)
	if err != nil {
		return
	}
	_, current, err := s.syncAccount(commitCtx, fresh, "")
	if err != nil || !current.sameGeneration(lease.Control) {
		return
	}
	if err = s.store.Finish(commitCtx, lease, record, result); err != nil {
		slog.Debug("openai_turn_state_probe_discarded", "account_id", key.AccountID, "model", key.Model)
		return
	}
	if record != nil {
		_ = s.store.Schedule(commitCtx, key, record.RefreshAt)
	} else {
		_ = s.store.Schedule(commitCtx, key, result.NextAttemptAt)
	}
	slog.Info("openai_turn_state_probe_finished", "account_id", key.AccountID, "model", key.Model, "service_tier", key.ServiceTier, "result", result.Result, "attempts", result.Attempts, "distinct_exits", result.DistinctExits)
}

// Resolve never waits for a probe. Only an already validated Redis value may
// override the caller's current (legacy-validated) header value.
func (s *OpenAITurnStateService) Resolve(ctx context.Context, a *Account, model, tier string, h http.Header) OpenAITurnStateDecision {
	d := OpenAITurnStateDecision{Key: OpenAITurnStateKey{a.ID, model, turnStateTier(tier)}, Source: "legacy", OriginalValues: append([]string(nil), openAITurnStateHeaderValues(h)...)}
	if s == nil || a.Platform != PlatformOpenAI || a.Type != AccountTypeOAuth {
		return d
	}
	cfg, err := ParseOpenAITurnStateConfig(a.Extra)
	if err != nil || !cfg.allows(model, tier) {
		return d
	}
	d.Managed = true
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	id, err := s.identity(readCtx, a)
	if err != nil {
		return d
	}
	d.IdentityVersion = id
	d.ConfigVersion = openAITurnStateConfigVersion(cfg)
	r, ttl, err := s.store.Read(readCtx, d.Key, id, d.ConfigVersion)
	if err != nil {
		slog.Debug("openai_turn_state_cache_unavailable", "account_id", a.ID, "model", model)
		return d
	}
	if r == nil || ttl <= 0 {
		if origins, ok := s.store.(OpenAITurnStateOriginStore); ok {
			values := openAITurnStateHeaderValues(h)
			if len(values) == 1 {
				known, matches, originErr := origins.OriginMatches(readCtx, turnStateDigest(values[0]), a.ID, id)
				if originErr == nil && known && !matches {
					deleteOpenAIHeaderEqualFold(h, openAICodexTurnStateHeader)
					d.OriginalValues = nil
				}
			}
		}
		s.enqueue(d.Key)
		slog.Debug("openai_turn_state_cache_miss", "account_id", a.ID, "model", model, "service_tier", d.Key.ServiceTier)
		return d
	}
	if r.StateLength != cfg.TargetLength || len(r.State) != cfg.TargetLength || !validOpenAITurnState(r.State) {
		return d
	}
	now := time.Now()
	_, expiresAt, _, expiryErr := openAITurnStateLifetime(r.State, cfg, now)
	if expiryErr != nil {
		s.enqueue(d.Key)
		return d
	}
	if !r.ExpiresAt.IsZero() && r.ExpiresAt.Before(expiresAt) {
		expiresAt = r.ExpiresAt
	}
	if remaining := expiresAt.Sub(now); remaining < ttl {
		ttl = remaining
	}
	if ttl <= 0 {
		s.enqueue(d.Key)
		return d
	}
	deleteOpenAIHeaderEqualFold(h, openAICodexTurnStateHeader)
	h.Set(openAICodexTurnStateHeader, r.State)
	d.Source = "probe_cache"
	d.Generation = r.Generation
	d.StateDigest = r.StateDigest
	d.AccountEpoch = r.AccountEpoch
	d.TargetEpoch = r.TargetEpoch
	d.ExpiresAt = now.Add(ttl)
	if ttl <= time.Duration(cfg.RefreshBeforeSeconds)*time.Second {
		s.enqueue(d.Key)
	}
	slog.Debug("openai_turn_state_cache_hit", "account_id", a.ID, "model", model, "service_tier", d.Key.ServiceTier, "state_length", len(r.State), "remaining_seconds", int64(ttl.Seconds()))
	return d
}

func (s *OpenAITurnStateService) GuardClient(ctx context.Context, a *Account, headers http.Header) bool {
	origins, ok := s.store.(OpenAITurnStateOriginStore)
	if !ok {
		return false
	}
	values := openAITurnStateHeaderValues(headers)
	if len(values) != 1 {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	identity, err := s.identity(ctx, a)
	if err != nil {
		return false
	}
	known, matches, err := origins.OriginMatches(ctx, turnStateDigest(values[0]), a.ID, identity)
	if err != nil || !known {
		return false
	}
	if !matches {
		deleteOpenAIHeaderEqualFold(headers, openAICodexTurnStateHeader)
	}
	return true
}

func (s *OpenAITurnStateService) NoteOrigin(ctx context.Context, a *Account, state string) {
	if s == nil || a == nil || state == "" {
		return
	}
	cfg, err := ParseOpenAITurnStateConfig(a.Extra)
	if err != nil || !cfg.Enabled {
		return
	}
	origins, ok := s.store.(OpenAITurnStateOriginStore)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	identity, err := s.identity(ctx, a)
	if err == nil {
		_ = origins.NoteOrigin(ctx, turnStateDigest(state), a.ID, identity)
	}
}

type OpenAITurnStateDecision struct {
	Key             OpenAITurnStateKey
	Managed         bool
	Source          string
	IdentityVersion string
	ConfigVersion   string
	AccountEpoch    int64
	TargetEpoch     string
	Generation      string
	StateDigest     string
	ExpiresAt       time.Time
	OriginalValues  []string
}

func (d OpenAITurnStateDecision) compatibility() string {
	if !d.Managed {
		return ""
	}
	data, _ := json.Marshal([]any{d.Key, d.IdentityVersion, d.ConfigVersion, d.AccountEpoch, d.TargetEpoch, d.Source, d.Generation, d.StateDigest})
	return turnStateDigest(string(data))
}

func (s *OpenAITurnStateService) RefreshDecision(ctx context.Context, a *Account, h http.Header, prior OpenAITurnStateDecision) OpenAITurnStateDecision {
	deleteOpenAIHeaderEqualFold(h, openAICodexTurnStateHeader)
	for _, value := range prior.OriginalValues {
		h.Add(openAICodexTurnStateHeader, value)
	}
	// Read authoritative configuration before delayed WS dialing/prewarming.
	fresh, err := s.accounts.GetByID(ctx, a.ID)
	if err != nil {
		return OpenAITurnStateDecision{Key: prior.Key, Source: "legacy", OriginalValues: prior.OriginalValues}
	}
	return s.Resolve(ctx, fresh, prior.Key.Model, prior.Key.ServiceTier, h)
}

func (s *OpenAITurnStateService) Status(ctx context.Context, id int64) ([]OpenAITurnStateStatus, error) {
	a, err := s.accounts.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	cfg, control, err := s.syncAccount(ctx, a, "")
	if err != nil {
		return nil, err
	}
	statuses := make([]OpenAITurnStateStatus, 0)
	for _, key := range turnStateTargets(id, cfg) {
		meta, err := s.store.Meta(ctx, key)
		if err != nil {
			return nil, err
		}
		r, ttl, err := s.store.Read(ctx, key, control.IdentityVersion, control.ConfigVersion)
		if err != nil {
			return nil, err
		}
		status := OpenAITurnStateStatus{OpenAITurnStateKey: key, Status: "unprobed", Enabled: control.Enabled, Meta: meta}
		if !meta.LastExpiresAt.IsZero() && !meta.LastExpiresAt.After(time.Now()) {
			status.Status = "expired"
		} else if meta.ProbeStatus == "failed" {
			status.Status = "failed"
		}
		if meta.ProbeStatus == "queued" || meta.ProbeStatus == "running" {
			status.Status = "probing"
		}
		if r != nil {
			status.Status = "available"
			status.ProbedAt = &r.ProbedAt
			status.IssuedAt = &r.IssuedAt
			status.ExpiresAt = &r.ExpiresAt
			status.RemainingSeconds = int64(ttl.Seconds())
			status.SourceProxyID = r.SourceProxyID
			status.StateLength = r.StateLength
			status.StateDigest = r.StateDigest[:min(16, len(r.StateDigest))]
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func (s *OpenAITurnStateService) Request(ctx context.Context, id int64, model, tier string) ([]OpenAITurnStateMeta, error) {
	a, err := s.accounts.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	cfg, control, err := s.syncAccount(ctx, a, "")
	if err != nil {
		return nil, err
	}
	if !control.Enabled {
		return nil, turnStateConfigError("turn-state probing is disabled for this account")
	}
	var result []OpenAITurnStateMeta
	for _, key := range turnStateTargets(id, cfg) {
		if model != "" && key.Model != model {
			continue
		}
		if tier != "" && key.ServiceTier != turnStateTier(tier) {
			continue
		}
		m, err := s.store.Request(ctx, key, true)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
		if value, ok := s.pending.Load(key); ok {
			if _, paused := value.(time.Time); paused {
				s.pending.CompareAndDelete(key, value)
			}
		}
		_ = s.store.Schedule(ctx, key, time.Now())
		s.enqueue(key)
	}
	if len(result) == 0 {
		return nil, turnStateConfigError("model/service tier is not selected")
	}
	return result, nil
}

func (s *OpenAITurnStateService) Clear(ctx context.Context, id int64, model, tier string) error {
	a, err := s.accounts.GetByID(ctx, id)
	if err != nil {
		return err
	}
	cfg, err := ParseOpenAITurnStateConfig(a.Extra)
	if err != nil {
		return err
	}
	count := 0
	for _, key := range turnStateTargets(id, cfg) {
		if model != "" && key.Model != model {
			continue
		}
		if tier != "" && key.ServiceTier != turnStateTier(tier) {
			continue
		}
		if err := s.store.Clear(ctx, key); err != nil {
			return err
		}
		count++
	}
	if count == 0 {
		return turnStateConfigError("model/service tier is not selected")
	}
	return nil
}

func (s *OpenAITurnStateService) BeforeWrite(ctx context.Context, ids []int64) (func(), error) {
	token := uuid.NewString()
	var locked []int64
	// Parent identity edits invalidate the distinct caches of credential shadows.
	all := map[int64]bool{}
	for _, id := range ids {
		all[id] = true
		shadows, err := s.accounts.ListShadowsByParent(ctx, id)
		if err != nil {
			return nil, err
		}
		for _, a := range shadows {
			all[a.ID] = true
		}
	}
	finish := func() {
		for _, id := range locked {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if a, err := s.accounts.GetByID(cleanupCtx, id); err == nil {
				_, _, _ = s.syncAccount(cleanupCtx, a, token)
			}
			_ = s.store.EndChange(cleanupCtx, id, token)
			cancel()
		}
	}
	for id := range all {
		a, err := s.accounts.GetByID(ctx, id)
		if err != nil {
			finish()
			return nil, err
		}
		if _, present := a.Extra[OpenAITurnStateExtraKey]; !present {
			continue
		}
		ok, err := s.store.BeginChange(ctx, id, token)
		if err != nil {
			finish()
			return nil, err
		}
		if ok {
			locked = append(locked, id)
		}
	}
	if len(locked) == 0 {
		return func() {}, nil
	}
	renewCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				for _, id := range locked {
					c, stop := context.WithTimeout(renewCtx, 2*time.Second)
					_ = s.store.RenewChange(c, id, token)
					stop()
				}
			}
		}
	}()
	return func() { cancel(); <-done; finish() }, nil
}

func OpenAITurnStateAccountChanged(before, after *Account) bool {
	if before == nil || after == nil {
		return true
	}
	a, _ := ParseOpenAITurnStateConfig(before.Extra)
	b, _ := ParseOpenAITurnStateConfig(after.Extra)
	return openAITurnStateIdentity(before, before) != openAITurnStateIdentity(after, after) || openAITurnStateConfigVersion(a) != openAITurnStateConfigVersion(b) || before.Status != after.Status
}

// Log only real upstream response headers. Raw values require an explicit
// account setting or deployment debug switch, separate from probe enablement.
func logOpenAITurnStateResponse(ctx context.Context, a *Account, h http.Header, transport string) {
	if a == nil || a.Platform != PlatformOpenAI || a.Type != AccountTypeOAuth || h == nil {
		return
	}
	values := openAITurnStateHeaderValues(h)
	if len(values) == 0 {
		return
	}
	cfg, _ := ParseOpenAITurnStateConfig(a.Extra)
	raw := cfg.LogResponseValues || strings.EqualFold(os.Getenv("OPENAI_TURN_STATE_LOG_VALUES"), "true")
	for _, value := range values {
		args := []any{"account_id", a.ID, "transport", transport, "state_length", len(value), "state_digest", turnStateDigest(value)[:16]}
		if raw {
			args = append(args, "x-codex-turn-state", value)
			slog.InfoContext(ctx, "openai_turn_state_upstream_response", args...)
		} else {
			slog.DebugContext(ctx, "openai_turn_state_upstream_response", args...)
		}
	}
}

var errOpenAITurnStateInvalid = errors.New("invalid turn-state response header")

func openAITurnStateHeaderValues(headers http.Header) []string {
	var values []string
	for key, list := range headers {
		if strings.EqualFold(key, openAICodexTurnStateHeader) {
			values = append(values, list...)
		}
	}
	return values
}

func validOpenAITurnState(value string) bool {
	if len(value) == 0 {
		return false
	}
	for i := range len(value) {
		if value[i] < 33 || value[i] > 126 {
			return false
		}
	}
	return true
}
