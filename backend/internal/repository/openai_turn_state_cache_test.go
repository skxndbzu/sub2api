package repository

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newOTSCache(t *testing.T) (*openAITurnStateCache, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return &openAITurnStateCache{client}, server
}
func seedOTS(t *testing.T, s *openAITurnStateCache, key service.OpenAITurnStateKey) *service.OpenAITurnStateRecord {
	t.Helper()
	ctx := context.Background()
	_, err := s.SyncControl(ctx, key.AccountID, "identity", "config", true, "")
	require.NoError(t, err)
	l, err := s.Acquire(ctx, key, 4*time.Minute)
	require.NoError(t, err)
	require.NotNil(t, l)
	now := time.Now().UTC()
	r := &service.OpenAITurnStateRecord{OpenAITurnStateKey: key, State: strings.Repeat("a", 292), StateLength: 292, StateDigest: strings.Repeat("b", 64), IdentityVersion: "identity", ConfigVersion: "config", AccountEpoch: l.Control.Epoch, TargetEpoch: l.TargetEpoch, Generation: "generation1", ProbedAt: now, ExpiresAt: now.Add(time.Hour), RefreshAt: now.Add(50 * time.Minute)}
	require.NoError(t, s.Finish(ctx, l, r, service.OpenAITurnStateMeta{TaskID: l.Token, ProbeStatus: "succeeded", Result: "matched", LastSuccessAt: now, LastExpiresAt: r.ExpiresAt}))
	return r
}

func TestOpenAITurnStateCacheIsolationExpiryAndFailedRefresh(t *testing.T) {
	s, server := newOTSCache(t)
	ctx := context.Background()
	key := service.OpenAITurnStateKey{AccountID: 1, Model: "model-a", ServiceTier: "omitted"}
	original := seedOTS(t, s, key)
	r, ttl, err := s.Read(ctx, key, "identity", "config")
	require.NoError(t, err)
	require.Equal(t, original.State, r.State)
	require.InDelta(t, 3600, ttl.Seconds(), 2)
	for _, other := range []service.OpenAITurnStateKey{{AccountID: 2, Model: "model-a", ServiceTier: "omitted"}, {AccountID: 1, Model: "model-b", ServiceTier: "omitted"}, {AccountID: 1, Model: "model-a", ServiceTier: "priority"}} {
		got, _, err := s.Read(ctx, other, "identity", "config")
		require.NoError(t, err)
		require.Nil(t, got)
	}
	server.FastForward(50 * time.Minute)
	l, err := s.Acquire(ctx, key, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, l)
	require.NoError(t, s.Finish(ctx, l, nil, service.OpenAITurnStateMeta{ProbeStatus: "failed", Failures: 1, Result: "state_length_mismatch", NextAttemptAt: time.Now().Add(time.Minute)}))
	r, ttl, err = s.Read(ctx, key, "identity", "config")
	require.NoError(t, err)
	require.Equal(t, original.State, r.State)
	require.LessOrEqual(t, ttl, 10*time.Minute)
	blocked, err := s.Acquire(ctx, key, time.Minute)
	require.NoError(t, err)
	require.Nil(t, blocked)
	server.FastForward(10 * time.Minute)
	r, _, err = s.Read(ctx, key, "identity", "config")
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestOpenAITurnStateDistributedDeduplicationAndClearFence(t *testing.T) {
	s, server := newOTSCache(t)
	otherClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer otherClient.Close()
	other := &openAITurnStateCache{otherClient}
	ctx := context.Background()
	key := service.OpenAITurnStateKey{AccountID: 1, Model: "model-a", ServiceTier: "omitted"}
	seedOTS(t, s, key)
	var won atomic.Int64
	var wg sync.WaitGroup
	var leases sync.Map
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store := s
			if i%2 == 1 {
				store = other
			}
			l, err := store.Acquire(ctx, key, time.Minute)
			if err != nil {
				t.Error(err)
				return
			}
			if l != nil {
				won.Add(1)
				leases.Store(i, l)
			}
		}(i)
	}
	wg.Wait()
	require.Equal(t, int64(1), won.Load())
	differentTier := key
	differentTier.ServiceTier = "priority"
	l, err := other.Acquire(ctx, differentTier, time.Minute)
	require.NoError(t, err)
	require.Nil(t, l, "same model, different tiers still serialize")
	require.NoError(t, s.Clear(ctx, key))
	leases.Range(func(_, value any) bool {
		old := value.(*service.OpenAITurnStateLease)
		require.Error(t, other.Finish(ctx, old, nil, service.OpenAITurnStateMeta{ProbeStatus: "succeeded"}))
		require.NoError(t, other.Release(ctx, old))
		return true
	})
	m, err := s.Meta(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "cleared", m.Result)
	_, err = other.Request(ctx, key, true)
	require.NoError(t, err)
	fresh, err := other.Acquire(ctx, key, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, fresh)
	leases.Range(func(_, value any) bool {
		require.NoError(t, s.Release(ctx, value.(*service.OpenAITurnStateLease)))
		return true
	})
	valid, err := s.LeaseValid(ctx, fresh)
	require.NoError(t, err)
	require.True(t, valid, "old release must not delete a new lock")
}

func TestOpenAITurnStateIdentityBarrierAndRestart(t *testing.T) {
	s, server := newOTSCache(t)
	ctx := context.Background()
	key := service.OpenAITurnStateKey{AccountID: 7, Model: "model", ServiceTier: "omitted"}
	seedOTS(t, s, key)
	ok, err := s.BeginChange(ctx, 7, "editor")
	require.NoError(t, err)
	require.True(t, ok)
	r, _, err := s.Read(ctx, key, "identity", "config")
	require.NoError(t, err)
	require.Nil(t, r)
	_, err = s.SyncControl(ctx, 7, "old-reader", "config", true, "")
	require.Error(t, err)
	_, err = s.SyncControl(ctx, 7, "identity2", "config", true, "editor")
	require.NoError(t, err)
	require.NoError(t, s.EndChange(ctx, 7, "editor"))
	r, _, err = s.Read(ctx, key, "identity", "config")
	require.NoError(t, err)
	require.Nil(t, r)
	r, _, err = s.Read(ctx, key, "identity2", "config")
	require.NoError(t, err)
	require.Nil(t, r)
	seedOTS(t, s, key)
	client2 := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client2.Close()
	restarted := &openAITurnStateCache{client2}
	r, _, err = restarted.Read(ctx, key, "identity", "config")
	require.NoError(t, err)
	require.NotNil(t, r)
	_, err = s.SyncControl(ctx, 7, "identity", "config", false, "")
	require.NoError(t, err)
	r, _, err = restarted.Read(ctx, key, "identity", "config")
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestOpenAITurnStateManualProbeBypassesCooldownAndOriginIsolation(t *testing.T) {
	s, _ := newOTSCache(t)
	ctx := context.Background()
	key := service.OpenAITurnStateKey{AccountID: 1, Model: "m", ServiceTier: "omitted"}
	seedOTS(t, s, key)
	l, err := s.Acquire(ctx, key, time.Minute)
	require.NoError(t, err)
	require.NoError(t, s.Finish(ctx, l, nil, service.OpenAITurnStateMeta{NextAttemptAt: time.Now().Add(time.Hour), ProbeStatus: "failed"}))
	_, err = s.Request(ctx, key, false)
	require.NoError(t, err)
	l, err = s.Acquire(ctx, key, time.Minute)
	require.NoError(t, err)
	require.Nil(t, l)
	m, err := s.Request(ctx, key, true)
	require.NoError(t, err)
	require.True(t, m.NextAttemptAt.IsZero())
	l, err = s.Acquire(ctx, key, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, l)
	require.NoError(t, s.NoteOrigin(ctx, "digest", 1, "identity"))
	known, match, err := s.OriginMatches(ctx, "digest", 2, "identity")
	require.NoError(t, err)
	require.True(t, known)
	require.False(t, match)
	require.NoError(t, s.NoteOrigin(ctx, "digest", 2, "identity2"))
	known, match, err = s.OriginMatches(ctx, "digest", 1, "identity")
	require.NoError(t, err)
	require.True(t, known)
	require.True(t, match)
}

func TestOpenAITurnStateRefreshScheduleSurvivesRestartAndHasOneConsumer(t *testing.T) {
	s, server := newOTSCache(t)
	ctx := context.Background()
	now := time.Now().UTC()
	server.SetTime(now)
	key := service.OpenAITurnStateKey{AccountID: 8, Model: "m", ServiceTier: "priority"}
	require.NoError(t, s.Schedule(ctx, key, now.Add(50*time.Minute)))
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	restarted := NewOpenAITurnStateCache(client).(*openAITurnStateCache)
	defer restarted.Close()
	ready, err := restarted.Due(ctx, 64)
	require.NoError(t, err)
	require.Empty(t, ready)
	server.SetTime(now.Add(50 * time.Minute))
	ready, err = restarted.Due(ctx, 64)
	require.NoError(t, err)
	require.Equal(t, []service.OpenAITurnStateKey{key}, ready)
	ready, err = s.Due(ctx, 64)
	require.NoError(t, err)
	require.Empty(t, ready)
}

func TestOpenAITurnStateStaleAccountRevisionCannotReenableCache(t *testing.T) {
	s, _ := newOTSCache(t)
	ctx := context.Background()
	_, err := s.SyncControl(ctx, 1, "old", "config", true, "", 100)
	require.NoError(t, err)
	latest, err := s.SyncControl(ctx, 1, "new", "config", false, "", 200)
	require.NoError(t, err)
	stale, err := s.SyncControl(ctx, 1, "old", "config", true, "", 100)
	require.NoError(t, err)
	require.Equal(t, latest, stale)
	require.False(t, stale.Enabled)
}
