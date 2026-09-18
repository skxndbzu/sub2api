package service

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type turnStatePoolTestStore struct {
	OpenAITurnStateStore
	current atomic.Pointer[OpenAITurnStateRecord]
}

func (s *turnStatePoolTestStore) Read(context.Context, OpenAITurnStateKey, string, string) (*OpenAITurnStateRecord, time.Duration, error) {
	r := s.current.Load()
	return r, time.Until(r.ExpiresAt), nil
}

func (s *turnStatePoolTestStore) set(generation string) {
	value := strings.Repeat(generation, 292)
	s.current.Store(&OpenAITurnStateRecord{
		State: value, StateLength: len(value), StateDigest: turnStateDigest(value),
		Generation: generation, ExpiresAt: time.Now().Add(time.Hour),
	})
}

func TestOpenAITurnStateWSPoolQueueWakePreservesStateCompatibility(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		name := "same_state"
		if refresh {
			name = "refreshed_state"
		}
		t.Run(name, func(t *testing.T) {
			account := otsAccount(1)
			store := &turnStatePoolTestStore{}
			store.set("a")
			svc := NewOpenAITurnStateService(store, &otsTestAccounts{account: account}, nil, nil)
			defer svc.Stop()
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 2
			cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 2
			pool := newOpenAIWSConnPool(cfg)
			defer pool.Close()
			pool.turnStates = svc
			pool.setClientDialerForTest(&openAIWSCountingDialer{})
			headers := http.Header{}
			req := openAIWSAcquireRequest{
				Account: account, WSURL: "wss://example.com/responses", Headers: headers,
				TurnState: svc.Resolve(context.Background(), account, "model-a", "", headers),
			}
			target, err := pool.Acquire(context.Background(), req)
			require.NoError(t, err)
			defer target.Release()
			other, err := pool.Acquire(context.Background(), req)
			require.NoError(t, err)
			defer other.Release()
			// Keep the waiter on target so releasing other must wake it via the pool.
			other.conn.waiters.Add(1)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			type result struct {
				lease *openAIWSConnLease
				err   error
			}
			results := make(chan result, 1)
			go func() {
				lease, acquireErr := pool.Acquire(ctx, req)
				results <- result{lease, acquireErr}
			}()
			require.Eventually(t, func() bool { return target.conn.waiters.Load() == 1 }, time.Second, time.Millisecond)
			other.conn.waiters.Add(-1)
			if refresh {
				store.set("b")
			}
			other.Release()
			select {
			case got := <-results:
				require.NoError(t, got.err)
				require.NotNil(t, got.lease)
				defer got.lease.Release()
				expected := svc.Resolve(ctx, account, "model-a", "", http.Header{})
				require.Equal(t, expected.compatibility(), got.lease.conn.handshakeCompatibility.turnState)
				if refresh {
					require.NotEqual(t, other.ConnID(), got.lease.ConnID(), "a refreshed state must not reuse the old handshake")
				} else {
					require.Equal(t, other.ConnID(), got.lease.ConnID())
				}
				require.Positive(t, got.lease.QueueWaitDuration())
				require.Equal(t, int64(1), pool.SnapshotMetrics().AcquireQueueWaitTotal)
			case <-ctx.Done():
				t.Fatal("managed turn-state waiter did not wake after another connection was released")
			}
		})
	}
}

func TestOpenAITurnStateWSPoolRejectsRefreshDuringHandshake(t *testing.T) {
	account := otsAccount(1)
	store := &turnStatePoolTestStore{}
	store.set("a")
	svc := NewOpenAITurnStateService(store, &otsTestAccounts{account: account}, nil, nil)
	defer svc.Stop()
	pool := newOpenAIWSConnPool(&config.Config{})
	defer pool.Close()
	pool.turnStates = svc
	dialer := newOpenAIWSFirstDialBlockingCaptureDialer()
	pool.setClientDialerForTest(dialer)
	headers := http.Header{}
	req := openAIWSAcquireRequest{
		Account: account, WSURL: "wss://example.com/responses", Headers: headers,
		TurnState: svc.Resolve(context.Background(), account, "model-a", "", headers),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	errors := make(chan error, 1)
	go func() {
		lease, err := pool.Acquire(ctx, req)
		if lease != nil {
			lease.Release()
		}
		errors <- err
	}()
	select {
	case <-dialer.firstStarted:
	case <-ctx.Done():
		t.Fatal("handshake did not start")
	}
	store.set("b")
	close(dialer.releaseFirst)
	select {
	case err := <-errors:
		require.ErrorContains(t, err, "upstream handshake no longer matches the requested turn state")
	case <-ctx.Done():
		t.Fatal("acquire did not finish after handshake")
	}
	lease, err := pool.Acquire(ctx, req)
	require.NoError(t, err)
	defer lease.Release()
	expected := svc.Resolve(ctx, account, "model-a", "", http.Header{})
	require.Equal(t, expected.compatibility(), lease.conn.handshakeCompatibility.turnState)
}
