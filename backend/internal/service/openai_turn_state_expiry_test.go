package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Structurally valid Fernet fixture, not authenticated ciphertext.
func otsTestToken(issuedAt time.Time, marker byte) string {
	raw := bytes.Repeat([]byte{marker}, 57+160)
	raw[0] = 0x80
	binary.BigEndian.PutUint64(raw[1:9], uint64(issuedAt.Unix()))
	return base64.URLEncoding.EncodeToString(raw)
}

func TestOpenAITurnStateLifetime(t *testing.T) {
	now := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	cfg := DefaultOpenAITurnStateConfig()
	issued := now.Add(-20 * time.Minute)
	token := otsTestToken(issued, 'a')
	require.Len(t, token, 292)
	for _, value := range []string{token, strings.TrimRight(token, "=")} {
		created, expires, refresh, err := openAITurnStateLifetime(value, cfg, now)
		require.NoError(t, err)
		require.Equal(t, issued, created)
		require.Equal(t, now.Add(40*time.Minute), expires)
		require.Equal(t, now.Add(30*time.Minute), refresh)
		_, laterExpiry, _, err := openAITurnStateLifetime(value, cfg, now.Add(time.Minute))
		require.NoError(t, err)
		require.Equal(t, expires, laterExpiry, "reading the token must not renew its lifetime")
	}
	badVersion, _ := base64.URLEncoding.DecodeString(token)
	badVersion[0] = 0x81
	badBlock := make([]byte, 74)
	badBlock[0] = 0x80
	for name, value := range map[string]string{
		"empty": "", "not_base64": "gAAAA!", "truncated": "gAAAAA==",
		"wrong_version":  base64.URLEncoding.EncodeToString(badVersion),
		"bad_block_size": base64.URLEncoding.EncodeToString(badBlock),
		"zero":           otsTestToken(time.Unix(0, 0), 'a'),
		"future":         otsTestToken(now.Add(2*time.Minute), 'a'),
		"overflow":       otsTestToken(time.Unix(-1, 0), 'a'),
		"expired":        otsTestToken(now.Add(-time.Hour), 'a'),
		"whitespace":     token + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := openAITurnStateLifetime(value, cfg, now)
			require.Error(t, err)
		})
	}
	_, skewExpiry, _, err := openAITurnStateLifetime(otsTestToken(now.Add(30*time.Second), 'a'), cfg, now)
	require.NoError(t, err)
	require.Equal(t, now.Add(time.Hour), skewExpiry)
	_, expires, refresh, err := openAITurnStateLifetime(otsTestToken(now.Add(-55*time.Minute), 'a'), cfg, now)
	require.NoError(t, err)
	require.Equal(t, now.Add(5*time.Minute), expires)
	require.True(t, refresh.Before(now), "near-expiry tokens must refresh immediately")
}

func TestOpenAITurnStateResolveUsesTokenAge(t *testing.T) {
	for _, age := range []time.Duration{20 * time.Minute, 55 * time.Minute, 2 * time.Hour} {
		t.Run(age.String(), func(t *testing.T) {
			account := otsAccount(1)
			key := OpenAITurnStateKey{1, "model-a", "omitted"}
			issued := time.Now().UTC().Add(-age).Truncate(time.Second)
			state := otsTestToken(issued, 'x')
			store := &otsTestStore{records: map[OpenAITurnStateKey]*OpenAITurnStateRecord{
				key: {State: state, StateLength: len(state), ExpiresAt: time.Now().Add(time.Hour)},
			}}
			svc := NewOpenAITurnStateService(store, &otsTestAccounts{account: account}, nil, nil)
			defer svc.Stop()
			headers := http.Header{}
			decision := svc.Resolve(context.Background(), account, key.Model, "", headers)
			if age >= time.Hour {
				require.Empty(t, headers.Get(openAICodexTurnStateHeader))
				require.Equal(t, "legacy", decision.Source)
			} else {
				require.Equal(t, state, headers.Get(openAICodexTurnStateHeader))
				require.WithinDuration(t, issued.Add(time.Hour), decision.ExpiresAt, time.Millisecond)
			}
			if age >= 50*time.Minute {
				require.Len(t, svc.queue, 1, "refresh threshold must enqueue a probe")
			} else {
				require.Empty(t, svc.queue)
			}
		})
	}
}
