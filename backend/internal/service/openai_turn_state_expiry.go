package service

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"time"
)

// Fernet's cleartext timestamp is the creation time, not an expiry claim.
// Without the upstream signing key we cannot authenticate it or infer the
// upstream TTL/key rotation schedule. Use it only to shorten local reuse.
func openAITurnStateLifetime(value string, cfg OpenAITurnStateConfig, now time.Time) (issuedAt, expiresAt, refreshAt time.Time, err error) {
	invalid := func(reason string) (time.Time, time.Time, time.Time, error) {
		return time.Time{}, time.Time{}, time.Time{}, errors.New(reason)
	}
	if len(value) > 8192 || !validOpenAITurnState(value) {
		return invalid("state_timestamp_invalid")
	}
	raw, decodeErr := base64.URLEncoding.Strict().DecodeString(value)
	if decodeErr != nil {
		raw, decodeErr = base64.RawURLEncoding.Strict().DecodeString(value)
	}
	// Version(1) + timestamp(8) + IV(16) + ciphertext(16*n) + HMAC(32).
	if decodeErr != nil || len(raw) < 73 || raw[0] != 0x80 || (len(raw)-57)%16 != 0 {
		return invalid("state_timestamp_invalid")
	}
	seconds := binary.BigEndian.Uint64(raw[1:9])
	if seconds == 0 || seconds > uint64(now.Add(time.Minute).Unix()) {
		return invalid("state_timestamp_invalid")
	}
	issuedAt = time.Unix(int64(seconds), 0).UTC()
	lifetime := time.Duration(cfg.CacheTTLSeconds) * time.Second
	expiresAt = issuedAt.Add(lifetime)
	// Tolerate small upstream clock skew without extending the old local cap.
	if cap := now.Add(lifetime); expiresAt.After(cap) {
		expiresAt = cap
	}
	if !expiresAt.After(now) {
		return invalid("state_expired")
	}
	refreshAt = expiresAt.Add(-time.Duration(cfg.RefreshBeforeSeconds) * time.Second)
	return issuedAt, expiresAt, refreshAt, nil
}
