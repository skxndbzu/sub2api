package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type openAITurnStateProbeContextKey struct{}

func (s *OpenAITurnStateService) probeTarget(ctx context.Context, a *Account, cfg OpenAITurnStateConfig, key OpenAITurnStateKey, lease *OpenAITurnStateLease) (*OpenAITurnStateRecord, OpenAITurnStateMeta) {
	result := OpenAITurnStateMeta{Result: "no_distinct_egress"}
	seen := map[netip.Addr]bool{}
	session := uuid.NewString()
	for index, id := range cfg.ProxyIDs {
		if index >= cfg.MaxAttempts || ctx.Err() != nil {
			break
		}
		valid, err := s.store.LeaseValid(ctx, lease)
		if err != nil || !valid {
			result.Result = "lease_lost"
			break
		}
		proxy, err := s.proxies.GetByID(ctx, id)
		if err != nil || proxy == nil || !proxy.IsActive() || proxy.IsExpired(time.Now()) {
			result.Result = "proxy_unavailable"
			continue
		}
		attemptCtx, cancel := context.WithTimeout(context.WithValue(ctx, openAITurnStateProbeContextKey{}, true), time.Duration(cfg.ProbeTimeoutSeconds)*time.Second)
		ip, err := s.probeExit(attemptCtx, proxy)
		if err != nil {
			result.Result = "egress_unverified"
			cancel()
			continue
		}
		if seen[ip] {
			result.Result = "duplicate_egress"
			cancel()
			continue
		}
		seen[ip] = true
		result.DistinctExits = len(seen)
		state, sampled, status, headers, err := s.probeResponse(attemptCtx, a, key, proxy, session)
		result.Attempts++
		cancel()
		if err != nil {
			result.Result = "probe_request_failed"
			if attemptCtx.Err() == context.DeadlineExceeded {
				result.Result = "probe_timeout"
			}
		} else if status < 200 || status >= 300 {
			result.Result = "upstream_http_error"
		} else if len(state) != cfg.TargetLength {
			result.Result = "state_length_mismatch"
		} else {
			issuedAt, expiresAt, refreshAt, lifetimeErr := openAITurnStateLifetime(state, cfg, sampled)
			if lifetimeErr != nil {
				result.Result = lifetimeErr.Error()
				slog.Debug("openai_turn_state_probe_candidate", "account_id", a.ID, "model", key.Model, "service_tier", key.ServiceTier, "proxy_id", id, "state_length", len(state), "result", result.Result)
				continue
			}
			r := &OpenAITurnStateRecord{OpenAITurnStateKey: key, State: state, IssuedAt: issuedAt, ProbedAt: sampled, ExpiresAt: expiresAt, RefreshAt: refreshAt, SourceProxyID: id, EgressDigest: turnStateDigest(ip.String()), StateLength: len(state), StateDigest: turnStateDigest(state), IdentityVersion: lease.Control.IdentityVersion, ConfigVersion: lease.Control.ConfigVersion, AccountEpoch: lease.Control.Epoch, TargetEpoch: lease.TargetEpoch, Generation: uuid.NewString()}
			slog.Info("openai_turn_state_probe_match", "account_id", a.ID, "model", key.Model, "service_tier", key.ServiceTier, "proxy_id", id, "state_length", len(state), "state_digest", r.StateDigest[:16], "usage", "unknown")
			return r, result
		}
		slog.Debug("openai_turn_state_probe_candidate", "account_id", a.ID, "model", key.Model, "service_tier", key.ServiceTier, "proxy_id", id, "state_length", len(state), "http_status", status, "result", result.Result)
		if status == 429 || status == 401 || status == 403 {
			if after := turnStateRetryAfter(headers.Get("Retry-After")); !after.IsZero() {
				result.NextAttemptAt = after
			}
			// Authentication and account throttling are not repaired by changing IP.
			break
		}
	}
	return nil, result
}

func (s *OpenAITurnStateService) probeExit(ctx context.Context, proxy *Proxy) (netip.Addr, error) {
	// Verify the destination's own observed IP, not an unrelated IP lookup site.
	// Two fresh requests detect simple rotating exits; no software can guarantee
	// a third connection will retain its IP on a provider that does not pin it.
	var first netip.Addr
	for i := 0; i < 2; i++ {
		req, err := http.NewRequestWithContext(WithHTTPUpstreamRedirectsDisabled(ctx), http.MethodGet, "https://chatgpt.com/cdn-cgi/trace", nil)
		if err != nil {
			return first, err
		}
		req.Close = true
		resp, err := s.gateway.httpUpstream.Do(req, proxy.URL(), 0, 1)
		if err != nil {
			return first, fmt.Errorf("exit check failed")
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8193))
		_ = resp.Body.Close()
		if readErr != nil || len(body) > 8192 || resp.StatusCode != http.StatusOK {
			return first, fmt.Errorf("invalid exit check response")
		}
		var ip netip.Addr
		for _, line := range strings.Split(string(body), "\n") {
			if value, ok := strings.CutPrefix(strings.TrimSpace(line), "ip="); ok {
				ip, err = netip.ParseAddr(value)
				if err != nil {
					return first, err
				}
				ip = ip.Unmap()
				break
			}
		}
		if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() {
			return first, fmt.Errorf("exit is not a public IP")
		}
		if i == 0 {
			first = ip
		} else if first != ip {
			return first, fmt.Errorf("rotating exit is not pinned")
		}
	}
	return first, nil
}

func (s *OpenAITurnStateService) probeResponse(ctx context.Context, a *Account, key OpenAITurnStateKey, proxy *Proxy, session string) (string, time.Time, int, http.Header, error) {
	ctx = WithHTTPUpstreamRedirectsDisabled(ctx)
	source, err := resolveCredentialAccount(ctx, s.accounts, a)
	if err != nil {
		return "", time.Time{}, 0, nil, err
	}
	token, err := s.gateway.openAITokenProvider.GetAccessToken(ctx, source)
	if err != nil {
		return "", time.Time{}, 0, nil, err
	}
	c := &gin.Context{Request: &http.Request{Header: make(http.Header)}}
	c.Request = c.Request.WithContext(ctx)
	c.Request.Header.Set("session-id", session)
	c.Set(codexAccountIdentitySourceContextKey, source)
	body := map[string]any{"model": key.Model, "stream": true, "store": false, "instructions": "Reply with OK.", "input": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_text", "text": "OK"}}}}}
	if key.ServiceTier != "omitted" {
		body["service_tier"] = key.ServiceTier
	}
	ids := resolveCodexFingerprintIDsFromRequest(a, c.Request.Header)
	if ids != nil {
		applyCodexFingerprintClientMetadata(body, ids)
		stageCodexFingerprintIDs(c, ids)
	}
	data, err := json.Marshal(body)
	if err != nil {
		return "", time.Time{}, 0, nil, err
	}
	req, err := s.gateway.buildUpstreamRequest(ctx, c, a, data, token, true, "", true)
	if err != nil {
		return "", time.Time{}, 0, nil, err
	}
	deleteOpenAIHeaderEqualFold(req.Header, openAICodexTurnStateHeader)
	req.Close = true
	// Use the explicitly chosen proxy. No account selection, plugin routing or
	// business proxy fallback is allowed for this diagnostic request.
	resp, err := s.gateway.httpUpstream.Do(req, proxy.URL(), 0, 1)
	if err != nil {
		return "", time.Time{}, 0, nil, err
	}
	sampled := time.Now().UTC()
	headers := resp.Header.Clone()
	status := resp.StatusCode
	_ = resp.Body.Close()
	logOpenAITurnStateResponse(ctx, a, headers, "probe_sse")
	if status < 200 || status >= 300 {
		return "", sampled, status, headers, nil
	}
	contentType, _, err := mime.ParseMediaType(headers.Get("Content-Type"))
	if err != nil || contentType != "text/event-stream" {
		return "", sampled, status, headers, errOpenAITurnStateInvalid
	}
	values := openAITurnStateHeaderValues(headers)
	if len(values) != 1 || !validOpenAITurnState(values[0]) {
		return "", sampled, status, headers, errOpenAITurnStateInvalid
	}
	return values[0], sampled, status, headers, nil
}

func turnStateRetryAfter(value string) time.Time {
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Now().Add(time.Duration(min(seconds, 86400)) * time.Second)
	}
	if t, err := http.ParseTime(value); err == nil && t.After(time.Now()) {
		return t
	}
	return time.Time{}
}
