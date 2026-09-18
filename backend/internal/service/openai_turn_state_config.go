package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const OpenAITurnStateExtraKey = "openai_turn_state"

type OpenAITurnStateTarget struct {
	Model        string   `json:"model"`
	ServiceTiers []string `json:"service_tiers"`
}

type OpenAITurnStateConfig struct {
	SchemaVersion        int                     `json:"schema_version"`
	Enabled              bool                    `json:"enabled"`
	ProxyIDs             []int64                 `json:"proxy_ids"`
	Targets              []OpenAITurnStateTarget `json:"targets"`
	TargetLength         int                     `json:"target_length"`
	CacheTTLSeconds      int                     `json:"cache_ttl_seconds"`
	RefreshBeforeSeconds int                     `json:"refresh_before_seconds"`
	MaxAttempts          int                     `json:"max_attempts"`
	ProbeTimeoutSeconds  int                     `json:"probe_timeout_seconds"`
	LogResponseValues    bool                    `json:"log_response_values"`
}

func DefaultOpenAITurnStateConfig() OpenAITurnStateConfig {
	return OpenAITurnStateConfig{SchemaVersion: 1, ProxyIDs: []int64{}, Targets: []OpenAITurnStateTarget{}, TargetLength: 292, CacheTTLSeconds: 3600, RefreshBeforeSeconds: 600, MaxAttempts: 10, ProbeTimeoutSeconds: 20}
}

func ParseOpenAITurnStateConfig(extra map[string]any) (OpenAITurnStateConfig, error) {
	cfg := DefaultOpenAITurnStateConfig()
	raw, ok := extra[OpenAITurnStateExtraKey]
	if !ok || raw == nil {
		return cfg, nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return cfg, err
	}
	if err = json.Unmarshal(data, &cfg); err != nil {
		return cfg, turnStateConfigError("invalid configuration")
	}
	if cfg.SchemaVersion != 1 || cfg.TargetLength < 1 || cfg.TargetLength > 8192 || cfg.CacheTTLSeconds < 60 || cfg.CacheTTLSeconds > 86400 || cfg.RefreshBeforeSeconds <= 0 || cfg.RefreshBeforeSeconds >= cfg.CacheTTLSeconds || cfg.MaxAttempts < 1 || cfg.MaxAttempts > 10 || cfg.ProbeTimeoutSeconds < 1 || cfg.ProbeTimeoutSeconds > 60 {
		return cfg, turnStateConfigError("invalid length, TTL, refresh threshold, attempt limit or timeout")
	}
	if len(cfg.ProxyIDs) > 100 || len(cfg.Targets) > 32 {
		return cfg, turnStateConfigError("too many proxies or models")
	}
	seenProxy := map[int64]bool{}
	for _, id := range cfg.ProxyIDs {
		if id <= 0 || seenProxy[id] {
			return cfg, turnStateConfigError("proxy IDs must be positive and unique")
		}
		seenProxy[id] = true
	}
	seenModel := map[string]bool{}
	total := 0
	for i := range cfg.Targets {
		t := &cfg.Targets[i]
		if t.Model == "" || t.Model != strings.TrimSpace(t.Model) || len(t.Model) > 200 || strings.ContainsAny(t.Model, "\r\n;=*") || seenModel[t.Model] {
			return cfg, turnStateConfigError("targets must contain unique final upstream model IDs")
		}
		seenModel[t.Model] = true
		if len(t.ServiceTiers) == 0 {
			t.ServiceTiers = []string{"omitted"}
		}
		seenTier := map[string]bool{}
		for j, tier := range t.ServiceTiers {
			tier = turnStateTier(tier)
			switch tier {
			case "omitted", "auto", "default", "priority", "flex", "scale", "ultrafast":
			default:
				return cfg, turnStateConfigError("unsupported service tier")
			}
			if seenTier[tier] {
				return cfg, turnStateConfigError("duplicate service tier")
			}
			seenTier[tier] = true
			t.ServiceTiers[j] = tier
			total++
		}
	}
	if total > 64 {
		return cfg, turnStateConfigError("at most 64 model/tier targets are allowed")
	}
	if cfg.Enabled && (len(cfg.ProxyIDs) == 0 || len(cfg.Targets) == 0) {
		return cfg, turnStateConfigError("select at least one proxy and final upstream model")
	}
	return cfg, nil
}

func turnStateConfigError(message string) error {
	return infraerrors.BadRequest("INVALID_OPENAI_TURN_STATE_CONFIG", message)
}

func ValidateOpenAITurnStateConfig(account *Account) error {
	if account == nil {
		return nil
	}
	cfg, err := ParseOpenAITurnStateConfig(account.Extra)
	if err != nil {
		return err
	}
	if cfg.Enabled && (account.Platform != PlatformOpenAI || account.Type != AccountTypeOAuth) {
		return turnStateConfigError("turn-state probing requires an OpenAI OAuth account")
	}
	return nil
}

// MergeOpenAITurnStateExtra merges only this nested configuration. Other extra
// fields retain the caller's existing full-update / patch semantics.
func MergeOpenAITurnStateExtra(current, incoming map[string]any) map[string]any {
	if incoming == nil {
		return nil
	}
	result := make(map[string]any, len(incoming)+1)
	for k, v := range incoming {
		result[k] = v
	}
	patch, present := incoming[OpenAITurnStateExtraKey]
	if !present {
		if value, exists := current[OpenAITurnStateExtraKey]; exists {
			result[OpenAITurnStateExtraKey] = value
		}
		return result
	}
	if patch == nil {
		result[OpenAITurnStateExtraKey] = DefaultOpenAITurnStateConfig()
		return result
	}
	base := map[string]any{}
	if data, err := json.Marshal(current[OpenAITurnStateExtraKey]); err == nil {
		_ = json.Unmarshal(data, &base)
	}
	if base == nil {
		base = map[string]any{}
	}
	var changes map[string]any
	if data, err := json.Marshal(patch); err == nil && json.Unmarshal(data, &changes) == nil && changes != nil {
		for k, v := range changes {
			base[k] = v
		}
		result[OpenAITurnStateExtraKey] = base
	}
	return result
}

func turnStateTier(value string) string {
	if value == "" || value == "omitted" {
		return "omitted"
	}
	if normalized := normalizedOpenAIServiceTierValue(value); normalized != "" {
		return normalized
	}
	return strings.ToLower(strings.TrimSpace(value))
}

func (c OpenAITurnStateConfig) allows(model, tier string) bool {
	if !c.Enabled {
		return false
	}
	for _, target := range c.Targets {
		if target.Model == model {
			for _, t := range target.ServiceTiers {
				if t == turnStateTier(tier) {
					return true
				}
			}
		}
	}
	return false
}

func turnStateDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// Identity is configuration-scoped: token rotation and per-request thread/turn
// IDs must not invalidate a state shared by different client sessions.
func openAITurnStateIdentity(account *Account, source *Account) string {
	if source == nil {
		source = account
	}
	stableCredentials := map[string]any{}
	for k, v := range source.Credentials {
		switch k {
		case "access_token", "refresh_token", "expires_at", "expires_in", "token_type", "id_token", "last_refresh", "last_refresh_at":
			continue
		}
		stableCredentials[k] = v
	}
	stableExtra := map[string]any{}
	for _, a := range []*Account{source, account} {
		for k, v := range a.Extra {
			if k == "codex_fingerprint_seed" || k == "codex_fingerprint_mode" || strings.HasPrefix(k, "tls_fingerprint") || strings.HasPrefix(k, "codex_identity") {
				stableExtra[fmt.Sprintf("%d:%s", a.ID, k)] = v
			}
		}
	}
	data, _ := json.Marshal([]any{account.ID, source.ID, source.Platform, source.Type, stableCredentials, stableExtra})
	return turnStateDigest(string(data))
}

func openAITurnStateConfigVersion(cfg OpenAITurnStateConfig) string {
	cfg.LogResponseValues = false
	data, _ := json.Marshal(cfg)
	return turnStateDigest(string(data))
}

// AccountTurnStateLifecycle is installed on the repository by the provider.
// BeforeWrite establishes a distributed invalidation barrier; the returned
// callback reconciles against committed DB state (also after a failed write).
type AccountTurnStateLifecycle interface {
	BeforeWrite(ctx context.Context, ids []int64) (func(), error)
}
