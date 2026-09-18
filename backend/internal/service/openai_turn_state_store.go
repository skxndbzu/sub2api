package service

import (
	"context"
	"time"
)

type OpenAITurnStateKey struct {
	AccountID   int64  `json:"account_id"`
	Model       string `json:"model"`
	ServiceTier string `json:"service_tier"`
}

type OpenAITurnStateControl struct {
	Revision        int64  `json:"revision"`
	IdentityVersion string `json:"identity_version"`
	ConfigVersion   string `json:"config_version"`
	Epoch           int64  `json:"epoch"`
	Enabled         bool   `json:"enabled"`
}

// Unrelated account writes advance the DB revision without changing the
// identity/configuration that authorized a probe.
func (c OpenAITurnStateControl) sameGeneration(other OpenAITurnStateControl) bool {
	return c.Epoch == other.Epoch && c.Enabled == other.Enabled && c.IdentityVersion == other.IdentityVersion && c.ConfigVersion == other.ConfigVersion
}

type OpenAITurnStateRecord struct {
	OpenAITurnStateKey
	State           string    `json:"state"`
	ProbedAt        time.Time `json:"probed_at"`
	ExpiresAt       time.Time `json:"expires_at"`
	RefreshAt       time.Time `json:"refresh_at"`
	SourceProxyID   int64     `json:"source_proxy_id"`
	EgressDigest    string    `json:"egress_digest"`
	StateLength     int       `json:"state_length"`
	StateDigest     string    `json:"state_digest"`
	IdentityVersion string    `json:"identity_version"`
	ConfigVersion   string    `json:"config_version"`
	AccountEpoch    int64     `json:"account_epoch"`
	TargetEpoch     string    `json:"target_epoch"`
	Generation      string    `json:"generation"`
}

type OpenAITurnStateMeta struct {
	TaskID        string    `json:"task_id"`
	ProbeStatus   string    `json:"probe_status"`
	Result        string    `json:"result"`
	Attempts      int       `json:"attempts"`
	DistinctExits int       `json:"distinct_exits"`
	Failures      int       `json:"failures"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
	LastSuccessAt time.Time `json:"last_success_at"`
	LastExpiresAt time.Time `json:"last_expires_at"`
}

type OpenAITurnStateLease struct {
	Key         OpenAITurnStateKey
	Control     OpenAITurnStateControl
	TargetEpoch string
	Token       string
	Meta        OpenAITurnStateMeta
}

// Raw state is deliberately not part of any administration response.
type OpenAITurnStateStatus struct {
	OpenAITurnStateKey
	Status           string              `json:"status"`
	Enabled          bool                `json:"enabled"`
	Meta             OpenAITurnStateMeta `json:"probe"`
	ProbedAt         *time.Time          `json:"probed_at,omitempty"`
	ExpiresAt        *time.Time          `json:"expires_at,omitempty"`
	RemainingSeconds int64               `json:"remaining_seconds"`
	SourceProxyID    int64               `json:"source_proxy_id,omitempty"`
	StateLength      int                 `json:"state_length,omitempty"`
	StateDigest      string              `json:"state_digest,omitempty"`
}

type OpenAITurnStateStore interface {
	Schedule(context.Context, OpenAITurnStateKey, time.Time) error
	Due(context.Context, int64) ([]OpenAITurnStateKey, error)
	SyncControl(context.Context, int64, string, string, bool, string, ...int64) (OpenAITurnStateControl, error)
	BeginChange(context.Context, int64, string) (bool, error)
	RenewChange(context.Context, int64, string) error
	EndChange(context.Context, int64, string) error
	Read(context.Context, OpenAITurnStateKey, string, string) (*OpenAITurnStateRecord, time.Duration, error)
	Meta(context.Context, OpenAITurnStateKey) (OpenAITurnStateMeta, error)
	Request(context.Context, OpenAITurnStateKey, bool) (OpenAITurnStateMeta, error)
	Acquire(context.Context, OpenAITurnStateKey, time.Duration) (*OpenAITurnStateLease, error)
	LeaseValid(context.Context, *OpenAITurnStateLease) (bool, error)
	Finish(context.Context, *OpenAITurnStateLease, *OpenAITurnStateRecord, OpenAITurnStateMeta) error
	Release(context.Context, *OpenAITurnStateLease) error
	Clear(context.Context, OpenAITurnStateKey) error
}

type OpenAITurnStateOriginStore interface {
	NoteOrigin(context.Context, string, int64, string) error
	OriginMatches(context.Context, string, int64, string) (bool, bool, error)
}
