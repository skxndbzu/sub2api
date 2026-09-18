package repository

import (
	"context"
	"strings"
	"sync"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

func (r *accountRepository) SetTurnStateLifecycle(lifecycle service.AccountTurnStateLifecycle) {
	r.turnStateLifecycle = lifecycle
}

func (r *accountRepository) beginTurnStateWrite(ctx context.Context, ids []int64) (func(), error) {
	if r.turnStateLifecycle == nil {
		return func() {}, nil
	}
	finish, err := r.turnStateLifecycle.BeforeWrite(ctx, ids)
	if err != nil {
		return nil, err
	}
	if tx := dbent.TxFromContext(ctx); tx != nil {
		var once sync.Once
		tx.OnCommit(func(next dbent.Committer) dbent.Committer {
			return dbent.CommitFunc(func(ctx context.Context, tx *dbent.Tx) error {
				err := next.Commit(ctx, tx)
				once.Do(finish)
				return err
			})
		})
		tx.OnRollback(func(next dbent.Rollbacker) dbent.Rollbacker {
			return dbent.RollbackFunc(func(ctx context.Context, tx *dbent.Tx) error {
				err := next.Rollback(ctx, tx)
				once.Do(finish)
				return err
			})
		})
		return func() {}, nil
	}
	return finish, nil
}

func turnStateExtraChanges(updates map[string]any) bool {
	for key := range updates {
		if key == service.OpenAITurnStateExtraKey || strings.HasPrefix(key, "codex_fingerprint") || strings.HasPrefix(key, "tls_fingerprint") || strings.HasPrefix(key, "codex_identity") {
			return true
		}
	}
	return false
}

func turnStateNestedMergeSQL(expression, placeholder string, updates map[string]any) string {
	if _, ok := updates[service.OpenAITurnStateExtraKey]; !ok {
		return expression
	}
	previous := "CASE WHEN jsonb_typeof(extra->'openai_turn_state') = 'object' THEN extra->'openai_turn_state' ELSE '{}'::jsonb END"
	patch := placeholder + "::jsonb->'openai_turn_state'"
	return "jsonb_set((" + expression + "), '{openai_turn_state}', CASE WHEN jsonb_typeof(" + patch + ") = 'object' THEN (" + previous + ") || (" + patch + ") ELSE '{}'::jsonb END, true)"
}

func (r *accountRepository) validateTurnStateExtraPatch(ctx context.Context, ids []int64, patch map[string]any) error {
	if _, present := patch[service.OpenAITurnStateExtraKey]; !present {
		return nil
	}
	accounts, err := r.GetByIDs(ctx, ids)
	if err != nil {
		return err
	}
	for _, a := range accounts {
		next := *a
		next.Extra = service.MergeOpenAITurnStateExtra(a.Extra, patch)
		if err := service.ValidateOpenAITurnStateConfig(&next); err != nil {
			return err
		}
	}
	return nil
}
