package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type openAITurnStateCache struct{ client *redis.Client }

func (s *openAITurnStateCache) Schedule(ctx context.Context, key service.OpenAITurnStateKey, at time.Time) error {
	member, err := json.Marshal(key)
	if err != nil {
		return err
	}
	return s.client.ZAdd(ctx, "ots:v1:due", redis.Z{Score: float64(at.UnixMilli()), Member: string(member)}).Err()
}

func (s *openAITurnStateCache) Due(ctx context.Context, limit int64) ([]service.OpenAITurnStateKey, error) {
	members, err := s.client.Eval(ctx, `local now=redis.call('TIME');local ms=now[1]*1000+math.floor(now[2]/1000);local members=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ms,'LIMIT',0,ARGV[1]);for _,m in ipairs(members) do redis.call('ZREM',KEYS[1],m) end return members`, []string{"ots:v1:due"}, limit).StringSlice()
	if err != nil {
		return nil, err
	}
	var keys []service.OpenAITurnStateKey
	for _, member := range members {
		var k service.OpenAITurnStateKey
		if json.Unmarshal([]byte(member), &k) == nil {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func NewOpenAITurnStateCache(client *redis.Client) service.OpenAITurnStateStore {
	opts := *client.Options()
	opts.ContextTimeoutEnabled = true
	opts.MaxRetries = -1
	opts.PoolTimeout = 30 * time.Millisecond
	opts.MinIdleConns = 0
	opts.PoolSize = 16
	return &openAITurnStateCache{client: redis.NewClient(&opts)}
}

func (s *openAITurnStateCache) Close() error { return s.client.Close() }

func (s *openAITurnStateCache) NoteOrigin(ctx context.Context, digest string, accountID int64, identity string) error {
	return s.client.Eval(ctx, `if redis.call('HLEN',KEYS[1])<16 or redis.call('HEXISTS',KEYS[1],ARGV[1])==1 then redis.call('HSET',KEYS[1],ARGV[1],ARGV[2]);redis.call('EXPIRE',KEYS[1],7200) end return 1`, []string{"ots:v1:origin:" + digest}, accountID, identity).Err()
}
func (s *openAITurnStateCache) OriginMatches(ctx context.Context, digest string, accountID int64, identity string) (bool, bool, error) {
	n, err := s.client.Eval(ctx, `if redis.call('EXISTS',KEYS[1])==0 then return 0 end if redis.call('HGET',KEYS[1],ARGV[1])==ARGV[2] then return 1 end return 2`, []string{"ots:v1:origin:" + digest}, accountID, identity).Int()
	return n != 0, n == 1, err
}

func otsPrefix(id int64) string { return fmt.Sprintf("ots:v1:{acct:%d}:", id) }
func otsHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:16])
}
func otsKeys(k service.OpenAITurnStateKey) []string {
	p := otsPrefix(k.AccountID)
	target := otsHash(k.Model) + ":" + otsHash(k.ServiceTier)
	return []string{p + "control", p + "mutation", p + "state:" + target, p + "meta:" + target, p + "epoch:" + target, p + "lock:" + otsHash(k.Model), p + "cooldown:" + target, p + "pending:" + target}
}

const otsControlScript = `
local mut=redis.call('GET',KEYS[2])
if mut and mut~=ARGV[4] then return redis.error_reply('turn_state_configuration_changing') end
local old=redis.call('GET',KEYS[1])
local c={epoch=1}
if old then c=cjson.decode(old) end
if c.revision and c.revision>tonumber(ARGV[5]) then return old end
local enabled=ARGV[3]=='1'
if c.identity_version~=ARGV[1] or c.config_version~=ARGV[2] or c.enabled~=enabled then c.epoch=c.epoch+1 end
c.identity_version=ARGV[1]; c.config_version=ARGV[2]; c.enabled=enabled;c.revision=tonumber(ARGV[5])
local value=cjson.encode(c); redis.call('SET',KEYS[1],value); return value`

func (s *openAITurnStateCache) SyncControl(ctx context.Context, id int64, identity, version string, enabled bool, token string, revisions ...int64) (service.OpenAITurnStateControl, error) {
	revision := int64(0)
	if len(revisions) > 0 {
		revision = revisions[0]
	}
	flag := "0"
	if enabled {
		flag = "1"
	}
	raw, err := s.client.Eval(ctx, otsControlScript, []string{otsPrefix(id) + "control", otsPrefix(id) + "mutation"}, identity, version, flag, token, revision).Text()
	var c service.OpenAITurnStateControl
	if err == nil {
		err = json.Unmarshal([]byte(raw), &c)
	}
	return c, err
}

func (s *openAITurnStateCache) BeginChange(ctx context.Context, id int64, token string) (bool, error) {
	n, err := s.client.Eval(ctx, `
if redis.call('EXISTS',KEYS[1])==0 then return 0 end
if not redis.call('SET',KEYS[2],ARGV[1],'NX','PX',30000) then return redis.error_reply('turn_state_configuration_busy') end
local c=cjson.decode(redis.call('GET',KEYS[1])); c.epoch=c.epoch+1; c.enabled=false
redis.call('SET',KEYS[1],cjson.encode(c)); return 1`, []string{otsPrefix(id) + "control", otsPrefix(id) + "mutation"}, token).Int()
	return n == 1, err
}

func (s *openAITurnStateCache) RenewChange(ctx context.Context, id int64, token string) error {
	n, err := s.client.Eval(ctx, `if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('PEXPIRE',KEYS[1],30000) end return 0`, []string{otsPrefix(id) + "mutation"}, token).Int()
	if err == nil && n != 1 {
		return errors.New("turn_state_mutation_lease_lost")
	}
	return err
}
func (s *openAITurnStateCache) EndChange(ctx context.Context, id int64, token string) error {
	return s.client.Eval(ctx, `if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0`, []string{otsPrefix(id) + "mutation"}, token).Err()
}

func (s *openAITurnStateCache) Read(ctx context.Context, k service.OpenAITurnStateKey, identity, version string) (*service.OpenAITurnStateRecord, time.Duration, error) {
	values, err := s.client.Eval(ctx, `
if redis.call('EXISTS',KEYS[2])==1 then return {} end
local control=redis.call('GET',KEYS[1]); local state=redis.call('GET',KEYS[3])
if not control or not state then return {} end
local c=cjson.decode(control); local v=cjson.decode(state)
if not c.enabled or c.identity_version~=ARGV[1] or c.config_version~=ARGV[2] or v.account_epoch~=c.epoch or v.identity_version~=ARGV[1] or v.config_version~=ARGV[2] or v.target_epoch~=(redis.call('GET',KEYS[5]) or '0') then return {} end
local ttl=redis.call('PTTL',KEYS[3]); if ttl<=0 then return {} end
return {state,tostring(ttl)}`, otsKeys(k), identity, version).StringSlice()
	if err != nil || len(values) != 2 {
		return nil, 0, err
	}
	var record service.OpenAITurnStateRecord
	if err = json.Unmarshal([]byte(values[0]), &record); err != nil {
		return nil, 0, err
	}
	if record.OpenAITurnStateKey != k {
		return nil, 0, errors.New("turn_state_key_mismatch")
	}
	ms, err := strconv.ParseInt(values[1], 10, 64)
	return &record, time.Duration(ms) * time.Millisecond, err
}

func (s *openAITurnStateCache) Meta(ctx context.Context, k service.OpenAITurnStateKey) (service.OpenAITurnStateMeta, error) {
	keys := otsKeys(k)
	raw, err := s.client.Get(ctx, keys[3]).Bytes()
	var m service.OpenAITurnStateMeta
	if errors.Is(err, redis.Nil) {
		return m, nil
	}
	if err != nil {
		return m, err
	}
	if err = json.Unmarshal(raw, &m); err != nil {
		return m, err
	}
	if m.ProbeStatus == "running" {
		owner, e := s.client.Get(ctx, keys[5]).Result()
		if e != nil && !errors.Is(e, redis.Nil) {
			return m, e
		}
		if owner != m.TaskID {
			m.ProbeStatus = "failed"
			m.Result = "interrupted"
		}
	}
	return m, nil
}

func (s *openAITurnStateCache) Request(ctx context.Context, k service.OpenAITurnStateKey, manual bool) (service.OpenAITurnStateMeta, error) {
	flag := "0"
	if manual {
		flag = "1"
	}
	raw, err := s.client.Eval(ctx, `
local raw=redis.call('GET',KEYS[4]); local m={}
if raw then m=cjson.decode(raw) end
if m.task_id and redis.call('GET',KEYS[6])==m.task_id then return cjson.encode(m) end
if ARGV[2]=='1' then redis.call('DEL',KEYS[7]);m.next_attempt_at='0001-01-01T00:00:00Z' elseif redis.call('EXISTS',KEYS[7])==1 then return cjson.encode(m) end
local pending=redis.call('GET',KEYS[8]); if not pending then pending=ARGV[1]; redis.call('SET',KEYS[8],pending,'EX',86400) end
m.task_id=pending;m.probe_status='queued';m.result='queued'
local value=cjson.encode(m);redis.call('SET',KEYS[4],value,'EX',604800);return value`, otsKeys(k), uuid.NewString(), flag).Text()
	var m service.OpenAITurnStateMeta
	if err == nil {
		err = json.Unmarshal([]byte(raw), &m)
	}
	return m, err
}

func (s *openAITurnStateCache) Acquire(ctx context.Context, k service.OpenAITurnStateKey, ttl time.Duration) (*service.OpenAITurnStateLease, error) {
	raw, err := s.client.Eval(ctx, `
if redis.call('EXISTS',KEYS[2])==1 or redis.call('EXISTS',KEYS[7])==1 then return '' end
local raw=redis.call('GET',KEYS[1]);if not raw then return '' end
local c=cjson.decode(raw);if not c.enabled then return '' end
local token=redis.call('GET',KEYS[8]) or ARGV[1]
if not redis.call('SET',KEYS[6],token,'NX','PX',ARGV[2]) then return '' end
local mraw=redis.call('GET',KEYS[4]);local m={};if mraw then m=cjson.decode(mraw) end
m.task_id=token;m.probe_status='running';m.result='running';m.started_at=ARGV[3];m.attempts=0;m.distinct_exits=0
redis.call('SET',KEYS[4],cjson.encode(m),'EX',604800)
return cjson.encode({Control=c,TargetEpoch=redis.call('GET',KEYS[5]) or '0',Token=token,Meta=m})`, otsKeys(k), uuid.NewString(), ttl.Milliseconds(), time.Now().UTC().Format(time.RFC3339Nano)).Text()
	if err != nil || raw == "" {
		return nil, err
	}
	var lease service.OpenAITurnStateLease
	if err = json.Unmarshal([]byte(raw), &lease); err != nil {
		return nil, err
	}
	lease.Key = k
	return &lease, nil
}

const otsLeaseCheck = `
local raw=redis.call('GET',KEYS[1]);if not raw or redis.call('EXISTS',KEYS[2])==1 then return 0 end
local c=cjson.decode(raw)
if not c.enabled or c.epoch~=tonumber(ARGV[2]) or c.identity_version~=ARGV[3] or c.config_version~=ARGV[4] or (redis.call('GET',KEYS[5]) or '0')~=ARGV[5] or redis.call('GET',KEYS[6])~=ARGV[1] then return 0 end
`

func otsLeaseArgs(l *service.OpenAITurnStateLease) []any {
	return []any{l.Token, l.Control.Epoch, l.Control.IdentityVersion, l.Control.ConfigVersion, l.TargetEpoch}
}
func (s *openAITurnStateCache) LeaseValid(ctx context.Context, l *service.OpenAITurnStateLease) (bool, error) {
	n, err := s.client.Eval(ctx, otsLeaseCheck+`return 1`, otsKeys(l.Key), otsLeaseArgs(l)...).Int()
	return n == 1, err
}

func (s *openAITurnStateCache) Finish(ctx context.Context, l *service.OpenAITurnStateLease, r *service.OpenAITurnStateRecord, m service.OpenAITurnStateMeta) error {
	state := ""
	ttl := int64(0)
	if r != nil {
		raw, err := json.Marshal(r)
		if err != nil {
			return err
		}
		state = string(raw)
		ttl = time.Until(r.ExpiresAt).Milliseconds()
		if ttl <= 0 {
			return errors.New("turn_state_sample_expired")
		}
	}
	meta, err := json.Marshal(m)
	if err != nil {
		return err
	}
	args := append(otsLeaseArgs(l), state, ttl, string(meta), max(int64(0), time.Until(m.NextAttemptAt).Milliseconds()))
	n, err := s.client.Eval(ctx, otsLeaseCheck+`
if ARGV[6]~='' then redis.call('SET',KEYS[3],ARGV[6],'PX',ARGV[7]) end
redis.call('SET',KEYS[4],ARGV[8],'EX',604800)
if tonumber(ARGV[9])>0 then redis.call('SET',KEYS[7],'1','PX',ARGV[9]) else redis.call('DEL',KEYS[7]) end
if redis.call('GET',KEYS[8])==ARGV[1] then redis.call('DEL',KEYS[8]) end
redis.call('DEL',KEYS[6]);return 1`, otsKeys(l.Key), args...).Int()
	if err == nil && n != 1 {
		return errors.New("turn_state_stale_probe")
	}
	return err
}

func (s *openAITurnStateCache) Release(ctx context.Context, l *service.OpenAITurnStateLease) error {
	return s.client.Eval(ctx, `if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0`, []string{otsKeys(l.Key)[5]}, l.Token).Err()
}
func (s *openAITurnStateCache) Clear(ctx context.Context, k service.OpenAITurnStateKey) error {
	return s.client.Eval(ctx, `redis.call('SET',KEYS[5],ARGV[1]);redis.call('DEL',KEYS[3],KEYS[7],KEYS[8]);redis.call('SET',KEYS[4],'{"result":"cleared","probe_status":"unprobed"}','EX',604800);return 1`, otsKeys(k), uuid.NewString()).Err()
}
