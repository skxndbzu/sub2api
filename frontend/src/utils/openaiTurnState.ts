export interface OpenAITurnStateTarget {
  model: string
  service_tiers: string[]
}

export interface OpenAITurnStateConfig {
  schema_version: number
  enabled: boolean
  proxy_ids: number[]
  targets: OpenAITurnStateTarget[]
  target_length: number
  cache_ttl_seconds: number
  refresh_before_seconds: number
  max_attempts: number
  probe_timeout_seconds: number
  log_response_values: boolean
}

export const defaultOpenAITurnStateConfig = (): OpenAITurnStateConfig => ({
  schema_version: 1,
  enabled: false,
  proxy_ids: [],
  targets: [],
  target_length: 292,
  cache_ttl_seconds: 3600,
  refresh_before_seconds: 600,
  max_attempts: 10,
  probe_timeout_seconds: 20,
  log_response_values: false
})

export function readOpenAITurnStateConfig(extra?: Record<string, unknown> | null): OpenAITurnStateConfig {
  const raw = extra?.openai_turn_state
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return defaultOpenAITurnStateConfig()
  return JSON.parse(JSON.stringify({ ...defaultOpenAITurnStateConfig(), ...raw })) as OpenAITurnStateConfig
}

export interface OpenAITurnStateStatus {
  account_id: number
  model: string
  service_tier: string
  status: 'unprobed' | 'probing' | 'available' | 'expired' | 'failed'
  enabled: boolean
  remaining_seconds: number
  source_proxy_id?: number
  state_length?: number
  state_digest?: string
  probed_at?: string
  expires_at?: string
  probe: {
    task_id: string
    probe_status: string
    result: string
    attempts: number
    distinct_exits: number
    next_attempt_at: string
  }
}
