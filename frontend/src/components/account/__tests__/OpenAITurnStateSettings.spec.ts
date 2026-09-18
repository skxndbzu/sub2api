import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import OpenAITurnStateSettings from '../OpenAITurnStateSettings.vue'
import { defaultOpenAITurnStateConfig } from '@/utils/openaiTurnState'
import { clearTurnState, getTurnStateStatus, probeTurnState } from '@/api/admin/accounts'

vi.mock('@/api/admin/accounts', () => ({
  getTurnStateStatus: vi.fn(), probeTurnState: vi.fn(), clearTurnState: vi.fn()
}))
vi.mock('vue-i18n', async () => ({
  ...await vi.importActual<typeof import('vue-i18n')>('vue-i18n'),
  useI18n: () => ({ t: (key: string) => key })
}))

const wrappers: ReturnType<typeof mount>[] = []
const mountSettings = (props = {}) => {
  const wrapper = mount(OpenAITurnStateSettings, {
    props: { modelValue: defaultOpenAITurnStateConfig(), proxies: [{ id: 1, name: 'exit-one' }], ...props },
    global: { stubs: { ModelWhitelistSelector: true } }
  })
  wrappers.push(wrapper)
  return wrapper
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.mocked(getTurnStateStatus).mockResolvedValue([])
  vi.mocked(probeTurnState).mockResolvedValue(undefined)
  vi.mocked(clearTurnState).mockResolvedValue(undefined)
})
afterEach(() => { wrappers.splice(0).forEach(wrapper => wrapper.unmount()) })

describe('OpenAI turn-state controls', () => {
  it('keeps probes off by default and requires a saved account for actions', () => {
    const wrapper = mountSettings()
    expect((wrapper.get('[data-testid="turn-state-enabled"]').element as HTMLInputElement).checked).toBe(false)
    expect(wrapper.findAll('button').every(button => button.attributes('disabled') !== undefined)).toBe(true)
    expect(getTurnStateStatus).not.toHaveBeenCalled()
    expect(wrapper.text()).toContain('admin.accounts.turnState.saveFirst')
  })

  it('patches only checked bulk fields without resetting existing proxy or model choices', async () => {
    const wrapper = mountSettings({ bulk: true })
    const selector = wrapper.get('[aria-label="admin.accounts.turnState.changeEnabled"]')
    await selector.setValue(true)
    await wrapper.get('[data-testid="turn-state-enabled"]').setValue(true)
    expect(wrapper.emitted('patch')?.at(-1)?.[0]).toEqual({ enabled: true })
    await selector.setValue(false)
    expect(wrapper.emitted('patch')?.at(-1)?.[0]).toEqual({})
  })

  it('uses the saved account for probe and clear actions and displays per-tier results', async () => {
    vi.mocked(getTurnStateStatus).mockResolvedValue([{
      account_id: 42, model: 'actual-model', service_tier: 'priority', status: 'available', enabled: true,
      remaining_seconds: 3500, source_proxy_id: 1, state_length: 292, state_digest: 'diagnostic-only',
      issued_at: '2026-09-18T10:00:00Z', expires_at: '2026-09-18T11:00:00Z',
      probe: { task_id: 'task1', probe_status: 'succeeded', result: 'matched', attempts: 2, distinct_exits: 2, next_attempt_at: '' }
    }])
    const wrapper = mountSettings({ accountId: 42 })
    await flushPromises()
    expect(wrapper.text()).toContain('actual-model')
    expect(wrapper.text()).toContain('priority')
    expect(wrapper.text()).toContain('3500s')
    expect(wrapper.text()).toContain('admin.accounts.turnState.issuedAt')
    expect(wrapper.text()).toContain('admin.accounts.turnState.expiresAt')
    expect(wrapper.text()).toContain('admin.accounts.turnState.expiryHint')
    const actions = wrapper.findAll('button')
    await actions[0].trigger('click')
    await flushPromises()
    expect(probeTurnState).toHaveBeenCalledWith(42, undefined, undefined)
    await actions[1].trigger('click')
    await flushPromises()
    expect(clearTurnState).toHaveBeenCalledWith(42, undefined, undefined)
    const rowActions = wrapper.findAll('tbody button')
    await rowActions[0].trigger('click')
    await flushPromises()
    expect(probeTurnState).toHaveBeenLastCalledWith(42, 'actual-model', 'priority')
  })

  it('reports unavailable status without claiming the cache is empty', async () => {
    vi.mocked(getTurnStateStatus).mockRejectedValue(new Error('Redis unavailable'))
    const wrapper = mountSettings({ accountId: 42 })
    await flushPromises()
    expect(wrapper.get('[role="alert"]').text()).toContain('statusUnavailable')
    expect(wrapper.find('tbody').exists()).toBe(false)
  })

  it('preserves the editable early refresh threshold', async () => {
    const wrapper = mountSettings()
    const label = wrapper.findAll('label').find(item => item.text().includes('admin.accounts.turnState.refreshBefore'))!
    await label.get('input[type="number"]').setValue(300)
    expect(wrapper.emitted('update:modelValue')?.at(-1)?.[0]).toMatchObject({
      refresh_before_seconds: 300, cache_ttl_seconds: 3600
    })
  })
})
