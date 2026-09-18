<template>
  <section class="space-y-4 rounded-xl border border-gray-200 p-4 dark:border-dark-600" data-testid="turn-state-settings">
    <div>
      <h3 class="font-medium text-gray-900 dark:text-gray-100">{{ t('admin.accounts.turnState.title') }}</h3>
      <p class="mt-1 text-xs text-gray-500">{{ t('admin.accounts.turnState.description') }}</p>
    </div>
    <p v-if="bulk" class="text-xs text-gray-500">{{ t('admin.accounts.turnState.bulkHint') }}</p>
    <div class="flex flex-wrap items-center gap-3">
      <input v-if="bulk" type="checkbox" :checked="selected.has('enabled')" :aria-label="t('admin.accounts.turnState.changeEnabled')" @change="toggleField('enabled')" />
      <label class="flex items-center gap-2 text-sm">
        <input type="checkbox" :checked="modelValue.enabled" :disabled="bulk && !selected.has('enabled')" data-testid="turn-state-enabled" @change="setField('enabled', checked($event))" />
        {{ t('admin.accounts.turnState.enabled') }}
      </label>
    </div>
    <div>
      <label class="input-label flex items-center gap-2">
        <input v-if="bulk" type="checkbox" :checked="selected.has('proxy_ids')" @change="toggleField('proxy_ids')" />
        {{ t('admin.accounts.turnState.proxies') }}
      </label>
      <div class="max-h-40 space-y-1 overflow-y-auto rounded-lg border border-gray-200 p-2 dark:border-dark-600">
        <label v-for="proxy in proxies" :key="proxy.id" class="flex items-center gap-2 text-sm">
          <input type="checkbox" :value="proxy.id" :checked="modelValue.proxy_ids.includes(proxy.id)" :disabled="bulk && !selected.has('proxy_ids')" @change="toggleProxy(proxy.id)" />
          {{ proxy.name }} <span v-if="proxy.ip_address" class="text-xs text-gray-500">{{ proxy.ip_address }}</span>
        </label>
        <p v-if="proxies.length === 0" class="text-xs text-gray-500">{{ t('admin.accounts.turnState.noProxies') }}</p>
      </div>
      <p class="mt-1 text-xs text-gray-500">{{ t('admin.accounts.turnState.egressHint') }}</p>
    </div>
    <div>
      <label class="input-label flex items-center gap-2">
        <input v-if="bulk" type="checkbox" :checked="selected.has('targets')" @change="toggleField('targets')" />
        {{ t('admin.accounts.turnState.models') }}
      </label>
      <fieldset :disabled="bulk && !selected.has('targets')" :class="bulk && !selected.has('targets') ? 'pointer-events-none opacity-50' : ''">
        <ModelWhitelistSelector :model-value="models" platform="openai" :account-id="accountId" @update:model-value="setModels" />
        <div v-for="target in modelValue.targets" :key="target.model" class="mt-2 rounded-lg bg-gray-50 p-2 dark:bg-dark-700">
          <p class="mb-1 break-all text-xs font-medium">{{ target.model }}</p>
          <div class="flex flex-wrap gap-3">
            <label v-for="tier in tiers" :key="tier" class="flex items-center gap-1 text-xs">
              <input type="checkbox" :checked="target.service_tiers.includes(tier)" @change="toggleTier(target.model, tier)" />
              {{ tier === 'omitted' ? t('admin.accounts.turnState.omittedTier') : tier }}
            </label>
          </div>
        </div>
      </fieldset>
      <p class="mt-1 text-xs text-gray-500">{{ t('admin.accounts.turnState.modelsHint') }}</p>
    </div>
    <div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
      <label v-for="field in numericFields" :key="field.key" class="block text-sm">
        <span class="mb-1 flex items-center gap-2">
          <input v-if="bulk" type="checkbox" :checked="selected.has(field.key)" @change="toggleField(field.key)" />
          {{ t(`admin.accounts.turnState.${field.label}`) }}
        </span>
        <input type="number" class="input w-full" :min="field.min" :max="field.max" step="1" :value="modelValue[field.key]" :disabled="bulk && !selected.has(field.key)" @input="setField(field.key, Number(inputValue($event)))" />
      </label>
    </div>
    <p class="text-xs text-gray-500">{{ t('admin.accounts.turnState.expiryHint') }}</p>
    <div class="flex items-center gap-3">
      <input v-if="bulk" type="checkbox" :checked="selected.has('log_response_values')" :aria-label="t('admin.accounts.turnState.changeLog')" @change="toggleField('log_response_values')" />
      <label class="flex items-center gap-2 text-sm">
        <input type="checkbox" :checked="modelValue.log_response_values" :disabled="bulk && !selected.has('log_response_values')" @change="setField('log_response_values', checked($event))" />
        {{ t('admin.accounts.turnState.rawLog') }}
      </label>
    </div>
    <p class="text-xs text-gray-500">{{ t('admin.accounts.turnState.rawLogHint') }}</p>
    <template v-if="!bulk">
      <p class="text-xs text-gray-500">{{ accountId ? t('admin.accounts.turnState.savedConfig') : t('admin.accounts.turnState.saveFirst') }}</p>
      <div class="flex flex-wrap gap-2">
        <button type="button" class="btn btn-secondary btn-sm" :disabled="!accountId || busy" @click="operate('probe')">{{ t('admin.accounts.turnState.probeNow') }}</button>
        <button type="button" class="btn btn-secondary btn-sm" :disabled="!accountId || busy" @click="operate('clear')">{{ t('admin.accounts.turnState.clear') }}</button>
        <button v-if="accountId" type="button" class="btn btn-secondary btn-sm" :disabled="busy" @click="loadStatus">{{ t('common.refresh') }}</button>
      </div>
      <p v-if="error" class="text-sm text-red-600" role="alert">{{ error }}</p>
      <p v-if="notice" class="text-xs text-gray-500" role="status">{{ notice }}</p>
      <div v-if="statuses.length" class="overflow-x-auto">
        <table class="w-full text-left text-xs">
          <thead><tr class="border-b border-gray-200 dark:border-dark-600"><th class="p-2">{{ t('admin.accounts.turnState.modelTier') }}</th><th class="p-2">{{ t('admin.accounts.turnState.status') }}</th><th class="p-2">{{ t('admin.accounts.turnState.details') }}</th><th class="p-2">{{ t('common.actions') }}</th></tr></thead>
          <tbody>
            <tr v-for="row in statuses" :key="`${row.model}:${row.service_tier}`" class="border-b border-gray-100 dark:border-dark-700">
              <td class="p-2">{{ row.model }}<br />{{ row.service_tier }}</td>
              <td class="p-2">{{ t(`admin.accounts.turnState.states.${row.status}`) }}<br /><span v-if="!row.enabled">{{ t('admin.accounts.turnState.disabled') }}</span><span v-else-if="row.status === 'available' && ['queued', 'running'].includes(row.probe.probe_status)">{{ t('admin.accounts.turnState.refreshing') }}</span></td>
              <td class="space-y-1 p-2">
                <div v-if="row.state_length">{{ row.state_length }} {{ t('admin.accounts.turnState.characters') }} · {{ row.remaining_seconds }}s · #{{ row.source_proxy_id }}</div>
                <div v-if="row.issued_at">{{ t('admin.accounts.turnState.issuedAt') }} {{ formatTime(row.issued_at) }}</div>
                <div v-if="row.probed_at">{{ t('admin.accounts.turnState.probedAt') }} {{ formatTime(row.probed_at) }}</div>
                <div v-if="row.expires_at">{{ t('admin.accounts.turnState.expiresAt') }} {{ formatTime(row.expires_at) }}</div>
                <div v-if="row.probe.result">{{ row.probe.result }} · {{ row.probe.attempts || 0 }}/{{ modelValue.max_attempts }}</div>
                <div v-if="validTime(row.probe.next_attempt_at)">{{ t('admin.accounts.turnState.retryAt') }} {{ formatTime(row.probe.next_attempt_at) }}</div>
                <code v-if="row.state_digest">{{ row.state_digest }}</code>
              </td>
              <td class="p-2"><button type="button" class="text-primary-600 disabled:opacity-50" :disabled="busy || !row.enabled" @click="operate('probe', row)">{{ t('admin.accounts.turnState.probeNow') }}</button><br /><button type="button" class="text-primary-600 disabled:opacity-50" :disabled="busy" @click="operate('clear', row)">{{ t('admin.accounts.turnState.clear') }}</button></td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import ModelWhitelistSelector from './ModelWhitelistSelector.vue'
import { clearTurnState, getTurnStateStatus, probeTurnState } from '@/api/admin/accounts'
import type { OpenAITurnStateConfig, OpenAITurnStateStatus } from '@/utils/openaiTurnState'

const props = defineProps<{
  modelValue: OpenAITurnStateConfig
  proxies: { id: number; name: string; ip_address?: string }[]
  accountId?: number
  bulk?: boolean
}>()
const emit = defineEmits<{
  'update:modelValue': [value: OpenAITurnStateConfig]
  patch: [value: Partial<OpenAITurnStateConfig>]
}>()
const { t } = useI18n()
const selected = ref(new Set<keyof OpenAITurnStateConfig>())
const statuses = ref<OpenAITurnStateStatus[]>([])
const busy = ref(false)
const error = ref('')
const notice = ref('')
let timer: ReturnType<typeof setTimeout> | undefined
let disposed = false
const models = computed(() => props.modelValue.targets.map(target => target.model))
const tiers = ['omitted', 'priority', 'flex', 'ultrafast', 'auto', 'default', 'scale']
const numericFields = [
  { key: 'target_length', label: 'length', min: 1, max: 8192 },
  { key: 'cache_ttl_seconds', label: 'ttl', min: 60, max: 86400 },
  { key: 'refresh_before_seconds', label: 'refreshBefore', min: 1, max: 86399 },
  { key: 'max_attempts', label: 'attempts', min: 1, max: 10 },
  { key: 'probe_timeout_seconds', label: 'timeout', min: 1, max: 60 }
] as const
const checked = (event: Event) => (event.target as HTMLInputElement).checked
const inputValue = (event: Event) => (event.target as HTMLInputElement).value
function publishPatch(value: OpenAITurnStateConfig) {
  emit('patch', Object.fromEntries([...selected.value].map(key => [key, value[key]])))
}
function setField<K extends keyof OpenAITurnStateConfig>(key: K, value: OpenAITurnStateConfig[K]) {
  const next = { ...props.modelValue, [key]: value }
  emit('update:modelValue', next)
  publishPatch(next)
}
function toggleField(key: keyof OpenAITurnStateConfig) {
  const next = new Set(selected.value)
  if (next.has(key)) next.delete(key)
  else next.add(key)
  selected.value = next
  publishPatch(props.modelValue)
}
function toggleProxy(id: number) {
  setField('proxy_ids', props.modelValue.proxy_ids.includes(id) ? props.modelValue.proxy_ids.filter(value => value !== id) : [...props.modelValue.proxy_ids, id])
}
function setModels(value: string[]) {
  setField('targets', value.map(model => props.modelValue.targets.find(target => target.model === model) || { model, service_tiers: ['omitted'] }))
}
function toggleTier(model: string, tier: string) {
  setField('targets', props.modelValue.targets.map(target => {
    if (target.model !== model) return target
    const values = target.service_tiers.includes(tier) ? target.service_tiers.filter(value => value !== tier) : [...target.service_tiers, tier]
    return { ...target, service_tiers: values.length ? values : ['omitted'] }
  }))
}
const validTime = (value?: string) => !!value && new Date(value).getFullYear() > 2000
const formatTime = (value: string) => new Date(value).toLocaleString()
async function loadStatus() {
  if (!props.accountId || disposed) return
  if (timer) clearTimeout(timer)
  const id = props.accountId
  try {
    const result = await getTurnStateStatus(id)
    if (id !== props.accountId || disposed) return
    statuses.value = result
    error.value = ''
  } catch {
    error.value = t('admin.accounts.turnState.statusUnavailable')
  } finally {
    if (!disposed) timer = setTimeout(loadStatus, 5000)
  }
}
async function operate(action: 'probe' | 'clear', row?: OpenAITurnStateStatus) {
  if (!props.accountId) return
  busy.value = true
  error.value = ''
  notice.value = ''
  try {
    if (action === 'probe') await probeTurnState(props.accountId, row?.model, row?.service_tier)
    else await clearTurnState(props.accountId, row?.model, row?.service_tier)
    notice.value = t(`admin.accounts.turnState.${action === 'probe' ? 'queued' : 'cleared'}`)
    await loadStatus()
  } catch {
    error.value = t('admin.accounts.turnState.operationFailed')
  } finally {
    busy.value = false
  }
}
watch(() => props.accountId, () => { statuses.value = []; void loadStatus() }, { immediate: true })
onBeforeUnmount(() => { disposed = true; if (timer) clearTimeout(timer) })
</script>
