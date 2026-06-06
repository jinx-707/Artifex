import api from './api'
import type {
  ReferralSubmission,
  ReferralResponse,
  WorkflowStatus,
  WorkflowStage,
  PendingApproval,
  AgentStatus,
  PendingApprovalsResponse,
  ApproveRequest,
  ApproveResponse,
  Placement,
  HealthStatus,
  DashboardMetrics,
  RiskDistribution,
  WorkflowEvent,
  DashboardEventsResponse,
  AgentStatusMap,
  Family,
  FamilyCreate,
  FamilyUpdate,
} from '@/types'

export function normalizeWorkflowId(input: unknown): string {
  if (typeof input !== 'string') return ''
  const trimmed = input.trim()

  // If already a foster-* id, keep as-is
  if (/^foster-/i.test(trimmed)) return trimmed

  // CHILD-123 or CHILD-123 (case-insensitive) -> foster-123
  const mChild = trimmed.match(/^CHILD-(\d+)$/i)
  if (mChild) {
    const normalized = `foster-${mChild[1]}`
    console.log(`[foster] normalized "${trimmed}" -> "${normalized}"`)
    return normalized
  }

  // CH-123 -> foster-123
  const mCh = trimmed.match(/^CH-(\d+)$/i)
  if (mCh) {
    const normalized = `foster-${mCh[1]}`
    console.log(`[foster] normalized "${trimmed}" -> "${normalized}"`)
    return normalized
  }

  // Pure digits -> foster-<digits>
  if (/^\d+$/.test(trimmed)) {
    const normalized = `foster-${trimmed}`
    console.log(`[foster] normalized numeric input "${trimmed}" -> "${normalized}"`)
    return normalized
  }

  // Default: prefix with foster- preserving non-numeric child ids (e.g., CABC123)
  const normalized = `foster-${trimmed}`
  console.log(`[foster] normalized default "${trimmed}" -> "${normalized}"`)
  return normalized
}

export async function submitReferral(data: ReferralSubmission): Promise<ReferralResponse> {
  const payload = {
    child_id: data.child_id,
    age: data.age,
    gender: data.gender,
    special_needs: data.special_needs,
    languages: data.languages,
    medical_needs: data.medical_needs,
    behavioral_support: data.behavioral_support,
    sibling_group: data.sibling_group,
    emergency_level: data.emergency_level,
    preferred_location: data.preferred_location,
    foster_home_type: data.foster_home_type,
    capacity_needed: data.capacity_needed,
    accessibility_needs: data.accessibility_needs,
    school_continuity: data.school_continuity,
    risk_flags: data.risk_flags,
    notes: data.notes,
  }
  console.log('[foster] submitReferral payload:', JSON.stringify(payload, null, 2))
  const res = await api.post<ReferralResponse>('/api/referral', payload)
  console.log('[foster] submitReferral response:', res.data)
  console.log(`[foster] workflow_id received: "${res.data.workflow_id}" — will navigate to /workflow/${res.data.workflow_id}`)
  return res.data
}

export async function getPendingApprovals(): Promise<PendingApproval[]> {
  console.log('[foster] fetching pending approvals...')
  const res = await api.get<PendingApprovalsResponse | PendingApproval[]>('/api/pending_approvals')
  console.log('[foster] pending approvals response:', res.data)

  if (Array.isArray(res.data)) {
    return res.data as PendingApproval[]
  }

  const wrapper = res.data as PendingApprovalsResponse
  if (wrapper && Array.isArray(wrapper.approvals)) {
    return wrapper.approvals
  }

  console.warn('[foster] unexpected approvals response shape:', res.data)
  return []
}

export async function approveReferral(data: ApproveRequest): Promise<ApproveResponse> {
  const payload = {
    workflow_id: data.workflow_id,
    approved: data.approved,
    comment: data.comment,
  }
  console.log('[foster] approveReferral payload:', JSON.stringify(payload, null, 2))
  const res = await api.post<ApproveResponse>('/api/approve', payload)
  console.log('[foster] approveReferral response:', res.data)
  return res.data
}

export async function supervisorApprove(data: ApproveRequest): Promise<ApproveResponse> {
  const payload = {
    workflow_id: data.workflow_id,
    approved: data.approved,
    comment: data.comment,
  }
  console.log('[foster] supervisorApprove payload:', JSON.stringify(payload, null, 2))
  const res = await api.post<ApproveResponse>('/api/supervisor_approve', payload)
  console.log('[foster] supervisorApprove response:', res.data)
  return res.data
}

export async function getPlacements(): Promise<Placement[]> {
  console.log('[foster] fetching placements...')
  const res = await api.get<Placement[] | { placements: Placement[] }>('/foster/placements')
  console.log('[foster] placements response:', res.data)

  let placements: Placement[] = []

  if (Array.isArray(res.data)) {
    placements = res.data as Placement[]
  } else {
    const wrapper = res.data as { placements: Placement[] }
    if (wrapper && Array.isArray(wrapper.placements)) {
      placements = wrapper.placements
    }
  }

  placements.forEach((p, i) => {
    if (!p || typeof p !== 'object') {
      console.warn(`[foster] placement[${i}] is not an object:`, p)
      return
    }
    // Only warn about null fields on placements that have had time to process
    // (i.e. not brand-new pending records still running through the pipeline)
    const isProcessing = p.status === 'pending' && !p.family_id && !p.family_json
    if (!isProcessing) {
      const expectedFields: (keyof Placement)[] = ['family_id', 'family_json', 'match_explanation', 'last_notes', 'foster_family_name', 'location', 'capacity', 'recommended_family']
      for (const field of expectedFields) {
        if (p[field] == null) {
          console.debug(`[foster] placement[${i}].${field} is null (workflow_id: ${p.workflow_id})`)
        }
      }
    }
  })

  return placements
}

export async function getWorkflowStatus(workflowId: string): Promise<WorkflowStatus> {
  const raw = workflowId
  const normalized = normalizeWorkflowId(workflowId)

  if (!normalized || !normalized.trim()) {
    throw new Error('workflow_id is required')
  }

  if (raw !== normalized) {
    console.log(`[foster] normalized workflow ID: "${raw}" -> "${normalized}"`)
  }

  console.log(`[foster] fetching workflow status for: "${normalized}"`)
  const res = await api.get<any>(`/foster/status/${encodeURIComponent(normalized)}`)
  console.log(`[foster] workflow status response for "${normalized}":`, res.data)

  const d = res.data || {}
  const now = new Date().toISOString()
  const timeline = Array.isArray(d.timeline) ? d.timeline : []
  const recommendedFamily = d.recommended_family ?? d.recommendation ?? null

  const normalizedStatus: WorkflowStatus = {
    workflow_id: d.workflow_id || normalized,
    child_id: d.child_id || '',
    family_id: d.family_id || '',
    status: d.status || 'unknown',
    current_stage: d.current_stage || d.stage || (timeline[0] && (timeline[0].stage || timeline[0].name)) || 'Unknown',
    risk_score: d.risk_score ?? null,
    match_score: d.match_score ?? null,
    confidence_score: d.confidence_score ?? null,
    recommended_family: recommendedFamily || null,
    capacity: d.capacity ?? null,
    progress: typeof d.progress === 'number' ? d.progress : 0,
    timeline,
    feature_importance: d.feature_importance || null,
    top_matches: d.top_matches || null,
    active: d.active ?? true,
    metadata: d.metadata || {},
    created_at: d.created_at || now,
    updated_at: d.updated_at || now,
  }
  console.log(`[foster] normalized workflow status:`, normalizedStatus)
  return normalizedStatus
}

export async function getAgentStatus(agentName: string): Promise<AgentStatus> {
  const res = await api.get<AgentStatus>(`/agent/status/${agentName}`)
  return res.data
}

export async function getHealth(): Promise<HealthStatus> {
  console.log('[foster] fetching health...')
  const res = await api.get<HealthStatus>('/health')
  console.log('[foster] health response:', res.data)
  return res.data
}

export async function sendChatMessage(
  message: string,
  workflowId?: string
): Promise<{ id: string; message: string; sources?: string[]; actions?: { label: string; action: string; payload?: Record<string, unknown> }[] }> {
  const body: Record<string, unknown> = { message }
  if (workflowId) body.workflow_id = workflowId
  const res = await api.post<
    | string
    | { id?: string; message?: string; sources?: string[] }
  >('/chat', body)

  // Handle both plain-text and JSON responses
  if (typeof res.data === 'string') {
    return { id: `chat-${Date.now()}`, message: res.data, sources: [] }
  }
  const data = res.data as { id?: string; message?: string; sources?: string[] }
  return {
    id: data.id || `chat-${Date.now()}`,
    message: data.message || String(res.data),
    sources: data.sources || [],
  }
}

export async function getHealthCheck(): Promise<boolean> {
  try {
    const res = await api.get('/health', { timeout: 5000 })
    const ok = res.status === 200
    console.log(`[foster] health check: ${ok ? 'OK' : 'FAIL'} (status ${res.status})`, res.data)
    return ok
  } catch (err) {
    console.warn('[foster] health check failed:', err instanceof Error ? err.message : err)
    return false
  }
}

// ── Families CRUD ─────────────────────────────────────────────────────────

export interface FamiliesResponse {
  families: Family[]
  count: number
}

export async function getFamilies(): Promise<Family[]> {
  console.log('[foster] fetching families...')
  const res = await api.get<FamiliesResponse | Family[]>('/families')
  if (Array.isArray(res.data)) {
    return res.data as Family[]
  }
  const wrapper = res.data as FamiliesResponse
  if (wrapper && Array.isArray(wrapper.families)) {
    return wrapper.families
  }
  return []
}

export async function getFamily(familyId: string): Promise<Family> {
  const res = await api.get<Family>(`/families/${encodeURIComponent(familyId)}`)
  return res.data
}

export async function createFamily(data: FamilyCreate): Promise<Family> {
  const res = await api.post<Family>('/families', data)
  return res.data
}

export async function updateFamily(familyId: string, data: FamilyUpdate): Promise<Family> {
  const res = await api.put<Family>(`/families/${encodeURIComponent(familyId)}`, data)
  return res.data
}

export async function deleteFamily(familyId: string): Promise<void> {
  await api.delete(`/families/${encodeURIComponent(familyId)}`)
}

// --- WebSocket streaming helper for workflow updates ---------------------
function _wsBaseUrl(): string {
  const apiUrl = import.meta.env.VITE_API_URL as string | undefined
  if (apiUrl) {
    // Convert https://... -> wss://... or http://... -> ws://...
    return apiUrl.replace(/^http/, 'ws').replace(/\/$/, '')
  }
  // Fallback: same host as page (works in local dev with Vite proxy)
  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
  return `${protocol}://${window.location.host}`
}

export function subscribeWorkflowStream(
  workflowId: string,
  onMessage: (msg: any) => void,
  onOpen?: () => void,
  onClose?: () => void,
): { close: () => void } {
  const token = localStorage.getItem('artifex_token') || ''
  const url = `${_wsBaseUrl()}/workflow/${encodeURIComponent(workflowId)}/stream?token=${encodeURIComponent(token)}`

  let ws: WebSocket | null = null
  let shouldClose = false
  // Fix ③: exponential backoff with full jitter (capped at 30 s)
  let attempt = 0
  const MAX_DELAY_MS = 30_000

  function backoffDelay(): number {
    const base = Math.min(500 * 2 ** attempt, MAX_DELAY_MS)
    // full jitter: random value in [0, base]
    return Math.random() * base
  }

  function connect() {
    ws = new WebSocket(url)
    ws.onopen = () => {
      attempt = 0  // reset backoff on successful connection
      onOpen?.()
    }
    ws.onmessage = (ev) => {
      try { const data = JSON.parse(ev.data); onMessage(data) } catch (err) { console.warn('[foster.ws] parse error', err) }
    }
    ws.onclose = (ev) => {
      if (shouldClose) { onClose?.(); return }
      // 1008 = policy violation (auth failure) – do not reconnect
      if (ev.code === 1008) {
        console.warn('[foster.ws] auth rejected (1008), not reconnecting')
        onClose?.()
        return
      }
      const delay = backoffDelay()
      attempt++
      console.info(`[foster.ws] closed (code ${ev.code}), reconnecting in ${Math.round(delay)}ms (attempt ${attempt})`)
      setTimeout(() => connect(), delay)
    }
    ws.onerror = (e) => { console.warn('[foster.ws] error', e) }
  }

  connect()

  return {
    close: () => {
      shouldClose = true
      try { ws?.close() } catch (_) {}
    },
  }
}

export async function getDashboardMetrics(): Promise<DashboardMetrics> {
  const res = await api.get<DashboardMetrics>('/dashboard/metrics')
  return res.data
}

export async function getRiskDistribution(): Promise<RiskDistribution> {
  const res = await api.get<RiskDistribution>('/dashboard/risk-distribution')
  return res.data
}

export async function getDashboardEvents(): Promise<WorkflowEvent[]> {
  const res = await api.get<DashboardEventsResponse | WorkflowEvent[]>('/dashboard/events')
  const data = res.data
  if (Array.isArray(data)) return data as WorkflowEvent[]
  if (data && 'events' in data) return data.events || []
  return []
}

export interface WorkflowActivityEntry {
  name: string
  submitted: number
  matched: number
  approved: number
}

export async function getWorkflowActivity(): Promise<WorkflowActivityEntry[]> {
  const res = await api.get<{ activity: WorkflowActivityEntry[] }>('/dashboard/workflow-activity')
  return res.data?.activity ?? []
}

export async function getAgentStatuses(): Promise<AgentStatusMap> {
  const res = await api.get<AgentStatusMap>('/agent/status')
  return res.data
}

export interface AgentDetail {
  id: string
  name: string
  type: string
  status: string
  last_heartbeat_age_s: number | null
}

export async function getAgents(): Promise<{ agents: Record<string, AgentDetail> }> {
  const res = await api.get<{ agents: Record<string, AgentDetail> }>('/api/agents')
  return res.data
}

// ── Crisis Prediction ─────────────────────────────────────────────────────

export interface CrisisPrediction {
  probability: number
  risk_level: 'low' | 'medium' | 'high' | 'critical'
  top_reasons: Array<{ reason: string; weight: number }>
  recommended_interventions: string[]
  prediction_date?: string
  cached?: boolean
}

export async function getCrisisPrediction(placementId: string): Promise<CrisisPrediction> {
  const res = await api.get<CrisisPrediction>(
    `/api/placements/${encodeURIComponent(placementId)}/crisis-prediction`
  )
  return res.data
}

export async function refreshCrisisPrediction(placementId: string): Promise<CrisisPrediction> {
  const res = await api.post<CrisisPrediction>(
    `/api/placements/${encodeURIComponent(placementId)}/refresh-prediction`
  )
  return res.data
}

// ── Fairness Metrics ──────────────────────────────────────────────────────

export interface FairnessGroupBreakdown {
  group: string
  total: number
  high_risk: number
  high_risk_rate: number
}

export interface FairnessMetrics {
  gender_bias: number
  special_needs_bias: number
  emergency_level_bias: number
  threshold: number
  status: 'PASS' | 'REVIEW'
  total_placements: number
  last_calculated: string
  breakdowns: {
    gender: FairnessGroupBreakdown[]
    special_needs: FairnessGroupBreakdown[]
    emergency_level: FairnessGroupBreakdown[]
  }
}

export async function getFairnessMetrics(): Promise<FairnessMetrics> {
  const res = await api.get<FairnessMetrics>('/api/fairness/metrics')
  return res.data
}

export interface ShapExplanation {
  workflow_id: string
  match_score: number | null
  confidence_score: number | null
  feature_importance: Array<{
    feature: string
    importance: number
    description: string
  }>
  top_matches: unknown[]
}

export async function getShapExplanation(workflowId: string): Promise<ShapExplanation> {
  const res = await api.get<ShapExplanation>(
    `/api/fairness/shap/${encodeURIComponent(workflowId)}`
  )
  return res.data
}

// ── Child Event WebSocket streaming ───────────────────────────────────────
export function subscribeChildEventStream(
  childId: string,
  onMessage: (event: TimelineEventV2) => void,
  onOpen?: () => void,
  onClose?: () => void,
): { close: () => void } {
  const token = localStorage.getItem('artifex_token') || ''
  const url = `${_wsBaseUrl()}/ws/child/${encodeURIComponent(childId)}?token=${encodeURIComponent(token)}`

  let ws: WebSocket | null = null
  let shouldClose = false
  let attempt = 0
  const MAX_DELAY_MS = 30_000

  function backoffDelay(): number {
    return Math.random() * Math.min(500 * 2 ** attempt, MAX_DELAY_MS)
  }

  function connect() {
    ws = new WebSocket(url)
    ws.onopen = () => { attempt = 0; onOpen?.() }
    ws.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data)
        if (data.type === 'pong') return
        onMessage(data as TimelineEventV2)
      } catch { /* ignore parse errors */ }
    }
    ws.onclose = (ev) => {
      if (shouldClose) { onClose?.(); return }
      if (ev.code === 1008) { onClose?.(); return }
      const delay = backoffDelay()
      attempt++
      setTimeout(() => connect(), delay)
    }
    ws.onerror = () => {}
  }
  connect()
  return {
    close: () => { shouldClose = true; try { ws?.close() } catch {} },
  }
}

// ── Child Timeline (v2 – child_life_events) ────────────────────────────────

export type TimelineEventType =
  | 'placement_start' | 'placement_end' | 'placement_change'
  | 'school_change' | 'incident_report' | 'court_date' | 'legal_milestone'
  | 'medical_appointment' | 'therapy_session' | 'sibling_contact'
  | 'family_visitation' | 'milestone' | 'crisis_alert' | 'drift_threshold'
  | 'prediction_feedback' | 'twin_simulation' | 'caseworker_assignment'
  | 'caseworker_change' | 'manual_entry'
  // Live-feed simplified types
  | 'school' | 'medical' | 'incident' | 'visit' | 'legal' | 'placement' | 'note'

export type TimelineEventSealLevel = 'none' | 'partial' | 'full'

export type EventSeverity = 'low' | 'medium' | 'high' | 'critical'

export interface TimelineEventV2 {
  id: number
  child_id: string
  event_type: TimelineEventType
  event_date: string
  event_time?: string | null
  title?: string | null
  description?: string | null
  severity?: EventSeverity | null
  created_by?: string | null
  recorded_at: string
  source_table?: string | null
  source_id?: number | null
  conflict_resolution?: string | null
  payload: Record<string, unknown>
  is_verified: boolean
  verified_by?: string | null
  verified_at?: string | null
  superseded_by?: number | null
  seal_level: TimelineEventSealLevel
}

export interface TimelineResponse {
  child_id: string
  events: TimelineEventV2[]
  total: number
  page: number
  per_page: number
  total_pages: number
}

export interface CreateEventRequest {
  event_type: TimelineEventType
  event_date: string
  event_time?: string | null
  title?: string | null
  description?: string | null
  severity?: EventSeverity | null
  created_by?: string | null
  payload?: Record<string, unknown>
  seal_level?: TimelineEventSealLevel
  source_table?: string | null
  source_id?: number | null
}

export interface CreateEventResponse {
  status: string
  event: TimelineEventV2
  message: string
}

export interface QuickAddRequest {
  event_type: 'school' | 'medical' | 'incident' | 'visit' | 'legal' | 'placement' | 'note'
  title?: string | null
  description?: string | null
  severity?: EventSeverity | null
}

export interface ChildEventStats {
  child_id: string
  child_name: string
  age: number | null
  emergency_level: string
  special_needs: boolean
  location: string
  total_events: number
  by_type: Record<string, number>
  by_severity: Record<string, number>
  last_activity: string | null
  placement: {
    family_id?: string
    status?: string
    since?: string
  }
}

export interface VerifyEventResponse {
  status: string
  message: string
}

export interface TimelineExportResponse {
  status: string
  child_info: Record<string, unknown>
  events: TimelineEventV2[]
  total_events: number
  redaction_level: string
  generated_at: string
  pdf_url: string
  footer_disclosure: string
}

export async function getTimelineEvents(
  childId: string,
  params?: {
    page?: number
    per_page?: number
    event_type?: string
    seal_level?: string
    redact?: string
    start_date?: string
    end_date?: string
  },
): Promise<TimelineResponse> {
  const query: Record<string, string | number> = {}
  if (params?.page) query.page = params.page
  if (params?.per_page) query.per_page = params.per_page
  if (params?.event_type) query.event_type = params.event_type
  if (params?.seal_level) query.seal_level = params.seal_level
  if (params?.redact) query.redact = params.redact
  if (params?.start_date) query.start_date = params.start_date
  if (params?.end_date) query.end_date = params.end_date
  const res = await api.get<TimelineResponse>(
    `/api/timeline/${encodeURIComponent(childId)}`,
    { params: query },
  )
  return res.data
}

export async function createTimelineEvent(
  childId: string,
  data: CreateEventRequest,
): Promise<CreateEventResponse> {
  const res = await api.post<CreateEventResponse>(
    `/api/timeline/${encodeURIComponent(childId)}/events`,
    data,
  )
  return res.data
}

export async function quickAddEvent(
  childId: string,
  data: QuickAddRequest,
): Promise<CreateEventResponse> {
  const res = await api.post<CreateEventResponse>(
    `/api/timeline/${encodeURIComponent(childId)}/events/quick`,
    data,
  )
  return res.data
}

export async function getChildEventStats(childId: string): Promise<ChildEventStats> {
  const res = await api.get<ChildEventStats>(
    `/api/timeline/${encodeURIComponent(childId)}/stats`,
  )
  return res.data
}

export async function verifyTimelineEvent(
  childId: string,
  eventId: number,
  verifiedBy?: string,
): Promise<VerifyEventResponse> {
  const res = await api.post<VerifyEventResponse>(
    `/api/timeline/${encodeURIComponent(childId)}/verify/${eventId}`,
    { verified_by: verifiedBy || '' },
  )
  return res.data
}

export async function exportTimelinePdf(
  childId: string,
  redact?: string,
): Promise<TimelineExportResponse> {
  const params: Record<string, string> = {}
  if (redact) params.redact = redact
  const res = await api.get<TimelineExportResponse>(
    `/api/timeline/${encodeURIComponent(childId)}/export/pdf`,
    { params },
  )
  return res.data
}

// ── Old Timeline (legacy – kept for backward compat) ───────────────────────

export interface TimelineEvent {
  date: string | null
  type: 'entry' | 'placement' | 'workflow' | 'incident' | 'checkin'
  title: string
  description: string
  icon: string
  risk_score?: number
  mood_score?: number
  workflow_id?: string
}

export interface ChildTimeline {
  child_id: string
  child_name: string
  age: number | null
  emergency_level: string
  special_needs: boolean
  school: string | null
  milestones: unknown[]
  therapy_history: unknown[]
  timeline: TimelineEvent[]
}

export async function getChildTimeline(childId: string): Promise<ChildTimeline> {
  const res = await api.get<ChildTimeline>(
    `/children/${encodeURIComponent(childId)}/timeline`
  )
  return res.data
}

// ── Child Digital Twin ─────────────────────────────────────────────────────

export interface InterventionComponent {
  domain: string
  action: string
  value: string
  label?: string
}

export interface SimulateRequest {
  interventions: InterventionComponent[]
  horizon_days: number
}

export interface TrajectoryForecast {
  outcome_distribution: Record<string, { stable: number; disrupted: number; reunified: number; runaway: number }>
  ci_95: Record<string, Record<string, [number, number]>>
  dominant_outcome: string
  uncertainty_score: number
}

export interface EffectSummary {
  effect_size: number
  probability_of_benefit: number
  number_needed_to_treat: number
  ci_95: [number, number]
  decomposition?: {
    components?: { domain: string; alone: number }[]
    interaction_effect?: number
    interaction_pct?: number
  }
  robustness_value: number
  sensitivity: {
    confounder_strength_to_nullify: number
    most_sensitive_feature: string
    most_sensitive_feature_effect: number
    placebo_test_passed: boolean
    negative_control_passed: boolean
  }
}

export interface SimulateResponse {
  simulation_id: string
  child_id: string
  generated_at: string
  model_version: string
  n_historical_placements: number
  intervention: {
    type: string
    components: InterventionComponent[]
  }
  baseline: TrajectoryForecast
  counterfactual: TrajectoryForecast
  effect: EffectSummary
}

export interface TwinState {
  child_id: string
  placement_id: string | null
  as_of: string
  current_features: Record<string, unknown>
  outcome_probs: Record<string, number> | null
  pending_simulations: ScenarioData[] | null
  version: number
  stale_at: string
}

export interface ScenarioData {
  slot: 'A' | 'B' | 'C'
  label: string
  simulation_id: string
  interventions: InterventionComponent[]
  outcome_summary: string
  verdict: 'positive' | 'uncertain' | 'negative' | ''
  caseworker_note: string
  saved_at: string
  expires_at: string
}

export async function getTwinState(childId: string): Promise<TwinState> {
  const res = await api.get<TwinState>(
    `/api/twin/${encodeURIComponent(childId)}/state`
  )
  return res.data
}

export async function runSimulation(
  childId: string,
  request: SimulateRequest,
): Promise<SimulateResponse> {
  const res = await api.post<SimulateResponse>(
    `/api/twin/${encodeURIComponent(childId)}/simulate`,
    request,
  )
  return res.data
}

export async function saveScenario(
  childId: string,
  slot: string,
  scenario: {
    slot: string
    label: string
    simulation_id: string
    interventions: InterventionComponent[]
    outcome_summary: string
    verdict: string
    caseworker_note: string
  },
): Promise<{ status: string; message: string }> {
  const res = await api.patch<{ status: string; message: string }>(
    `/api/twin/${encodeURIComponent(childId)}/scenarios`,
    { slot, scenario },
  )
  return res.data
}

export async function getScenarios(
  childId: string,
): Promise<{ scenarios: ScenarioData[] }> {
  const res = await api.get<{ scenarios: ScenarioData[] }>(
    `/api/twin/${encodeURIComponent(childId)}/scenarios`,
  )
  return res.data
}

export async function getCaseConferencePdf(
  childId: string,
): Promise<Record<string, unknown>> {
  const res = await api.get<Record<string, unknown>>(
    `/api/twin/${encodeURIComponent(childId)}/case-conference-pdf`,
  )
  return res.data
}

// ── Reasoning Traces (AI Thoughts) ──────────────────────────────────────────

export interface ReasoningTrace {
  id: number
  workflow_id: string
  stage: string
  agent_name: string
  step_index: number
  content: string
  timestamp: string
}

export async function getReasoningTraces(
  workflowId: string,
  stage?: string,
): Promise<ReasoningTrace[]> {
  const params: Record<string, string> = {}
  if (stage) params.stage = stage
  const res = await api.get<{ traces: ReasoningTrace[] }>(
    `/api/workflow/${encodeURIComponent(workflowId)}/reasoning`,
    { params },
  )
  return res.data?.traces ?? []
}

// ── Agent Executions ────────────────────────────────────────────────────────

export interface AgentExecution {
  id: number
  workflow_id: string
  stage: string
  agent_name: string
  action: string | null
  output: string | null
  confidence: number | null
  latency_seconds: number | null
  status: string
  details: Record<string, unknown> | null
  started_at: string
  completed_at: string | null
}

export async function getAgentExecutions(
  workflowId?: string,
): Promise<AgentExecution[]> {
  if (!workflowId) return []
  const res = await api.get<{ executions: AgentExecution[] }>(
    `/api/workflow/${encodeURIComponent(workflowId)}/executions`
  )
  return res.data?.executions ?? []
}

// ── Monitoring Summary ──────────────────────────────────────────────────────

export interface MonitoringSummary {
  agents: Record<string, {
    name: string
    status: string
    last_heartbeat_age_s: number | null
  }>
  metrics: {
    active_workflows: number
    running_pipelines: number
    success_rate: number
    average_latency_ms: number
    failure_count: number
    total_executions: number
    pipeline_completion_rate: number
  }
}

export async function getMonitoringSummary(): Promise<MonitoringSummary> {
  const res = await api.get<MonitoringSummary>('/api/monitoring/summary')
  return res.data
}

// ── ML Audit ──────────────────────────────────────────────────────────────────

export interface AuditVerifyResult {
  valid: boolean
  checked: number
  broken_links: { id: number; issues: string[] }[]
  oldest_row: string | null
  newest_row: string | null
  message: string
}

export async function getAuditVerify(): Promise<AuditVerifyResult> {
  const res = await api.get<AuditVerifyResult>('/api/ml-audit/verify')
  return res.data
}

export interface AuditDecision {
  id: number
  child_id: string
  placement_id: string
  decision_type: string
  model_name: string
  model_version: string
  child_demographics: Record<string, unknown>
  output_score: number
  output_label: string
  output_confidence: number
  human_overridden: boolean
  human_decision: string
  decided_at: string
  hash: string
}

export interface AuditDecisionsResponse {
  decisions: AuditDecision[]
  total: number
  limit: number
  offset: number
}

export async function getDecisionsByChildId(childId: string): Promise<AuditDecisionsResponse> {
  const res = await api.get<AuditDecisionsResponse>('/api/ml-audit/decisions', {
    params: { child_id: childId, decision_type: 'placement_match', limit: 1 },
  })
  return res.data
}

export async function getAuditDecisions(params?: {
  child_id?: string
  decision_type?: string
  limit?: number
  offset?: number
}): Promise<AuditDecisionsResponse> {
  const res = await api.get<AuditDecisionsResponse>('/api/ml-audit/decisions', { params })
  return res.data
}
