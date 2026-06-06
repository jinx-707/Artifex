export interface ReferralSubmission {
  child_id: string
  age: number
  gender: string
  special_needs: boolean
  languages: string
  medical_needs: string
  behavioral_support: string
  sibling_group: boolean
  emergency_level: string
  preferred_location: string
  foster_home_type: string
  capacity_needed: number
  accessibility_needs: boolean
  school_continuity: boolean
  risk_flags: string[]
  notes: string
}

export interface ReferralResponse {
  workflow_id: string
  child_id: string
  status: string
  message: string
  created_at: string
}

export interface WorkflowNestedStatus {
  stage?: string
  status?: string
  child_id?: string
  family_id?: string
  risk_score?: number
  risk_history?: Array<{ score: number; check_score?: number; notes?: string; timestamp?: string }>
  alert_sent?: boolean
  active?: boolean
  workflow_id?: string
  current_stage?: string
  stages?: WorkflowStage[]
  recommended_family?: string
  metadata?: Record<string, unknown>
  created_at?: string
  updated_at?: string
}

export interface TopMatch {
  family: Record<string, unknown>
  match_score: number
  blended_score: number
  confidence_score: number
  risk_probability: number
  capacity: number
  explanation: string
}

export interface WorkflowActivityEntry {
  timestamp: string
  message: string
  stage?: string
  status?: string
}

export interface WorkflowStatus {
  workflow_id: string
  status: string
  active: boolean
  child_id?: string
  family_id?: string
  recommended_family?: string | Record<string, unknown>
  match_score?: number | null
  confidence_score?: number | null
  risk_score?: number | null
  current_stage?: string
  progress?: number
  stages?: WorkflowStage[]
  timeline?: WorkflowStage[]
  feature_importance?: Array<Record<string, unknown>> | null
  top_matches?: TopMatch[] | null
  capacity?: number | null
  metadata?: Record<string, unknown>
  activity_feed?: WorkflowActivityEntry[]
  created_at?: string | null
  updated_at?: string | null
}

export interface WorkflowStage {
  stage: string
  status: 'pending' | 'in_progress' | 'completed' | 'failed' | string
  data?: Record<string, unknown> | null
  timestamp?: string
  started_at?: string
  completed_at?: string
  duration?: string
  details?: string
  name?: string
  label?: string
}

export interface PendingApproval {
  workflow_id?: string
  child_id?: string
  emergency_level?: string
  risk_score?: number
  recommended_family?: string
  status?: string
  created_at?: string
  age?: number
  gender?: string
  location?: string
}

export interface ApproveRequest {
  workflow_id: string
  approved: boolean
  comment: string
}

export interface ApproveResponse {
  status: string
  message?: string
}

export interface Placement {
  id?: string
  workflow_id?: string

  child_id?: string
  family_id?: string
  family_json?: Record<string, unknown> | null
  recommended_family?: string | Record<string, unknown>
  foster_family_name?: string
  family?: Record<string, unknown>
  location?: string
  emergency_level?: string
  risk_score?: number
  match_explanation?: string | null
  last_notes?: string | null
  status?: string
  current_stage?: string
  progress?: number

  // API timestamp fields
  created_at?: string | null
  updated_at?: string | null

  placement_date?: string
  match_score?: number
  confidence_score?: number
  capacity?: number
  siblings_accommodated?: boolean
  special_needs_met?: string[]
  top_matches?: TopMatch[]
  feature_importance?: Array<{ feature: string; importance: number }> | any[]
}


export interface HealthStatus {
  status: string
  service?: string
  version?: string
  uptime?: string
  services?: {
    nats?: ServiceHealth
    temporal?: ServiceHealth
    postgres?: ServiceHealth
    agents?: AgentHealth
  }
}

export interface ServiceHealth {
  status: string
  latency_ms?: number
  message?: string
}

export interface AgentHealth {
  total: number
  active: number
  failed: number
  agents: AgentStatus[]
}

export interface AgentStatus {
  name: string
  status: 'active' | 'inactive' | 'error' | 'busy'
  task: string
  uptime?: string
  last_heartbeat: string
  workflows_processed: number
}

export interface EventMessage {
  id: string
  type: string
  source: string
  message: string
  severity: 'info' | 'warning' | 'error' | 'success'
  timestamp: string
  workflow_id?: string
}

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant' | 'system'
  content: string
  timestamp: string
  sources?: string[]
  actions?: ChatAction[]
}

export interface ChatAction {
  label: string
  action: string
  payload?: Record<string, unknown>
}

export interface ChatRequest {
  message: string
  workflow_id?: string
  context?: Record<string, unknown>
}

export interface ChatResponse {
  id: string
  message: string
  sources?: string[]
  actions?: ChatAction[]
}

export interface DashboardMetrics {
  active_workflows: number
  pending_approvals: number
  placements_matched: number
  emergency_referrals: number
  workflows_change: number
  approvals_change: number
  placements_change: number
  emergency_change: number
}

export interface DashboardEventsResponse {
  events: WorkflowEvent[]
}

export interface AgentStatusMap {
  agents: Record<string, AgentStatusItem>
}

export interface AgentStatusItem {
  name: string
  status: string
  last_heartbeat_age_s: number | null
}

export interface WorkflowEvent {
  id: string
  type: string
  workflow_id: string
  workflow_stage: string
  child_id: string
  message: string
  timestamp: string
}

export interface RiskDistribution {
  low: number
  medium: number
  high: number
  critical: number
}

export interface PendingApprovalsResponse {
  approvals: PendingApproval[]
  count: number
}

export interface Family {
  id: number
  family_id: string
  name: string
  location: string
  capacity: number
  available_capacity: number
  experience: string
  specializations: string
  languages: string
  special_needs_trained: boolean
  accepts_siblings: boolean
  emergency_available: boolean
  max_age: number
  can_take_siblings: boolean
  has_animals: boolean
  created_at: string | null
  updated_at: string | null
}

export interface FamilyCreate {
  name: string
  location?: string
  capacity?: number
  experience?: string
  specializations?: string
  languages?: string
  special_needs_trained?: boolean
  accepts_siblings?: boolean
  emergency_available?: boolean
  max_age?: number
  can_take_siblings?: boolean
  has_animals?: boolean
}

export interface FamilyUpdate {
  name?: string
  location?: string
  capacity?: number
  experience?: string
  specializations?: string
  languages?: string
  special_needs_trained?: boolean
  accepts_siblings?: boolean
  emergency_available?: boolean
  max_age?: number
  can_take_siblings?: boolean
  has_animals?: boolean
}
