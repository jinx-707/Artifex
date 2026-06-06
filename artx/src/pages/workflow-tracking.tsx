import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useParams, useNavigate } from 'react-router-dom'
import api from '@/services/api'
import { GlassCard } from '@/components/ui/glass-card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { StatusBadge } from '@/components/ui/badge'
import { DataLoader } from '@/components/data-loader'
import { formatDate } from '@/lib/utils'
import { normalizeWorkflowId, subscribeWorkflowStream } from '@/services/foster'
import { motion } from 'framer-motion'
import {
  Search, ArrowLeft, RefreshCw, Bot, BrainCircuit,
  Clock, Timer, ListChecks,
} from 'lucide-react'
import { Link } from 'react-router-dom'
import type { WorkflowStatus, WorkflowStage } from '@/types'
import { useState, useEffect, useCallback, useRef, useMemo } from 'react'
import { cn } from '@/lib/utils'
import type { TimelineEvent, StageStatus, ExecutionMetrics, ReasoningEntry } from '@/types/workflow-timeline'
import { MOCK_TIMELINE_EVENTS, MOCK_REASONING_ENTRIES } from '@/types/workflow-timeline'
import ExecutionMetricsBar from '@/components/workflow-timeline/ExecutionMetricsBar'
import TimelineList from '@/components/workflow-timeline/TimelineList'
import AgentActivityPanel from '@/components/workflow-timeline/AgentActivityPanel'
import AIReasoningFeed from '@/components/workflow-timeline/AIReasoningFeed'
import TimelineDetailDrawer from '@/components/workflow-timeline/TimelineDetailDrawer'
import ReplayButton from '@/components/workflow-timeline/ReplayButton'

// ── Pipeline stage ordering ───────────────────────────────────────────────────
// Index position defines priority: higher index = later in pipeline.
// Any stage with a later index being completed forces all earlier stages to completed.
const PIPELINE_ORDER = [
  'referral_submitted',       // 0
  'eligibility_validated',    // 1
  'child_profile_created',    // 2
  'risk_assessment',          // 3
  'family_matching',          // 4
  'fairness_validation',      // 5
  'recommendation_generated', // 6
  'supervisor_approval',      // 7
  'awaiting_approval',        // 8
  'placement_approved',       // 9
  'placement_created',        // 10
  'monitoring_active',        // 11
]

const STATUS_RANK: Record<string, number> = {
  completed: 4, in_progress: 3, failed: 2, pending: 1,
}

// Canonical mapping from raw backend status strings → StageStatus
const RAW_STATUS_MAP: Record<string, StageStatus> = {
  completed:   'completed',
  approved:    'completed',
  active:      'in_progress',
  running:     'in_progress',
  started:     'pending',
  awaiting:    'in_progress',

  in_progress: 'in_progress',
  pending:     'pending',
  failed:      'failed',
  needs_manual_review: 'in_progress',
}

/**
 * reconcileStages — the core fix.
 *
 * Rules:
 * 1. Find the highest pipeline index that has status === 'completed'.
 * 2. Every stage with pipeline index <= that index → force to 'completed'.
 * 3. The highest completed stage itself stays 'completed'.
 * 4. Stages beyond the highest completed stage keep their actual status.
 *
 * This means: if family_matching (idx 4) is completed, then
 * risk_assessment (idx 3) CANNOT stay in_progress — it is forced to completed.
 */
function reconcileStages(events: TimelineEvent[]): TimelineEvent[] {
  if (events.length === 0) return events

  // Only consider stages we actually render in PIPELINE_ORDER.
  // Unknown stages are left untouched.
  let highestCompletedIdx = -1

  for (const ev of events) {
    if (ev.status === 'completed') {
      const idx = PIPELINE_ORDER.indexOf(ev.stage)
      if (idx > highestCompletedIdx) highestCompletedIdx = idx
    }
  }

  console.log('[reconcile] highest completed stage index:', highestCompletedIdx,
    highestCompletedIdx >= 0 ? `(${PIPELINE_ORDER[highestCompletedIdx]})` : '')

  if (highestCompletedIdx < 0) return events

  return events.map((ev) => {
    const stageIdx = PIPELINE_ORDER.indexOf(ev.stage)
    if (stageIdx < 0) return ev

    // Force ALL stages at or before the highest completed stage → completed
    if (stageIdx <= highestCompletedIdx && ev.status !== 'completed') {
      console.log(`[reconcile] ${ev.stage} (idx ${stageIdx}) → completed (forced by idx ${highestCompletedIdx})`)
      return { ...ev, status: 'completed' as StageStatus }
    }
    return ev
  })
}


function formatPercent(value?: number | null, decimals = 0): string {
  if (value == null || Number.isNaN(value)) return '\u2014'
  const normalized = value <= 1 ? value * 100 : value
  return `${Number(normalized.toFixed(decimals))}%`
}

function formatRiskScore(value?: number | null): string {
  if (value == null || Number.isNaN(value)) return '\u2014'
  const normalized = value <= 1 ? value * 100 : value
  return `${normalized.toFixed(normalized < 10 ? 2 : 0)}%`
}

function apiEventsToTimeline(rawEvents: WorkflowStage[]): TimelineEvent[] {
  const seen = new Map<string, TimelineEvent>()

  for (const e of rawEvents || []) {
    const stageName = (e.stage || e.name || '').trim()
    if (!stageName) continue

    const status: StageStatus = RAW_STATUS_MAP[e.status] ?? 'pending'

    // Parse data field
    let parsedData: Record<string, unknown> = {}
    if (typeof e.data === 'object' && e.data !== null) {
      parsedData = e.data as Record<string, unknown>
    } else if (typeof e.data === 'string' && e.data.trim().startsWith('{')) {
      try { parsedData = JSON.parse(e.data) } catch { parsedData = {} }
    }

    // Parse details field
    let detailsStr = ''
    if (typeof e.details === 'string' && e.details.trim().startsWith('{')) {
      try {
        const dp = JSON.parse(e.details) as Record<string, unknown>
        parsedData = { ...parsedData, ...dp }
      } catch { detailsStr = e.details }
    } else {
      detailsStr = e.details || ''
    }

    const agentName         = (parsedData.agent as string)              || ''
    const agentAction       = (parsedData.action as string)             || ''
    const agentOutput       = (parsedData.output as string)             || ''
    const latency           = typeof parsedData.latency === 'number' ? parsedData.latency : 0
    const confidence        = Number(parsedData.confidence ?? 0)
    const confidenceScore   = (parsedData.confidence_score
      ?? (confidence <= 1 ? Math.round(confidence * 100) : Math.round(confidence))) as number
    const reasoning         = Array.isArray(parsedData.reasoning) ? parsedData.reasoning as string[] : []
    const inputData         = (parsedData.inputData as string) || (parsedData.input as string) || ''
    const outputData        = (parsedData.outputData as string) || ''
    const decisionExpl      = (parsedData.decisionExplanation as string) || ''
    const logs              = Array.isArray(parsedData.logs) ? parsedData.logs as string[] : []
    if (!detailsStr) detailsStr = (parsedData.message as string) || (parsedData.details as string) || ''
    const stagePayload      = Object.keys(parsedData).length > 0 ? parsedData : undefined

    const existing    = seen.get(stageName)
    const rank        = STATUS_RANK[status] ?? 0
    const existingRank = existing ? (STATUS_RANK[existing.status] ?? 0) : -1

    if (!existing || rank > existingRank) {
      const raw   = e.label || stageName.replace(/_/g, ' ')
      const label = raw.charAt(0).toUpperCase() + raw.slice(1)
      seen.set(stageName, {
        id: `api-${stageName}`,
        stage: stageName, label, status,
        agentName, agentAction, agentOutput,
        latency, confidenceScore,
        reasoning, inputData, outputData, decisionExplanation: decisionExpl, logs,
        timestamp:   e.timestamp || e.started_at,
        startedAt:   e.started_at,
        completedAt: e.completed_at,
        details:     detailsStr || undefined,
        payload:     stagePayload,
      })
    } else if (existing && !existing.agentName && agentName) {
      seen.set(stageName, {
        ...existing,
        agentName,
        agentAction: agentAction || existing.agentAction,
        agentOutput: agentOutput || existing.agentOutput,
        latency:     latency     || existing.latency,
        confidenceScore: confidenceScore || existing.confidenceScore,
        reasoning:   reasoning.length > 0 ? reasoning : existing.reasoning,
      })
    }
  }

  return Array.from(seen.values())
}

/** Sort events by pipeline order; unknown stages go to the end sorted by timestamp. */
function sortTimelineEvents(events: TimelineEvent[]): TimelineEvent[] {
  return [...events].sort((a, b) => {
    const ia = PIPELINE_ORDER.indexOf(a.stage)
    const ib = PIPELINE_ORDER.indexOf(b.stage)
    if (ia >= 0 && ib >= 0) return ia - ib
    if (ia >= 0) return -1
    if (ib >= 0) return 1
    return (a.timestamp ? new Date(a.timestamp).getTime() : 0) -
           (b.timestamp ? new Date(b.timestamp).getTime() : 0)
  })
}

/** Build and reconcile a timeline from raw WorkflowStage events in one pass. */
function buildTimeline(rawEvents: WorkflowStage[]): TimelineEvent[] {
  const parsed = apiEventsToTimeline(rawEvents)

  // highest stage based on completed events (for debug + correctness)
  const highestCompletedIdx = parsed.reduce((acc, ev) => {
    if (ev.status !== 'completed') return acc
    const idx = PIPELINE_ORDER.indexOf(ev.stage)
    return idx > acc ? idx : acc
  }, -1)

  console.log('[buildTimeline] highest stage:', highestCompletedIdx,
    highestCompletedIdx >= 0 ? `(${PIPELINE_ORDER[highestCompletedIdx]})` : '')

  const reconciled = reconcileStages(parsed)
  const sorted = sortTimelineEvents(reconciled)

  // Debug: show which stage reconciliation thinks is completed/highest
  const highestAfterReconcile = sorted.reduce((acc, ev) => {
    if (ev.status !== 'completed') return acc
    const idx = PIPELINE_ORDER.indexOf(ev.stage)
    return idx > acc ? idx : acc
  }, -1)

  console.log('[reconcile] highest after reconcile:', highestAfterReconcile,
    highestAfterReconcile >= 0 ? `(${PIPELINE_ORDER[highestAfterReconcile]})` : '')

  console.log('[timeline state]', sorted.map((e) => `${e.stage}=${e.status}`).join(' → '))
  return sorted
}


const KNOWN_STAGE_COUNT = PIPELINE_ORDER.length

function computeMetrics(events: TimelineEvent[], elapsed: number): ExecutionMetrics {
  const completed  = events.filter((e) => e.status === 'completed').length
  const inProgress = events.filter((e) => e.status === 'in_progress').length
  const total      = Math.max(events.length, KNOWN_STAGE_COUNT)
  const progress   = events.length > 0 ? Math.min(100, (completed / total) * 100) : 0
  return {
    progress,
    completedStages:  completed,
    totalStages:      total,
    executionTime:    elapsed,
    activeAgents:     inProgress,
    messagesExchanged: events.reduce((s, e) => s + (e.reasoning?.length || 0) + (e.logs?.length || 0), 0),
    riskScore:  45,
    matchScore: 30,
    confidenceScore: completed > 0
      ? Math.round(events.filter((e) => e.status === 'completed')
          .reduce((s, e) => s + e.confidenceScore, 0) / completed)
      : 0,
  }
}

function extractWorkflowIdFromPath(): string {
  const m = window.location.pathname.match(/\/workflow\/(.+)/)
  return m?.[1] ? decodeURIComponent(m[1]) : ''
}

const REPLAY_DELAYS    = [1, 5, 9.5, 14.5, 20, 25.5, 31, 36.5, 42, 47]
const REPLAY_TOTAL_STEPS = MOCK_TIMELINE_EVENTS.length

// ─────────────────────────────────────────────────────────────────────────────

export default function WorkflowTrackingPage() {
  const params   = useParams<{ workflowId: string }>()
  const navigate = useNavigate()

  const routeId         = params.workflowId || extractWorkflowIdFromPath()
  const initialNorm     = routeId ? normalizeWorkflowId(routeId) : ''

  const [workflowId,   setWorkflowId]   = useState(initialNorm)
  const [searchInput,  setSearchInput]  = useState(routeId || '')
  const queryClient = useQueryClient()

  const wsRef       = useRef<ReturnType<typeof subscribeWorkflowStream> | null>(null)
  const [selectedStageId, setSelectedStageId] = useState<string | null>(null)
  const [drawerStageId,   setDrawerStageId]   = useState<string | null>(null)
  const [activeTab,       setActiveTab]       = useState<'activity' | 'reasoning'>('activity')

  const [isReplaying, setIsReplaying] = useState(false)
  const [hasReplayed, setHasReplayed] = useState(false)
  const [replayStep,  setReplayStep]  = useState(-1)
  const [elapsed,     setElapsed]     = useState(0)
  const timersRef    = useRef<ReturnType<typeof setTimeout>[]>([])
  const intervalRef  = useRef<ReturnType<typeof setInterval> | null>(null)
  const startTimeRef = useRef(0)

  // liveEvents is the single source of truth for the displayed timeline
  const [liveEvents, setLiveEvents] = useState<TimelineEvent[]>([])
  // seenKeys prevents duplicate WS events from being applied twice
  const seenKeys = useRef<Set<string>>(new Set())
  // Track which workflowId liveEvents were last seeded for, to force re-seed on ID change
  const seededForId = useRef<string>('')

  const clearAllTimers = useCallback(() => {
    timersRef.current.forEach(clearTimeout)
    timersRef.current = []
    if (intervalRef.current) { clearInterval(intervalRef.current); intervalRef.current = null }
  }, [])

  // ── Sync route param → state ──────────────────────────────────────────────
  useEffect(() => {
    const id = params.workflowId || extractWorkflowIdFromPath()
    if (id) {
      const norm = normalizeWorkflowId(id)
      setWorkflowId(norm)
      setSearchInput(id)
      if (norm !== id) navigate(`/workflow/${norm}`, { replace: true })
    }
  }, [params.workflowId, navigate])

  // ── REST query ────────────────────────────────────────────────────────────
  const { data: workflow, isLoading, error, refetch } = useQuery<WorkflowStatus>({
    queryKey: ['workflow-status', workflowId],
    queryFn: async () => {
      if (!workflowId) throw new Error('No workflow ID')
      console.log('[workflow] Loading:', workflowId)
      const res = await api.get<WorkflowStatus>(`/foster/status/${encodeURIComponent(workflowId)}`)
      const tl = Array.isArray(res.data?.timeline) ? res.data.timeline : []
      console.log('[workflow] Events count:', tl.length, '| current_stage:', res.data?.current_stage)
      return {
        ...res.data,
        status:        res.data?.status        || 'unknown',
        current_stage: res.data?.current_stage || 'Unknown',
        progress:      typeof res.data?.progress === 'number' ? res.data.progress : 0,
        timeline:      tl,
      } as WorkflowStatus
    },
    enabled:              !!workflowId?.trim(),
    refetchOnWindowFocus: true,
    staleTime:            0,
    refetchInterval: (query) => {
      const d = query.state.data as WorkflowStatus | undefined
      if (!d) return 5000
      return ['approved', 'rejected', 'closed', 'completed'].includes(d.status) ? false : 8000
    },
    retry:      2,
    retryDelay: (a) => Math.min(1000 * 2 ** a, 4000),
  })

  // ── Seed liveEvents from REST response ────────────────────────────────────
  // Deps: workflowId + stable JSON of timeline content.
  // Using both ensures the effect re-runs when either the ID changes OR
  // the timeline content changes (e.g. after a poll refetch adds more events).
  const timelineJson = useMemo(
    () => JSON.stringify(workflow?.timeline ?? []),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [workflow?.timeline],
  )

  useEffect(() => {
    // workflowId not yet resolved or mismatched with what query returned
    if (!workflowId) return
    if (!workflow?.timeline) return

    console.log('[workflow loaded]', workflowId)
    const rawEvents = workflow.timeline as WorkflowStage[]
    console.log('[events]', rawEvents)

    const built = buildTimeline(rawEvents)


    console.log('[rest] workflowId:', workflowId, '| raw events:', rawEvents.length, '| built:', built.length)
    console.log('[rest] events:', built.map((e) => `${e.stage}=${e.status}`).join(', '))

    if (built.length === 0) return

    // If workflowId changed, force a full replace (don't merge with stale prev state)
    const isNewWorkflow = seededForId.current !== workflowId
    seededForId.current = workflowId

    setLiveEvents((prev) => {
      const base: TimelineEvent[] = isNewWorkflow ? [] : prev
      // Merge: for each stage, keep the higher-ranked status
      const map = new Map(base.map((e) => [e.stage, e]))
      for (const ev of built) {
        const existing = map.get(ev.stage)
        if (!existing || (STATUS_RANK[ev.status] ?? 0) >= (STATUS_RANK[existing.status] ?? 0)) {
          map.set(ev.stage, ev)
          seenKeys.current.add(`${ev.stage}:${ev.status}`)
        }
      }
      const merged = sortTimelineEvents(reconcileStages(Array.from(map.values())))
      console.log('[rest] final timeline state:', merged.map((e) => `${e.stage}=${e.status}`).join(' → '))
      return merged
    })
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workflowId, timelineJson])  // workflowId forces re-seed when ID changes; timelineJson for content changes

  // ── WebSocket subscription ────────────────────────────────────────────────
  useEffect(() => {
    if (!workflowId || isReplaying) return
    if (wsRef.current) { wsRef.current.close(); wsRef.current = null }

    console.log('[ws] connecting for workflowId:', workflowId)


    // Reset dedup keys on every new workflowId so stale entries from a
    // previous workflow never block events for the new one
    seenKeys.current = new Set()

    const sub = subscribeWorkflowStream(
      workflowId,
      (msg) => {
        if (msg.type === 'ping') return
        console.log('[ws] Received:', msg.type, '| stage:', msg.stage, '| status:', msg.status)

        // Update React Query cache metadata
        queryClient.setQueryData(['workflow-status', workflowId], (cur) => {
          const existing = (cur as WorkflowStatus) || {
            workflow_id: workflowId, status: 'unknown', active: true, progress: 0, timeline: [],
          }
          return {
            ...existing, ...msg,
            status:        msg.status        || existing.status        || 'unknown',
            current_stage: msg.current_stage || existing.current_stage || 'Unknown',
            progress:      typeof msg.progress === 'number' ? msg.progress : existing.progress || 0,
            timeline:      existing.timeline || [],
          } as WorkflowStatus
        })

        // ── Snapshot: full DB timeline from server ──────────────────────────
        if (msg.type === 'workflow_snapshot') {
          const raw = Array.isArray(msg.timeline) ? msg.timeline as WorkflowStage[] : []
          console.log('[ws] Snapshot events:', raw.length)
          const built = buildTimeline(raw)
          setLiveEvents((prev) => {
            const map = new Map(prev.map((e) => [e.stage, e]))
            for (const ev of built) {
              const existing = map.get(ev.stage)
              if (!existing || (STATUS_RANK[ev.status] ?? 0) >= (STATUS_RANK[existing.status] ?? 0)) {
                map.set(ev.stage, ev)
                seenKeys.current.add(`${ev.stage}:${ev.status}`)
              }
            }
            return sortTimelineEvents(reconcileStages(Array.from(map.values())))
          })
          return
        }

        // ── Individual event ────────────────────────────────────────────────
        if (msg.type === 'workflow_event') {
          const stageName = (msg.stage || '').toLowerCase().replace(/\s+/g, '_')
          const status    = RAW_STATUS_MAP[msg.status || ''] ?? 'in_progress'
          const key       = `${stageName}:${status}`

          if (seenKeys.current.has(key)) return
          seenKeys.current.add(key)

          const raw   = (msg.stage || '').replace(/_/g, ' ')
          const label = raw.charAt(0).toUpperCase() + raw.slice(1)

          const sp = msg.payload as Record<string, unknown> | undefined
          const pp: Record<string, unknown> = sp ? { ...sp } : {}
          if (sp) {
            for (const [k, v] of Object.entries(sp)) {
              if (typeof v === 'string' && v.trim().startsWith('{')) {
                try { pp[k] = JSON.parse(v) } catch { /* keep */ }
              }
            }
          }

          const newEv: TimelineEvent = {
            id:    `live-${stageName}-${status}-${Date.now()}`,
            stage: stageName, label, status,
            agentName:           (pp.agent  as string) || '',
            agentAction:         (pp.action as string) || '',
            agentOutput:         (pp.output as string) || '',
            latency:             typeof pp.latency === 'number' ? pp.latency : 0,
            confidenceScore:     typeof pp.confidence === 'number'
                                   ? Math.round(pp.confidence * 100)
                                   : typeof pp.confidence_score === 'number'
                                     ? Math.round(pp.confidence_score) : 0,
            reasoning:           Array.isArray(pp.reasoning) ? pp.reasoning as string[] : [],
            inputData:           (pp.inputData as string) || (pp.input as string) || '',
            outputData:          (pp.outputData as string) || '',
            decisionExplanation: (pp.decisionExplanation as string) || '',
            logs:                Array.isArray(pp.logs) ? pp.logs as string[] : [],
            timestamp:           msg.timestamp,
            startedAt:           msg.timestamp,
            completedAt:         status === 'completed' ? msg.timestamp : undefined,
            details:             (pp.message as string) || (pp.details as string) || undefined,
            payload:             Object.keys(pp).length > 0 ? pp : undefined,
          }

          setLiveEvents((prev) => {
            const newRank = STATUS_RANK[status] ?? 0
            const idx = prev.findIndex((e) => e.stage === stageName)
            let updated: TimelineEvent[]
            if (idx >= 0) {
              const oldRank = STATUS_RANK[prev[idx].status] ?? 0
              if (newRank >= oldRank) {
                updated = [...prev]
                updated[idx] = newEv
              } else {
                return prev
              }
            } else {
              updated = [...prev, newEv]
            }
            return sortTimelineEvents(reconcileStages(updated))
          })
        }
      },
      () => {},
      () => {},
    )
    wsRef.current = sub
    return () => { sub.close(); wsRef.current = null }
  }, [workflowId, queryClient, isReplaying])

  // ── Derived state ─────────────────────────────────────────────────────────
  const timelineEvents = useMemo(() => {
    if (isReplaying) return MOCK_TIMELINE_EVENTS.filter((_, i) => i <= replayStep)
    return liveEvents
  }, [isReplaying, replayStep, liveEvents])

  const selectedStage = useMemo(
    () => timelineEvents.find((e) => e.id === selectedStageId) || null,
    [timelineEvents, selectedStageId],
  )

  const drawerStage = useMemo(() => {
    const combined = isReplaying ? MOCK_TIMELINE_EVENTS : timelineEvents
    return combined.find((e) => e.id === drawerStageId) || null
  }, [drawerStageId, isReplaying, timelineEvents])

  const metrics = useMemo(() => computeMetrics(timelineEvents, elapsed), [timelineEvents, elapsed])

  const reasoningEntries = useMemo<ReasoningEntry[]>(() => {
    if (isReplaying) {
      const ratio = replayStep / REPLAY_TOTAL_STEPS
      return MOCK_REASONING_ENTRIES.slice(0, Math.max(1, Math.floor(ratio * MOCK_REASONING_ENTRIES.length)))
    }
    const entries: ReasoningEntry[] = []
    let n = 0
    for (const ev of timelineEvents) {
      for (const step of (ev.reasoning || [])) {
        entries.push({
          id:        `reasoning-${n++}`,
          timestamp: ev.timestamp || ev.startedAt || new Date().toISOString(),
          agentName: ev.agentName || ev.stage.replace(/_/g, ' '),
          content:   step,
        })
      }
    }
    return entries
  }, [isReplaying, replayStep, timelineEvents])

  // Auto-select: prefer in_progress stage, fallback to highest completed
  useEffect(() => {
    if (timelineEvents.length === 0) return
    const active = timelineEvents.find((e) => e.status === 'in_progress')
    if (active) { setSelectedStageId(active.id); return }
    const lastCompleted = [...timelineEvents]
      .filter((e) => e.status === 'completed')
      .sort((a, b) => PIPELINE_ORDER.indexOf(b.stage) - PIPELINE_ORDER.indexOf(a.stage))[0]
    if (lastCompleted) setSelectedStageId(lastCompleted.id)
  }, [timelineEvents])

  // ── Replay ────────────────────────────────────────────────────────────────
  const handleReplay = useCallback(() => {
    if (isReplaying) {
      clearAllTimers()
      setIsReplaying(false); setReplayStep(-1); setElapsed(0)
      setSelectedStageId(null); setDrawerStageId(null)
      return
    }
    setHasReplayed(true)
    setIsReplaying(true); setReplayStep(-1); setElapsed(0)
    startTimeRef.current = Date.now()
    intervalRef.current = setInterval(() => {
      setElapsed(Math.floor((Date.now() - startTimeRef.current) / 1000))
    }, 200)
    const scheduleNext = (step: number) => {
      if (step >= REPLAY_TOTAL_STEPS) {
        setTimeout(() => { setIsReplaying(false); if (intervalRef.current) clearInterval(intervalRef.current) }, 2000)
        return
      }
      const delayMs = step === 0
        ? REPLAY_DELAYS[0] * 1000
        : (REPLAY_DELAYS[step] - REPLAY_DELAYS[step - 1]) * 1000
      const t = setTimeout(() => {
        setReplayStep(step)
        if (step < MOCK_TIMELINE_EVENTS.length) setSelectedStageId(MOCK_TIMELINE_EVENTS[step].id)
        scheduleNext(step + 1)
      }, delayMs)
      timersRef.current.push(t)
    }
    scheduleNext(0)
  }, [isReplaying, clearAllTimers])

  // ── Search ────────────────────────────────────────────────────────────────
  // Key fix: we update workflowId synchronously so the query key changes,
  // then force the new query to fetch immediately via refetchQueries.
  const handleSearch = useCallback(() => {
    const raw = searchInput.trim()
    if (!raw) return
    const norm = normalizeWorkflowId(raw)
    console.log('[search] Searching for:', norm)

    // Reset all live state
    setLiveEvents([])
    seenKeys.current = new Set()
    seededForId.current = ''
    setSelectedStageId(null)
    setDrawerStageId(null)
    setIsReplaying(false)
    setHasReplayed(false)
    setReplayStep(-1)
    setElapsed(0)

    // Close existing WS — the workflowId useEffect will reconnect
    if (wsRef.current) { wsRef.current.close(); wsRef.current = null }

    // Force-load immediately: update ID + kick the query/seed cycle.
    setWorkflowId(norm)
    // Schedule in microtask so React state updates land before refetch.
    Promise.resolve().then(() => {
      refetch()
    })

    navigate(`/workflow/${norm}`, { replace: true })
  }, [searchInput, navigate, refetch])


  const handleStageClick = useCallback((id: string) => {
    setSelectedStageId(id)
    setDrawerStageId(id)
  }, [])

  useEffect(() => () => { clearAllTimers(); wsRef.current?.close() }, [clearAllTimers])

  const progressPct  = Math.round(metrics.progress)
  const showContent  = !!workflowId

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} className="space-y-5">
      <div className="flex items-center gap-4 flex-wrap">
        <Link to="/" className="text-muted-foreground hover:text-foreground transition-colors shrink-0">
          <ArrowLeft size={20} />
        </Link>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-3">
            <h1 className="text-xl lg:text-2xl font-bold text-foreground">Workflow Timeline</h1>
            {isReplaying && (
              <span className="px-2 py-0.5 rounded-full bg-warning/10 border border-warning/20 text-warning text-[10px] font-semibold font-mono uppercase tracking-wider animate-pulse">
                Replay Mode
              </span>
            )}
            {workflow?.status && !isReplaying && <StatusBadge status={workflow.status} />}
          </div>
          <p className="text-xs sm:text-sm text-muted-foreground mt-0.5">
            {isReplaying
              ? 'AI agent collaboration during foster placement decision-making'
              : 'Real-time AI orchestration pipeline with live agent activity'}
          </p>
        </div>
      </div>

      {/* Search bar */}
      <GlassCard className="p-4">
        <div className="flex flex-col sm:flex-row gap-3">
          <div className="flex-1 flex gap-2">
            <div className="flex-1">
              <Input
                placeholder="Workflow ID (e.g. foster-CH-2024-006)"
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
              />
            </div>
            <Button onClick={handleSearch} loading={isLoading && !isReplaying} variant="secondary" size="sm">
              <Search size={14} />
              <span className="hidden sm:inline">Search</span>
            </Button>
            <Button variant="ghost" size="sm" onClick={() => refetch()} disabled={isReplaying || !workflowId}>
              <RefreshCw size={14} className={isLoading ? 'animate-spin' : ''} />
            </Button>
          </div>
          <div className="flex items-center gap-2">
            {isReplaying && (
              <motion.div
                initial={{ opacity: 0, x: -10 }}
                animate={{ opacity: 1, x: 0 }}
                className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-surface-alt border border-border text-xs text-muted-foreground"
              >
                <Timer size={12} />
                <span className="font-mono">
                  {String(Math.floor(elapsed / 60)).padStart(2, '0')}:{String(elapsed % 60).padStart(2, '0')}
                </span>
              </motion.div>
            )}
            <ReplayButton
              isReplaying={isReplaying}
              hasReplayed={hasReplayed}
              onToggle={handleReplay}
              disabled={!workflowId && !hasReplayed}
            />
          </div>
        </div>
        {workflowId && !isReplaying && (
          <div className="flex items-center gap-2 mt-2">
            <span className="text-[10px] text-muted-foreground font-mono">
              Workflow: <span className="text-foreground">{workflowId}</span>
            </span>
            {workflow?.child_id && (
              <>
                <span className="text-muted">|</span>
                <span className="text-[10px] text-muted-foreground font-mono">
                  Child: <span className="text-foreground">{workflow.child_id}</span>
                </span>
              </>
            )}
            {workflow?.created_at && (
              <>
                <span className="text-muted">|</span>
                <span className="text-[10px] text-muted-foreground font-mono">
                  Created: <span className="text-foreground">{formatDate(workflow.created_at)}</span>
                </span>
              </>
            )}
          </div>
        )}
      </GlassCard>

      {/* Main content */}
      {showContent ? (
        <DataLoader
          isLoading={isLoading && !isReplaying && liveEvents.length === 0}
          error={!isReplaying ? error : null}
          refetch={refetch}
        >
          {timelineEvents.length > 0 || isReplaying || (workflow && !isLoading) ? (
            <div className="space-y-5">
              <ExecutionMetricsBar metrics={metrics} loading={false} />

              <div className="grid grid-cols-1 lg:grid-cols-[1.5fr_1fr] gap-5 min-h-0">
                {/* Timeline list */}
                <GlassCard className="p-0 overflow-hidden">
                  <div className="flex items-center justify-between px-5 py-3 border-b border-glass-border">
                    <div className="flex items-center gap-2">
                      <ListChecks size={14} className="text-primary" />
                      <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                        Workflow Stages
                      </span>
                    </div>
                    <div className="flex items-center gap-3 text-[10px] text-muted-foreground">
                      <span className="flex items-center gap-1">
                        <span className="w-1.5 h-1.5 rounded-full bg-success" />
                        {metrics.completedStages} done
                      </span>
                      <span className="flex items-center gap-1">
                        <span className="w-1.5 h-1.5 rounded-full bg-info" />
                        {metrics.activeAgents} active
                      </span>
                    </div>
                  </div>
                  <div className="p-5 max-h-[600px] overflow-y-auto">
                    <TimelineList
                      events={timelineEvents}
                      selectedId={selectedStageId}
                      onSelect={handleStageClick}
                      isReplay={isReplaying}
                    />
                  </div>
                </GlassCard>

                {/* Right panel */}
                <div className="flex flex-col gap-4">
                  <div className="flex border-b border-glass-border">
                    <button
                      onClick={() => setActiveTab('activity')}
                      className={cn(
                        'flex items-center gap-2 px-4 py-2.5 text-xs font-medium transition-colors border-b-2 -mb-[1px]',
                        activeTab === 'activity'
                          ? 'text-primary border-primary bg-primary/5'
                          : 'text-muted-foreground border-transparent hover:text-foreground',
                      )}
                    >
                      <Bot size={14} />
                      Agent Activity
                    </button>
                    <button
                      onClick={() => setActiveTab('reasoning')}
                      className={cn(
                        'flex items-center gap-2 px-4 py-2.5 text-xs font-medium transition-colors border-b-2 -mb-[1px]',
                        activeTab === 'reasoning'
                          ? 'text-primary border-primary bg-primary/5'
                          : 'text-muted-foreground border-transparent hover:text-foreground',
                      )}
                    >
                      <BrainCircuit size={14} />
                      AI Thoughts
                      {reasoningEntries.length > 0 && (
                        <span className="ml-1 px-1 py-0.5 rounded bg-primary/20 text-[10px] font-mono text-primary">
                          {reasoningEntries.length}
                        </span>
                      )}
                    </button>
                  </div>

                  <GlassCard className="flex-1 p-4 overflow-y-auto max-h-[520px]">
                    {activeTab === 'activity' ? (
                      <AgentActivityPanel event={selectedStage} />
                    ) : (
                      <AIReasoningFeed entries={reasoningEntries} />
                    )}
                  </GlassCard>

                  {!isReplaying && workflow && (
                    <div className="grid grid-cols-3 gap-3">
                      <div className="p-3 rounded-lg border border-glass-border bg-glass">
                        <p className="text-[10px] text-muted-foreground">Match Score</p>
                        <p className="text-sm font-bold font-mono text-primary">{formatPercent(workflow.match_score ?? null)}</p>
                      </div>
                      <div className="p-3 rounded-lg border border-glass-border bg-glass">
                        <p className="text-[10px] text-muted-foreground">Confidence</p>
                        <p className="text-sm font-bold font-mono text-success">{formatPercent(workflow.confidence_score ?? null)}</p>
                      </div>
                      <div className="p-3 rounded-lg border border-glass-border bg-glass">
                        <p className="text-[10px] text-muted-foreground">Risk</p>
                        <p className="text-sm font-bold font-mono text-warning">{formatRiskScore(workflow.risk_score ?? null)}</p>
                      </div>
                    </div>
                  )}
                </div>
              </div>

              {/* Progress bar */}
              <div className="relative h-1.5 rounded-full bg-surface-alt overflow-hidden">
                <motion.div
                  initial={{ width: 0 }}
                  animate={{ width: `${progressPct}%` }}
                  transition={{ duration: 0.8, ease: 'easeOut' }}
                  className={cn(
                    'absolute inset-y-0 left-0 rounded-full',
                    progressPct === 100 ? 'bg-success' : 'bg-gradient-to-r from-primary to-secondary',
                  )}
                />
              </div>

              {/* Top matches */}
              {!isReplaying && workflow?.top_matches && Array.isArray(workflow.top_matches) && workflow.top_matches.length > 0 && (
                <GlassCard className="p-4">
                  <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Top Matches</p>
                  <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                    {workflow.top_matches.map((match, i) => {
                      const familyObj  = (match as any).family ?? match
                      const familyName = typeof familyObj === 'object'
                        ? (familyObj as any).name ?? (familyObj as any).family_name ?? `Family ${(familyObj as any).family_id ?? ''}`
                        : String(familyObj)
                      const score = (match as any).blended_score ?? (match as any).match_score ?? 0
                      return (
                        <div key={i} className="p-3 rounded-lg border border-glass-border bg-glass">
                          <p className="text-sm font-medium text-foreground">
                            <span className="text-muted-foreground mr-1">#{i + 1}</span>
                            {familyName}
                          </p>
                          <div className="mt-1.5 h-1.5 rounded-full bg-surface-alt overflow-hidden">
                            <div className="h-full rounded-full bg-primary transition-all" style={{ width: `${Math.min(score, 100)}%` }} />
                          </div>
                          <p className="text-xs text-muted-foreground mt-1 font-mono">{formatPercent(score)} match</p>
                        </div>
                      )
                    })}
                  </div>
                </GlassCard>
              )}
            </div>
          ) : (
            <GlassCard className="p-8 text-center">
              <div className="flex flex-col items-center">
                <Search size={32} className="text-muted mb-3" />
                <p className="text-sm text-muted-foreground">No workflow data found for &quot;{workflowId}&quot;</p>
                <Button variant="secondary" size="sm" className="mt-4" onClick={() => refetch()}>
                  <RefreshCw size={14} />
                  Retry
                </Button>
              </div>
            </GlassCard>
          )}
        </DataLoader>
      ) : (
        <GlassCard className="p-12 text-center">
          <div className="flex flex-col items-center max-w-md mx-auto">
            <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-primary/20 to-accent/20 border border-primary/30 flex items-center justify-center mb-5">
              <ListChecks size={28} className="text-primary" />
            </div>
            <h2 className="text-lg font-semibold text-foreground mb-2">Workflow Timeline</h2>
            <p className="text-sm text-muted-foreground leading-relaxed">
              Enter a Workflow ID above to visualize the complete AI orchestration pipeline.
              Watch agents collaborate in real-time from referral intake to placement approval.
            </p>
            <div className="flex items-center gap-6 mt-6 text-xs text-muted-foreground">
              <span className="flex items-center gap-1.5"><Bot size={12} />7 AI Agents</span>
              <span className="flex items-center gap-1.5"><ListChecks size={12} />10 Stages</span>
              <span className="flex items-center gap-1.5"><Clock size={12} />Live Updates</span>
            </div>
            <Button variant="outline" size="sm" className="mt-6" onClick={() => {
              setSearchInput('foster-3001')
              setWorkflowId(normalizeWorkflowId('foster-3001'))
              navigate(`/workflow/${normalizeWorkflowId('foster-3001')}`, { replace: true })
            }}>
              <Search size={14} />
              Try Example: foster-3001
            </Button>
          </div>
        </GlassCard>
      )}

      <TimelineDetailDrawer event={drawerStage} onClose={() => setDrawerStageId(null)} />
    </motion.div>
  )
}
