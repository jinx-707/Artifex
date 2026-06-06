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

function apiEventsToTimeline(events: WorkflowStage[]): TimelineEvent[] {
  const STATUS_MAP: Record<string, StageStatus> = {
    completed: 'completed', in_progress: 'in_progress', failed: 'failed',
    pending: 'pending', active: 'in_progress', running: 'in_progress',
  }
  const STATUS_RANK: Record<string, number> = {
    completed: 4, in_progress: 3, failed: 2, pending: 1,
  }

  const seen = new Map<string, TimelineEvent>()

  const isNewStyleStage = (s: string) => s.length > 0 && !/^\s*$/.test(s)

  for (const e of events || []) {
    const stageName = e.stage || e.name || ''
    if (!stageName) continue
    // Skip legacy Title Case stages (e.g. "Intake", "Eligibility Validation")
    if (!isNewStyleStage(stageName)) continue
    const status = STATUS_MAP[e.status] || 'pending'

    // Parse e.data (which may be a raw JSON string instead of object)
    let parsedData: Record<string, unknown> = {}
    if (typeof e.data === 'object' && e.data !== null) {
      parsedData = e.data as Record<string, unknown>
    } else if (typeof e.data === 'string' && (e.data as string).trim().startsWith('{')) {
      try { parsedData = JSON.parse(e.data) } catch { parsedData = {} }
    }
    // Parse e.details (may also be a JSON string or nested message)
    let detailsStr = ''
    if (typeof e.details === 'string' && e.details.trim().startsWith('{')) {
      try {
        const detailsParsed = JSON.parse(e.details) as Record<string, unknown>
        parsedData = { ...parsedData, ...detailsParsed }
      } catch { detailsStr = e.details }
    } else { detailsStr = e.details || '' }

    const agentName = parsedData.agent as string || ''
    const agentAction = parsedData.action as string || ''
    const agentOutput = parsedData.output as string || ''
    const latency = typeof parsedData.latency === 'number' ? parsedData.latency : 0
    const confidence = Number(parsedData.confidence ?? 0)
    const confidenceScoreVal = (parsedData.confidence_score
      ?? (confidence <= 1
        ? Math.round(confidence * 100)
        : Math.round(confidence))) as number
    const reasoning = Array.isArray(parsedData.reasoning) ? parsedData.reasoning : []
    const inputData = (parsedData.inputData as string) || (parsedData.input as string) || ''
    const outputData = (parsedData.outputData as string) || ''
    const decisionExplanation = (parsedData.decisionExplanation as string) || ''
    const logs = Array.isArray(parsedData.logs) ? parsedData.logs : []
    if (!detailsStr) {
      detailsStr = (parsedData.message as string) || (parsedData.details as string) || ''
    }
    const stagePayload: Record<string, unknown> | undefined =
      Object.keys(parsedData).length > 0 ? parsedData : undefined

    const existing = seen.get(stageName)
    const rank = STATUS_RANK[status] ?? 0
    const existingRank = existing ? (STATUS_RANK[existing.status] ?? 0) : -1

    // Always update if new status is higher rank (e.g., completed > pending)
    if (!existing || rank > existingRank) {
      const raw = e.label || e.stage?.replace(/_/g, ' ') || e.name?.replace(/_/g, ' ') || stageName
      const label = raw.charAt(0).toUpperCase() + raw.slice(1)
      seen.set(stageName, {
        id: `api-${stageName}`,
        stage: stageName, label, status,
        agentName, agentAction, agentOutput,
        latency, confidenceScore: confidenceScoreVal,
        reasoning,
        inputData, outputData, decisionExplanation, logs,
        timestamp: e.timestamp || e.started_at,
        startedAt: e.started_at, completedAt: e.completed_at,
        details: detailsStr,
        payload: stagePayload,
      })
    } else if (existing && !existing.agentName && agentName) {
      // Populate empty agent fields from a later event that has more data
      seen.set(stageName, {
        ...existing,
        agentName: agentName || existing.agentName,
        agentAction: agentAction || existing.agentAction,
        agentOutput: agentOutput || existing.agentOutput,
        latency: latency || existing.latency,
        confidenceScore: confidenceScoreVal || existing.confidenceScore,
        reasoning: reasoning.length > 0 ? reasoning : existing.reasoning,
      })
    }
  }

  return Array.from(seen.values())
}

function sortTimelineEvents(events: TimelineEvent[]): TimelineEvent[] {
  const RANK: Record<string, number> = { completed: 4, in_progress: 3, failed: 2, pending: 1 }
  return [...events].sort((a, b) => {
    const r = (RANK[b.status] ?? 0) - (RANK[a.status] ?? 0)
    if (r !== 0) return r
    return (a.timestamp ? new Date(a.timestamp).getTime() : 0) - (b.timestamp ? new Date(b.timestamp).getTime() : 0)
  })
}

function computeMetrics(events: TimelineEvent[], elapsed: number): ExecutionMetrics {
  const completed = events.filter((e) => e.status === 'completed').length
  const inProgress = events.filter((e) => e.status === 'in_progress').length
  const total = Math.max(events.length, 1)
  const progress = total > 0 ? (completed / total) * 100 : 0
  return {
    progress,
    completedStages: completed,
    totalStages: total,
    executionTime: elapsed,
    activeAgents: inProgress,
    messagesExchanged: events.reduce((s, e) => s + (e.reasoning?.length || 0) + (e.logs?.length || 0), 0),
    riskScore: 45, matchScore: 30,
    confidenceScore: completed > 0
      ? Math.round(events.filter((e) => e.status === 'completed').reduce((s, e) => s + e.confidenceScore, 0) / completed)
      : 0,
  }
}

function extractWorkflowIdFromPath(): string {
  const m = window.location.pathname.match(/\/workflow\/(.+)/)
  return m?.[1] ? decodeURIComponent(m[1]) : ''
}

const REPLAY_DELAYS = [1, 5, 9.5, 14.5, 20, 25.5, 31, 36.5, 42, 47]
const REPLAY_TOTAL_STEPS = MOCK_TIMELINE_EVENTS.length

export default function WorkflowTrackingPage() {
  const params = useParams<{ workflowId: string }>()
  const navigate = useNavigate()
  const routeWorkflowId = params.workflowId || extractWorkflowIdFromPath()
  const initialNormalized = routeWorkflowId ? normalizeWorkflowId(routeWorkflowId) : ''

  const [workflowId, setWorkflowId] = useState(initialNormalized)
  const [searchInput, setSearchInput] = useState(routeWorkflowId || '')
  const queryClient = useQueryClient()
  const wsSubscriptionRef = useRef<ReturnType<typeof subscribeWorkflowStream> | null>(null)
  const [selectedStageId, setSelectedStageId] = useState<string | null>(null)
  const [drawerStageId, setDrawerStageId] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'activity' | 'reasoning'>('activity')

  const [isReplaying, setIsReplaying] = useState(false)
  const [hasReplayed, setHasReplayed] = useState(false)
  const [replayStep, setReplayStep] = useState(-1)
  const [elapsed, setElapsed] = useState(0)
  const timersRef = useRef<ReturnType<typeof setTimeout>[]>([])
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const startTimeRef = useRef(0)

  // ── Incrementally-built live timeline (separate from REST snapshot) ─────
  // Each incoming WS event appends ONE item so AnimatePresence can detect
  // new insertions and play the entrance animation.
  const [liveEvents, setLiveEvents] = useState<TimelineEvent[]>([])
  const seenEventKeys = useRef<Set<string>>(new Set())
  const liveInitialised = useRef(false)

  const clearAllTimers = useCallback(() => {
    timersRef.current.forEach(clearTimeout)
    timersRef.current = []
    if (intervalRef.current) { clearInterval(intervalRef.current); intervalRef.current = null }
  }, [])

  useEffect(() => {
    const id = params.workflowId || extractWorkflowIdFromPath()
    if (id) {
      const normalized = normalizeWorkflowId(id)
      setWorkflowId(normalized)
      setSearchInput(id)
      if (normalized !== id) navigate(`/workflow/${normalized}`, { replace: true })
    }
  }, [params.workflowId, navigate])

  const { data: workflow, isLoading, error, refetch } = useQuery<WorkflowStatus>({
    queryKey: ['workflow-status', workflowId],
    queryFn: async () => {
      if (!workflowId) throw new Error('No workflow ID provided')
      const res = await api.get<WorkflowStatus>(`/foster/status/${encodeURIComponent(workflowId)}`)
      return {
        ...res.data,
        status: res.data?.status || 'unknown',
        current_stage: res.data?.current_stage || 'Unknown',
        progress: typeof res.data?.progress === 'number' ? res.data.progress : 0,
        timeline: Array.isArray(res.data?.timeline) ? res.data.timeline : [],
      } as WorkflowStatus
    },
    enabled: !!workflowId?.trim(),
    refetchOnWindowFocus: true,
    staleTime: 0,
    refetchInterval: (query) => {
      const data = query.state.data as WorkflowStatus | undefined
      if (!data) return 5000
      const terminal = ['approved', 'rejected', 'closed', 'completed']
      return terminal.includes(data.status) ? false : 8000
    },
    retry: 1,
    retryDelay: (a) => Math.min(1000 * 2 ** a, 4000),
  })

  useEffect(() => {
    if (!workflowId || isReplaying) return
    if (wsSubscriptionRef.current) { wsSubscriptionRef.current.close(); wsSubscriptionRef.current = null }
    // Reset live state for a new workflow
    setLiveEvents([])
    seenEventKeys.current = new Set()
    liveInitialised.current = false

    const sub = subscribeWorkflowStream(workflowId,
      (msg) => {
        // Ignore application-level pings
        if (msg.type === 'ping') return

        // 1. Always update the React Query cache with status metadata
        queryClient.setQueryData(['workflow-status', workflowId], (current) => {
          const existing = (current as WorkflowStatus) || {
            workflow_id: workflowId, status: 'unknown', active: true, progress: 0, timeline: [],
          }
          return {
            ...existing, ...msg,
            status: msg.status || existing.status || 'unknown',
            current_stage: msg.current_stage || existing.current_stage || 'Unknown',
            progress: typeof msg.progress === 'number' ? msg.progress : existing.progress || 0,
            // Don't replace timeline from event messages – we build it incrementally below
            timeline: existing.timeline || [],
          } as WorkflowStatus
        })

        // 2. Build the live timeline incrementally
        if (msg.type === 'workflow_snapshot') {
          // Merge DB snapshot into live events (REST seed may already have data)
          const dbTimeline = Array.isArray(msg.timeline) ? msg.timeline : []
          const converted = sortTimelineEvents(apiEventsToTimeline(dbTimeline))
          setLiveEvents((prev) => {
            // Build a merged map: existing events + snapshot, snapshot wins on rank
            const STATUS_RANK: Record<string, number> = {
              completed: 4, in_progress: 3, failed: 2, pending: 1,
            }
            const map = new Map(prev.map((e) => [e.stage, e]))
            for (const ev of converted) {
              const existing = map.get(ev.stage)
              if (!existing || (STATUS_RANK[ev.status] ?? 0) >= (STATUS_RANK[existing.status] ?? 0)) {
                map.set(ev.stage, ev)
              }
            }
            const merged = sortTimelineEvents(Array.from(map.values()))
            for (const ev of merged) seenEventKeys.current.add(`${ev.stage}:${ev.status}`)
            return merged
          })
          liveInitialised.current = true
          return
        }

        if (msg.type === 'workflow_event') {
          const stageName = (msg.stage || '').toLowerCase().replace(/\s+/g, '_')
          const status = (msg.status || 'in_progress') as StageStatus
          const STATUS_RANK: Record<string, number> = {
            completed: 4, in_progress: 3, failed: 2, pending: 1,
          }
          const key = `${stageName}:${status}`
          // Deduplicate: skip if this exact stage+status combo has been seen
          if (seenEventKeys.current.has(key)) return
          seenEventKeys.current.add(key)

          const raw = (msg.stage || '').replace(/_/g, ' ')
          const label = raw.charAt(0).toUpperCase() + raw.slice(1)

          // Try to parse any string fields in msg.payload that look like JSON
          const safePayload = msg.payload as Record<string, unknown> | undefined
          const parsedPayload: Record<string, unknown> = safePayload ? { ...safePayload } : {}
          if (safePayload) {
            for (const [k, v] of Object.entries(safePayload)) {
              if (typeof v === 'string' && v.trim().startsWith('{')) {
                try { parsedPayload[k] = JSON.parse(v) } catch { /* keep original */ }
              }
            }
          }

          const newEvent: TimelineEvent = {
            id: `live-${stageName}-${status}-${Date.now()}`,
            stage: stageName,
            label,
            status,
            agentName: (parsedPayload.agent as string) || '',
            agentAction: (parsedPayload.action as string) || '',
            agentOutput: (parsedPayload.output as string) || '',
            latency: typeof parsedPayload.latency === 'number' ? parsedPayload.latency : 0,
            confidenceScore: typeof parsedPayload.confidence === 'number'
              ? Math.round(parsedPayload.confidence * 100)
              : typeof parsedPayload.confidence_score === 'number'
                ? Math.round(parsedPayload.confidence_score)
                : 0,
            reasoning: Array.isArray(parsedPayload.reasoning) ? parsedPayload.reasoning : [],
            inputData: (parsedPayload.inputData as string) || (parsedPayload.input as string) || '',
            outputData: (parsedPayload.outputData as string) || '',
            decisionExplanation: (parsedPayload.decisionExplanation as string) || '',
            logs: Array.isArray(parsedPayload.logs) ? parsedPayload.logs : [],
            timestamp: msg.timestamp,
            startedAt: msg.timestamp,
            completedAt: status === 'completed' ? msg.timestamp : undefined,
            details: (parsedPayload.message as string) || (parsedPayload.details as string) || undefined,
            payload: Object.keys(parsedPayload).length > 0 ? parsedPayload : undefined,
          }

          setLiveEvents((prev) => {
            // Upsert: replace existing entry for same stage if new status rank >= old
            const newRank = STATUS_RANK[status] ?? 0
            const idx = prev.findIndex((e) => e.stage === stageName)
            if (idx >= 0) {
              const oldRank = STATUS_RANK[prev[idx].status] ?? 0
              if (newRank >= oldRank) {
                const updated = [...prev]
                updated[idx] = newEvent
                return sortTimelineEvents(updated)
              }
              return prev
            }
            // No existing entry for this stage — append
            const updated = [...prev, newEvent]
            return sortTimelineEvents(updated)
          })
        }
      },
      () => {}, () => {},
    )
    wsSubscriptionRef.current = sub
    return () => { sub.close(); wsSubscriptionRef.current = null }
  }, [workflowId, queryClient, isReplaying])

  // Seed liveEvents from REST data when WS hasn't fired yet
  useEffect(() => {
    if (liveInitialised.current) return // WS snapshot already took over
    if (!workflow?.timeline || (workflow.timeline as WorkflowStage[]).length === 0) return
    const converted = sortTimelineEvents(apiEventsToTimeline(workflow.timeline as WorkflowStage[]))
    if (converted.length === 0) return
    for (const ev of converted) {
      seenEventKeys.current.add(`${ev.stage}:${ev.status}`)
    }
    setLiveEvents(converted)
  }, [workflow?.timeline])

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

  // Build reasoning entries from both WS events and DB timeline
  const reasoningEntries = useMemo<ReasoningEntry[]>(() => {
    if (isReplaying) {
      const ratio = replayStep / REPLAY_TOTAL_STEPS
      return MOCK_REASONING_ENTRIES.slice(0, Math.max(1, Math.floor(ratio * MOCK_REASONING_ENTRIES.length)))
    }
    // Build from live events' reasoning chains
    const entries: ReasoningEntry[] = []
    let idCounter = 0
    for (const ev of timelineEvents) {
      if (ev.reasoning && ev.reasoning.length > 0) {
        for (const step of ev.reasoning) {
          entries.push({
            id: `reasoning-${idCounter++}`,
            timestamp: ev.timestamp || ev.startedAt || new Date().toISOString(),
            agentName: ev.agentName || ev.stage.replace(/_/g, ' '),
            content: step,
          })
        }
      }
    }
    return entries
  }, [isReplaying, replayStep, timelineEvents])

  useEffect(() => {
    if (timelineEvents.length > 0) {
      const active = timelineEvents.find((e) => e.status === 'in_progress')
      if (active) setSelectedStageId(active.id)
      else {
        const last = timelineEvents[timelineEvents.length - 1]
        if (last) setSelectedStageId(last.id)
      }
    }
  }, [timelineEvents])

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
      const timer = setTimeout(() => {
        setReplayStep(step)
        if (step < MOCK_TIMELINE_EVENTS.length) setSelectedStageId(MOCK_TIMELINE_EVENTS[step].id)
        scheduleNext(step + 1)
      }, delayMs)
      timersRef.current.push(timer)
    }
    scheduleNext(0)
  }, [isReplaying, clearAllTimers])

  const handleSearch = useCallback(() => {
    const raw = searchInput.trim()
    if (raw) {
      const normalized = normalizeWorkflowId(raw)
      setWorkflowId(normalized)
      navigate(`/workflow/${normalized}`, { replace: true })
      setIsReplaying(false); setHasReplayed(false); setReplayStep(-1)
    }
  }, [searchInput, navigate])

  const handleStageClick = useCallback((id: string) => {
    setSelectedStageId(id)
    setDrawerStageId(id)
  }, [])

  useEffect(() => () => clearAllTimers(), [clearAllTimers])

  const progressPct = Math.round(metrics.progress)
  const showContent = !!workflowId && (!!workflow || isReplaying || isLoading || !!error)

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

      <GlassCard className="p-4">
        <div className="flex flex-col sm:flex-row gap-3">
          <div className="flex-1 flex gap-2">
            <div className="flex-1">
              <Input
                placeholder="Workflow ID (e.g. foster-3001)"
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
              />
            </div>
            <Button onClick={handleSearch} loading={isLoading && !isReplaying} variant="secondary" size="sm">
              <Search size={14} />
              <span className="hidden sm:inline">Search</span>
            </Button>
            <Button variant="ghost" size="sm" onClick={() => refetch()} disabled={isReplaying}>
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
            <ReplayButton isReplaying={isReplaying} hasReplayed={hasReplayed} onToggle={handleReplay} disabled={false} />
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

      {showContent ? (
        <DataLoader isLoading={isLoading && !isReplaying && !!workflowId} error={!isReplaying ? error : null} refetch={refetch}>
          {timelineEvents.length > 0 || isReplaying || workflow ? (
            <div className="space-y-5">
              <ExecutionMetricsBar metrics={metrics} loading={false} />

              <div className="grid grid-cols-1 lg:grid-cols-[1.5fr_1fr] gap-5 min-h-0">
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
                    <TimelineList events={timelineEvents} selectedId={selectedStageId} onSelect={handleStageClick} isReplay={isReplaying} />
                  </div>
                </GlassCard>

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

              {!isReplaying && workflow?.top_matches && Array.isArray(workflow.top_matches) && workflow.top_matches.length > 0 && (
                <GlassCard className="p-4">
                  <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Top Matches</p>
                  <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                    {workflow.top_matches.map((match, i) => {
                      const familyObj = (match as any).family ?? match
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
              const normalized = normalizeWorkflowId('foster-3001')
              setWorkflowId(normalized)
              navigate(`/workflow/${normalized}`, { replace: true })
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
