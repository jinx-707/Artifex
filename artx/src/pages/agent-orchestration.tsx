import { useState, useCallback, useRef, useEffect } from 'react'
import { motion } from 'framer-motion'
import { cn } from '@/lib/utils'
import { GlassCard } from '@/components/ui/glass-card'
import AgentNetworkGraph from '@/components/orchestration/AgentNetworkGraph'
import ExecutionTimeline from '@/components/orchestration/ExecutionTimeline'
import LiveEventPanel from '@/components/orchestration/LiveEventPanel'
import DemoModeButton from '@/components/orchestration/DemoModeButton'
import { useAgents } from '@/hooks/use-foster'
import { useQuery } from '@tanstack/react-query'
import api from '@/services/api'
import { subscribeWorkflowStream } from '@/services/foster'
import {
  type AgentType,
  type AgentStatus,
  type ExecutionStep,
  type AgentMessage,
} from '@/types/orchestration'
import { Radio, Layers, MessageSquare, Bot, Activity, Clock } from 'lucide-react'

type ActiveTab = 'events' | 'steps'

const AGENT_IDS: AgentType[] = ['intake', 'planner', 'risk', 'matching', 'fairness', 'approval', 'monitoring']

const TIMELINE_STEPS: ExecutionStep[] = [
  { id: 'intake',     label: 'Intake',          status: 'pending', agentId: 'intake',     agentName: '' },
  { id: 'plan',       label: 'Planning',         status: 'pending', agentId: 'planner',    agentName: '' },
  { id: 'risk',       label: 'Risk Assessment',  status: 'pending', agentId: 'risk',       agentName: '' },
  { id: 'matching',   label: 'Family Matching',  status: 'pending', agentId: 'matching',   agentName: '' },
  { id: 'fairness',   label: 'Fairness Check',   status: 'pending', agentId: 'fairness',   agentName: '' },
  { id: 'approval',   label: 'Approval',         status: 'pending', agentId: 'approval',   agentName: '' },
  { id: 'monitoring', label: 'Monitoring',        status: 'pending', agentId: 'monitoring', agentName: '' },
]

const AGENT_STAGE_MAP: Record<string, AgentType> = {
  referral_submitted:       'intake',
  eligibility_validated:    'intake',
  child_profile_created:    'planner',
  risk_assessment:          'risk',
  family_matching:          'matching',
  fairness_validation:      'fairness',
  recommendation_generated: 'approval',
  supervisor_approval:      'approval',
  awaiting_approval:        'approval',
  placement_approved:       'monitoring',
  placement_created:        'monitoring',
  monitoring_active:        'monitoring',
}

function buildAgentStates(
  apiAgents: Record<string, { id: string; name: string; type: string; status: string; last_heartbeat_age_s: number | null }> | undefined,
): Record<AgentType, { id: string; status: AgentStatus; confidence: number }> {
  const states: Record<string, { id: string; status: AgentStatus; confidence: number }> = {}
  for (const id of AGENT_IDS) {
    const agent = apiAgents?.[id]
    let visualStatus: AgentStatus = 'idle'
    let confidence = 0
    if (agent) {
      if (agent.status === 'active') {
        visualStatus = 'active'
        confidence = Math.max(20, Math.min(98, Math.round(100 - (agent.last_heartbeat_age_s ?? 90) * 0.5)))
      } else if (agent.status === 'idle' || agent.status === 'stale') {
        visualStatus = 'idle'
        confidence = Math.max(5, Math.min(50, Math.round(50 - (agent.last_heartbeat_age_s ?? 180) * 0.15)))
      }
    }
    states[id] = { id, status: visualStatus, confidence }
  }
  return states as Record<AgentType, { id: string; status: AgentStatus; confidence: number }>
}

function generateId(): string {
  return `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

export default function AgentOrchestrationPage() {
  const { data: agentsData, isLoading } = useAgents()

  const { data: latestPlacement } = useQuery({
    queryKey: ['latest-active-workflow'],
    queryFn: async () => {
      const res = await api.get<{ placements: Array<{ workflow_id: string; status: string }> }>('/foster/placements')
      const active = (res.data.placements || []).find(
        (p) => !['approved', 'rejected', 'closed'].includes(p.status)
      )
      return active?.workflow_id || null
    },
    refetchInterval: 15000,
    staleTime: 10000,
  })

  const wsRef = useRef<ReturnType<typeof subscribeWorkflowStream> | null>(null)
  const timersRef = useRef<ReturnType<typeof setTimeout>[]>([])
  const startTimeRef = useRef<number>(0)
  const elapsedTimerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const [isRunning, setIsRunning] = useState(false)
  const [agentStates, setAgentStates] = useState<Record<AgentType, { id: string; status: AgentStatus; confidence: number }>>(
    {} as Record<AgentType, { id: string; status: AgentStatus; confidence: number }>
  )
  const [steps, setSteps] = useState<ExecutionStep[]>(TIMELINE_STEPS)
  const [messages, setMessages] = useState<AgentMessage[]>([])
  const [activeEdges, setActiveEdges] = useState<string[]>([])
  const [completedEdges, setCompletedEdges] = useState<string[]>([])
  const [showParticles, setShowParticles] = useState(false)
  const [elapsed, setElapsed] = useState(0)
  const [activeTab, setActiveTab] = useState<ActiveTab>('events')

  // ── Sync real agent statuses ──────────────────────────────────────────────
  useEffect(() => {
    if (agentsData?.agents && !isRunning) {
      setAgentStates(buildAgentStates(agentsData.agents))
      const updatedSteps = TIMELINE_STEPS.map((s) => {
        const agent = agentsData.agents[s.agentId]
        const stepStatus = !agent
          ? 'pending' as const
          : agent.status === 'active'
            ? 'active' as const
            : agent.status === 'idle' || agent.status === 'stale'
              ? 'completed' as const
              : 'pending' as const
        return { ...s, status: stepStatus, agentName: agent ? agent.name : '' }
      })
      setSteps(updatedSteps)
    }
  }, [agentsData, isRunning])

  // ── Seed live events from real agent statuses on initial load ─────────────
  useEffect(() => {
    if (isRunning || !agentsData?.agents) return
    const seedMessages: AgentMessage[] = Object.values(agentsData.agents)
      .filter((a: any) => a.status === 'active')
      .map((a: any) => ({
        id: generateId(),
        timestamp: new Date(),
        from: a.name || a.id,
        content: `Agent online — last heartbeat ${a.last_heartbeat_age_s ?? 0}s ago`,
        type: 'info' as const,
      }))
    if (seedMessages.length > 0) {
      setMessages((prev) => prev.length === 0 ? seedMessages : prev)
    }
  }, [agentsData, isRunning])

  // ── Live WebSocket subscription for the most recent active workflow ───────
  useEffect(() => {
    if (!latestPlacement || isRunning) return
    if (wsRef.current) { wsRef.current.close(); wsRef.current = null }

    wsRef.current = subscribeWorkflowStream(
      latestPlacement,
      (msg) => {
        if (msg.type === 'ping') return
        const stage = (msg.stage || '').toLowerCase().replace(/\s+/g, '_')
        const status = msg.status || ''
        const agentId = AGENT_STAGE_MAP[stage]

        if (agentId) {
          const isComplete = status === 'completed' || status === 'approved'
          setAgentStates((prev) => ({
            ...prev,
            [agentId]: {
              ...prev[agentId],
              status: isComplete ? 'completed' as AgentStatus : 'active' as AgentStatus,
              confidence: Math.min(98, (prev[agentId]?.confidence ?? 50) + 15),
            },
          }))
          setSteps((prev) => prev.map((s) =>
            s.agentId === agentId
              ? { ...s, status: isComplete ? 'completed' as const : 'active' as const }
              : s
          ))
        }

        const payload = msg.payload as Record<string, unknown> | undefined
        const content =
          (payload?.output as string) ||
          (payload?.details as string) ||
          (payload?.message as string) ||
          `${stage}: ${status}`
        setMessages((prev) => [
          ...prev.slice(-49),
          {
            id: generateId(),
            timestamp: new Date(),
            from: (payload?.agent as string) || agentId || stage,
            content,
            type:
              status === 'completed' || status === 'approved' ? 'success' as const
              : status === 'failed' ? 'error' as const
              : 'info' as const,
          },
        ])
      },
    )
    return () => {
      wsRef.current?.close()
      wsRef.current = null
    }
  }, [latestPlacement, isRunning])

  // ── Timer helpers ─────────────────────────────────────────────────────────
  const clearAllTimers = useCallback(() => {
    timersRef.current.forEach(clearTimeout)
    timersRef.current = []
    if (elapsedTimerRef.current) {
      clearInterval(elapsedTimerRef.current)
      elapsedTimerRef.current = null
    }
  }, [])

  const resetAll = useCallback(() => {
    clearAllTimers()
    setAgentStates(
      agentsData?.agents
        ? buildAgentStates(agentsData.agents)
        : {} as Record<AgentType, { id: string; status: AgentStatus; confidence: number }>
    )
    setSteps(
      agentsData?.agents
        ? TIMELINE_STEPS.map((s) => {
            const agent = agentsData.agents[s.agentId]
            return {
              ...s,
              status: agent?.status === 'active' ? 'active' as const
                : agent?.status === 'idle' ? 'completed' as const
                : 'pending' as const,
              agentName: agent?.name ?? '',
            }
          })
        : TIMELINE_STEPS
    )
    setMessages([])
    setActiveEdges([])
    setCompletedEdges([])
    setShowParticles(false)
    setElapsed(0)
    setIsRunning(false)
  }, [clearAllTimers, agentsData])

  const stopDemo = useCallback(() => {
    clearAllTimers()
    setIsRunning(false)
    setShowParticles(false)
  }, [clearAllTimers])

  const startDemo = useCallback(() => {
    resetAll()
    setIsRunning(true)
    startTimeRef.current = Date.now()
    elapsedTimerRef.current = setInterval(() => {
      setElapsed(Math.floor((Date.now() - startTimeRef.current) / 1000))
    }, 1000)

    type DemoEvent = {
      id: string; label: string; delay: number
      activate: AgentType[]; complete: AgentType[]
      edges: { active: string[]; complete: string[] }
      message?: Omit<AgentMessage, 'id' | 'timestamp'>
    }

    const DEMO_EVENTS: DemoEvent[] = [
      { id: 'intake',        delay: 2000, label: 'Intake',         activate: ['intake'],                    complete: [],            edges: { active: [], complete: [] },                                                    message: { from: 'Intake Agent',     content: 'Referral received — processing child intake',              type: 'info' } },
      { id: 'intake-done',   delay: 4000, label: 'Profile Loaded', activate: [],                            complete: ['intake'],    edges: { active: ['e-intake-planner'], complete: [] },                                  message: { from: 'Intake Agent',     content: 'Child profile loaded — Age: 8, Priority: High',            type: 'success' } },
      { id: 'plan',          delay: 3000, label: 'Planning',       activate: ['planner'],                   complete: [],            edges: { active: [], complete: [] },                                                    message: { from: 'Planner Agent',    content: 'Created execution plan — spawning parallel agents',        type: 'info' } },
      { id: 'plan-done',     delay: 4000, label: 'Plan Finalized', activate: [],                            complete: ['planner'],   edges: { active: ['e-planner-risk','e-planner-matching','e-planner-fairness'], complete: ['e-intake-planner'] }, message: { from: 'Planner Agent', content: 'Risk, Matching, and Fairness agents activated', type: 'success' } },
      { id: 'risk',          delay: 3000, label: 'Risk Scoring',   activate: ['risk','matching','fairness'], complete: [],           edges: { active: [], complete: [] },                                                    message: { from: 'Risk Agent',       content: 'Risk assessment initialized — analyzing case history',      type: 'info' } },
      { id: 'risk-done',     delay: 5000, label: 'Risk: 45',       activate: [],                            complete: ['risk'],      edges: { active: [], complete: ['e-planner-risk'] },                                    message: { from: 'Risk Agent',       content: 'Risk Score = 45 (Low Risk) — No safety concerns',          type: 'success' } },
      { id: 'match-done',    delay: 3000, label: 'Match Found',    activate: [],                            complete: ['matching'],  edges: { active: [], complete: ['e-planner-matching'] },                               message: { from: 'Matching Agent',   content: 'Found 3 candidate families — Johnson Family ranked #1',    type: 'success' } },
      { id: 'fair-done',     delay: 3000, label: 'Fairness: Pass', activate: [],                            complete: ['fairness'],  edges: { active: [], complete: ['e-planner-fairness'] },                               message: { from: 'Fairness Agent',   content: 'Bias score: 0.03 (Acceptable) — No demographic skew',      type: 'success' } },
      { id: 'approval',      delay: 2000, label: 'Approval',       activate: ['approval'],                  complete: [],            edges: { active: ['e-risk-approval','e-matching-approval','e-fairness-approval'], complete: [] }, message: { from: 'Approval Agent', content: 'Recommendation generated — Johnson Family (Match: 30%)', type: 'info' } },
      { id: 'approval-done', delay: 5000, label: 'Approved',       activate: [],                            complete: ['approval'],  edges: { active: [], complete: ['e-risk-approval','e-matching-approval','e-fairness-approval'] }, message: { from: 'Approval Agent', content: 'Supervisor approval granted — All criteria validated', type: 'success' } },
      { id: 'monitor',       delay: 3000, label: 'Monitoring',     activate: ['monitoring'],                complete: [],            edges: { active: ['e-approval-monitoring'], complete: [] },                             message: { from: 'Monitoring Agent', content: 'Placement record created — Johnson Family assigned',        type: 'success' } },
      { id: 'monitor-done',  delay: 4000, label: 'Active',         activate: [],                            complete: ['monitoring'], edges: { active: [], complete: ['e-approval-monitoring'] },                           message: { from: 'Monitoring Agent', content: '21-day adjustment tracking initiated',                       type: 'success' } },
    ]

    let cumulativeDelay = 1000
    DEMO_EVENTS.forEach((evt) => {
      cumulativeDelay += evt.delay
      const timer = setTimeout(() => {
        setAgentStates((prev) => {
          const next = { ...prev }
          evt.activate.forEach((id) => { next[id] = { ...next[id], status: 'active', confidence: Math.min(98, (next[id]?.confidence ?? 0) + 40) } })
          evt.complete.forEach((id) => { next[id] = { ...next[id], status: 'completed', confidence: Math.min(99, (next[id]?.confidence ?? 0) + 30) } })
          return next
        })
        setActiveEdges((prev) => [...prev.filter((e) => !evt.edges.complete.includes(e)), ...evt.edges.active])
        setCompletedEdges((prev) => [...prev, ...evt.edges.complete])
        if (evt.edges.active.length > 0) setShowParticles(true)
        if (evt.message) setMessages((prev) => [...prev, { id: generateId(), timestamp: new Date(), ...evt.message! }])
        setSteps((prev) => prev.map((s) => {
          if (evt.activate.includes(s.agentId as AgentType) && s.status === 'pending')
            return { ...s, status: 'active' as const, agentName: s.agentId.charAt(0).toUpperCase() + s.agentId.slice(1) + ' Agent' }
          if (evt.complete.includes(s.agentId as AgentType))
            return { ...s, status: 'completed' as const }
          return s
        }))
      }, cumulativeDelay)
      timersRef.current.push(timer)
    })

    const stopTimer = setTimeout(() => {
      setShowParticles(false)
      setIsRunning(false)
      if (elapsedTimerRef.current) { clearInterval(elapsedTimerRef.current); elapsedTimerRef.current = null }
    }, cumulativeDelay + 2000)
    timersRef.current.push(stopTimer)
  }, [resetAll])

  const handleToggleDemo = useCallback(() => {
    if (isRunning) stopDemo()
    else startDemo()
  }, [isRunning, stopDemo, startDemo])

  useEffect(() => {
    return () => { clearAllTimers(); wsRef.current?.close() }
  }, [clearAllTimers])

  const isAnyActive = Object.values(agentStates).some((a) => a.status === 'active')
  const healthyCount = Object.values(agentStates).filter((a) => a.status !== 'idle').length
  const totalAgents = AGENT_IDS.length

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      className="h-[calc(100vh-4rem)] flex flex-col gap-3 p-2"
    >
      {/* Header */}
      <div className="flex items-center justify-between shrink-0 px-2">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-bold text-foreground">Agent Orchestration</h1>
          <div className={cn(
            'flex items-center gap-1.5 px-2.5 py-1 rounded-full border',
            isRunning ? 'bg-info/10 border-info/20' : agentsData?.agents ? 'bg-success/10 border-success/20' : 'bg-muted/10 border-muted/20'
          )}>
            <Radio size={12} className={isRunning ? 'text-info' : 'text-success'} />
            <span className="text-[10px] font-mono font-semibold uppercase tracking-wider">
              {isRunning ? 'Running' : agentsData?.agents ? `${healthyCount}/${totalAgents} Active` : 'Standby'}
            </span>
          </div>
          {isRunning && (
            <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-surface-alt border border-border">
              <Clock size={11} className="text-muted-foreground" />
              <span className="text-[10px] font-mono text-muted-foreground">
                {String(Math.floor(elapsed / 60)).padStart(2, '0')}:{String(elapsed % 60).padStart(2, '0')}
              </span>
            </div>
          )}
        </div>
        <div className="flex items-center gap-3">
          {isRunning && (
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <span className="flex items-center gap-1.5">
                <span className="w-2 h-2 rounded-full bg-success shadow-[0_0_6px_rgba(16,185,129,0.6)]" />
                Completed: {steps.filter((s) => s.status === 'completed').length}
              </span>
              <span className="text-muted">|</span>
              <span className="flex items-center gap-1.5">
                <span className="w-2 h-2 rounded-full bg-info shadow-[0_0_6px_rgba(59,130,246,0.6)]" />
                Active: {steps.filter((s) => s.status === 'active').length}
              </span>
            </div>
          )}
          <DemoModeButton isRunning={isRunning} onToggle={handleToggleDemo} />
        </div>
      </div>

      {/* Main: Graph + Sidebar */}
      <div className="flex-1 flex gap-3 min-h-0">
        <GlassCard className="flex-[4] min-w-0 overflow-hidden p-2 rounded-2xl">
          <div className="h-full w-full">
            <AgentNetworkGraph
              agentStates={agentStates}
              activeEdges={activeEdges}
              completedEdges={completedEdges}
              showParticles={showParticles && isAnyActive}
            />
          </div>
        </GlassCard>

        <GlassCard className="flex-[1] min-w-[280px] flex flex-col p-0 overflow-hidden rounded-2xl">
          <div className="flex items-center border-b border-glass-border shrink-0">
            <button
              onClick={() => setActiveTab('events')}
              className={cn(
                'flex items-center gap-2 px-4 py-3 text-xs font-medium transition-colors border-b-2 flex-1 justify-center',
                activeTab === 'events'
                  ? 'text-primary border-primary bg-primary/5'
                  : 'text-muted-foreground border-transparent hover:text-foreground'
              )}
            >
              <MessageSquare size={14} />
              Events
              {messages.length > 0 && (
                <span className="ml-1 px-1.5 py-0.5 rounded-full bg-primary/20 text-primary text-[10px] font-mono">
                  {messages.length}
                </span>
              )}
            </button>
            <button
              onClick={() => setActiveTab('steps')}
              className={cn(
                'flex items-center gap-2 px-4 py-3 text-xs font-medium transition-colors border-b-2 flex-1 justify-center',
                activeTab === 'steps'
                  ? 'text-primary border-primary bg-primary/5'
                  : 'text-muted-foreground border-transparent hover:text-foreground'
              )}
            >
              <Layers size={14} />
              Steps
              {steps.filter((s) => s.status !== 'pending').length > 0 && (
                <span className="ml-1 px-1.5 py-0.5 rounded-full bg-success/20 text-success text-[10px] font-mono">
                  {steps.filter((s) => s.status === 'completed').length}/{steps.length}
                </span>
              )}
            </button>
          </div>
          <div className="flex-1 overflow-y-auto min-h-0 p-2">
            {activeTab === 'events' ? (
              <LiveEventPanel messages={messages} />
            ) : (
              <ExecutionTimeline steps={steps} />
            )}
          </div>

          <div className="border-t border-glass-border p-3 space-y-2 shrink-0">
            <p className="text-[10px] uppercase tracking-widest text-muted-foreground font-medium">Agent Status</p>
            <div className="grid grid-cols-2 gap-1.5">
              {AGENT_IDS.map((id) => {
                const state = agentStates[id]
                const isActive = state?.status === 'active'
                const isCompleted = state?.status === 'completed'
                return (
                  <div key={id} className="flex items-center gap-1.5 text-xs">
                    <div className={cn(
                      'w-2 h-2 rounded-full shrink-0',
                      isActive ? 'bg-info shadow-[0_0_6px_rgba(59,130,246,0.6)]' :
                      isCompleted ? 'bg-success shadow-[0_0_6px_rgba(16,185,129,0.6)]' :
                      'bg-muted'
                    )} />
                    <span className="text-muted-foreground truncate capitalize">{id}</span>
                  </div>
                )
              })}
            </div>
          </div>
        </GlassCard>
      </div>

      {/* Timeline */}
      <GlassCard className="shrink-0 p-0 overflow-hidden rounded-2xl">
        <div className="border-b border-glass-border px-4 py-2.5 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Activity size={14} className="text-muted-foreground" />
            <h2 className="text-xs font-semibold text-foreground uppercase tracking-wider">Workflow Timeline</h2>
          </div>
          <span className="text-[10px] text-muted-foreground font-mono">
            {steps.filter((s) => s.status === 'completed').length}/{steps.length} steps
          </span>
        </div>
        <div className="px-3 py-2">
          <ExecutionTimeline steps={steps} horizontal />
        </div>
      </GlassCard>

      {/* Metrics bar */}
      <div className="grid grid-cols-3 gap-3 shrink-0">
        <GlassCard className="p-2.5 rounded-xl">
          <div className="flex items-center gap-2">
            <Bot size={14} className="text-muted-foreground" />
            <span className="text-[11px] text-muted-foreground">Registered Agents</span>
          </div>
          <p className="text-lg font-bold text-foreground font-mono mt-1">
            {isLoading ? '…' : totalAgents}
          </p>
        </GlassCard>
        <GlassCard className="p-2.5 rounded-xl">
          <div className="flex items-center gap-2">
            <Activity size={14} className="text-muted-foreground" />
            <span className="text-[11px] text-muted-foreground">Active / Healthy</span>
          </div>
          <p className="text-lg font-bold text-foreground font-mono mt-1">
            {isLoading ? '…' : `${healthyCount}/${totalAgents}`}
          </p>
        </GlassCard>
        <GlassCard className="p-2.5 rounded-xl">
          <div className="flex items-center gap-2">
            <Clock size={14} className="text-muted-foreground" />
            <span className="text-[11px] text-muted-foreground">Live WS</span>
          </div>
          <p className="text-lg font-bold text-foreground font-mono mt-1">
            {latestPlacement ? 'On' : 'Off'}
          </p>
        </GlassCard>
      </div>
    </motion.div>
  )
}
