/**
 * WorkflowActivityDashboard – replaces the sparse single chart.
 * Section A: Activity trend (area/bar chart)
 * Section B: Stage breakdown table
 * Section C: Live metric cards
 * Section D: Recent workflow events table
 */
import { useMemo } from 'react'
import {
  AreaChart, Area, BarChart, Bar, XAxis, YAxis, Tooltip, Legend,
  ResponsiveContainer, CartesianGrid,
} from 'recharts'
import { useWorkflowActivity, usePlacements, useDashboardEvents } from '@/hooks/use-foster'
import { GlassCard, GlassCardHeader, GlassCardTitle } from '@/components/ui/glass-card'
import { cn } from '@/lib/utils'
import { TrendingUp, Clock, CheckCircle, XCircle, Activity } from 'lucide-react'
import { formatDate } from '@/lib/utils'

const CHART_TOOLTIP_STYLE = {
  background: '#1a1a24',
  border: '1px solid #2a2a3d',
  borderRadius: 8,
  fontSize: 12,
}

const STAGE_LABELS: Record<string, string> = {
  referral_submitted: 'Intake',
  eligibility_validated: 'Eligibility',
  child_profile_created: 'Profile',
  risk_assessment: 'Risk Assessment',
  family_matching: 'Family Matching',
  fairness_validation: 'Fairness Check',
  recommendation_generated: 'Recommendation',
  supervisor_approval: 'Approval',
  awaiting_approval: 'Monitoring',
}

const PIPELINE_STAGES = Object.keys(STAGE_LABELS)

function LiveMetricCard({
  icon: Icon, label, value, sub, color,
}: {
  icon: React.ElementType
  label: string
  value: string | number
  sub?: string
  color: string
}) {
  return (
    <div className="flex items-center gap-3 p-3 rounded-lg border border-glass-border bg-glass">
      <div className={cn('w-9 h-9 rounded-lg flex items-center justify-center shrink-0', color)}>
        <Icon size={16} />
      </div>
      <div className="min-w-0">
        <p className="text-[10px] text-muted-foreground uppercase tracking-wider">{label}</p>
        <p className="text-lg font-bold font-mono text-foreground">{value}</p>
        {sub && <p className="text-[10px] text-muted-foreground">{sub}</p>}
      </div>
    </div>
  )
}

export default function WorkflowActivityDashboard() {
  const { data: activity, isLoading: actLoading } = useWorkflowActivity()
  const { data: placements } = usePlacements()
  const { data: events } = useDashboardEvents()

  // ── Section C: compute live metrics from placements ─────────────────────
  const metrics = useMemo(() => {
    const all = placements ?? []
    const active = all.filter((p) => !['approved', 'rejected', 'closed'].includes(p.status ?? '')).length
    const completedToday = all.filter((p) => {
      if (p.status !== 'approved') return false
      const updated = new Date(p.updated_at ?? '')
      const today = new Date()
      return updated.toDateString() === today.toDateString()
    }).length
    const approved = all.filter((p) => p.status === 'approved').length
    const rejected = all.filter((p) => p.status === 'rejected').length
    const total = all.length
    const approvalRate = total > 0 ? Math.round((approved / total) * 100) : 0
    const failureRate = total > 0 ? Math.round((rejected / total) * 100) : 0

    // Average completion time from workflow events duration
    const withTime = all.filter((p) => p.created_at && p.updated_at && p.status === 'approved')
    const avgCompletionMs = withTime.length
      ? withTime.reduce((s, p) => {
          const start = new Date(p.created_at ?? '').getTime()
          const end = new Date(p.updated_at ?? '').getTime()
          return s + (end - start)
        }, 0) / withTime.length
      : 0
    const avgCompletionMin = avgCompletionMs > 0 ? Math.round(avgCompletionMs / 60000) : 0

    return { active, completedToday, approvalRate, failureRate, avgCompletionMin }
  }, [placements])

  // ── Section B: stage breakdown from placements timeline events ───────────
  const stageBreakdown = useMemo(() => {
    const all = placements ?? []
    // Build stage stats from all placements' status
    return PIPELINE_STAGES.map((stage) => {
      // Count from events data
      const eventsForStage = (events ?? []).filter(
        (e) => (e.message ?? '').toLowerCase().includes(stage.replace(/_/g, ' '))
      )
      const total = all.length
      const completed = all.filter((p) => {
        const tl = (p as any).timeline
        if (!Array.isArray(tl)) return false
        return tl.some((t: any) => t.stage === stage && t.status === 'completed')
      }).length
      const inProgress = all.filter((p) => {
        const tl = (p as any).timeline
        if (!Array.isArray(tl)) return false
        return tl.some((t: any) => t.stage === stage && t.status === 'in_progress')
      }).length
      const failed = all.filter((p) => {
        const tl = (p as any).timeline
        if (!Array.isArray(tl)) return false
        return tl.some((t: any) => t.stage === stage && t.status === 'failed')
      }).length
      const pct = total > 0 ? Math.round((completed / total) * 100) : eventsForStage.length > 0 ? 50 : 0
      return {
        stage,
        label: STAGE_LABELS[stage] || stage,
        total,
        completed,
        inProgress,
        failed,
        pct,
      }
    })
  }, [placements, events])

  // ── Section A: activity trend ────────────────────────────────────────────
  const chartData = useMemo(() => {
    if (activity && activity.length > 0) return activity
    // Derive from placements if activity API returns nothing
    const all = placements ?? []
    if (all.length === 0) return []
    // Group by week from updated_at
    const byWeek = new Map<string, { submitted: number; matched: number; approved: number; rejected: number }>()
    for (const p of all) {
      const d = new Date(p.updated_at ?? p.created_at ?? '')
      if (isNaN(d.getTime())) continue
      const week = `W${Math.ceil(d.getDate() / 7)} ${d.toLocaleString('default', { month: 'short' })}`
      const entry = byWeek.get(week) ?? { submitted: 0, matched: 0, approved: 0, rejected: 0 }
      entry.submitted++
      if (p.status === 'approved') entry.approved++
      if (p.status === 'rejected') entry.rejected++
      if (['pending_supervisor', 'matched'].includes(p.status ?? '')) entry.matched++
      byWeek.set(week, entry)
    }
    return Array.from(byWeek.entries()).map(([name, v]) => ({ name, ...v }))
  }, [activity, placements])

  // ── Section D: recent events ─────────────────────────────────────────────
  const recentEvents = useMemo(() => (events ?? []).slice(0, 10), [events])

  return (
    <div className="space-y-6">
      {/* Section A – Activity Trend */}
      <GlassCard>
        <GlassCardHeader>
          <GlassCardTitle>Workflow Activity Trend</GlassCardTitle>
          <div className="flex items-center gap-4">
            {[
              { color: '#6366f1', label: 'Submitted' },
              { color: '#06b6d4', label: 'Matched' },
              { color: '#10b981', label: 'Approved' },
              { color: '#f97316', label: 'Rejected' },
            ].map((l) => (
              <div key={l.label} className="flex items-center gap-1.5">
                <div className="w-2 h-2 rounded-full" style={{ background: l.color }} />
                <span className="text-xs text-muted-foreground">{l.label}</span>
              </div>
            ))}
          </div>
        </GlassCardHeader>
        <div className="h-56 px-4 pb-4">
          {actLoading ? (
            <div className="h-full rounded-lg bg-surface-alt animate-pulse" />
          ) : chartData.length === 0 ? (
            <div className="h-full flex items-center justify-center">
              <p className="text-sm text-muted-foreground">No activity data yet</p>
            </div>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData}>
                <defs>
                  {[
                    { id: 'submitted', color: '#6366f1' },
                    { id: 'matched', color: '#06b6d4' },
                    { id: 'approved', color: '#10b981' },
                    { id: 'rejected', color: '#f97316' },
                  ].map(({ id, color }) => (
                    <linearGradient key={id} id={id} x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor={color} stopOpacity={0.25} />
                      <stop offset="95%" stopColor={color} stopOpacity={0} />
                    </linearGradient>
                  ))}
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" />
                <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fill: '#6b6b80', fontSize: 11 }} />
                <YAxis axisLine={false} tickLine={false} tick={{ fill: '#6b6b80', fontSize: 11 }} />
                <Tooltip contentStyle={CHART_TOOLTIP_STYLE} labelStyle={{ color: '#e8e8f0' }} />
                <Area type="monotone" dataKey="submitted" stroke="#6366f1" fill="url(#submitted)" strokeWidth={2} />
                <Area type="monotone" dataKey="matched" stroke="#06b6d4" fill="url(#matched)" strokeWidth={2} />
                <Area type="monotone" dataKey="approved" stroke="#10b981" fill="url(#approved)" strokeWidth={2} />
                <Area type="monotone" dataKey="rejected" stroke="#f97316" fill="url(#rejected)" strokeWidth={2} />
              </AreaChart>
            </ResponsiveContainer>
          )}
        </div>
      </GlassCard>

      {/* Section C – Live Metrics */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
        <LiveMetricCard icon={Activity} label="Active Workflows" value={metrics.active} color="bg-primary/15 text-primary" />
        <LiveMetricCard icon={Clock} label="Avg Completion" value={metrics.avgCompletionMin > 0 ? `${metrics.avgCompletionMin}m` : '—'} color="bg-secondary/15 text-secondary" />
        <LiveMetricCard icon={CheckCircle} label="Completed Today" value={metrics.completedToday} color="bg-success/15 text-success" />
        <LiveMetricCard icon={TrendingUp} label="Approval Rate" value={`${metrics.approvalRate}%`} color="bg-info/15 text-info" />
        <LiveMetricCard icon={XCircle} label="Failure Rate" value={`${metrics.failureRate}%`} color={metrics.failureRate > 20 ? 'bg-destructive/15 text-destructive' : 'bg-muted/15 text-muted-foreground'} />
      </div>

      {/* Section B – Stage Breakdown */}
      <GlassCard>
        <GlassCardHeader>
          <GlassCardTitle>Stage Breakdown</GlassCardTitle>
        </GlassCardHeader>
        <div className="overflow-x-auto px-4 pb-4">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-glass-border">
                {['Stage', 'Total', 'Completed', 'In Progress', 'Failed', 'Completion %'].map((h) => (
                  <th key={h} className="text-left py-2 pr-4 text-muted-foreground font-medium uppercase tracking-wider">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {stageBreakdown.map((row) => (
                <tr key={row.stage} className="border-b border-glass-border/50 hover:bg-glass transition-colors">
                  <td className="py-2 pr-4 font-medium text-foreground">{row.label}</td>
                  <td className="py-2 pr-4 font-mono text-muted-foreground">{row.total}</td>
                  <td className="py-2 pr-4 font-mono text-success">{row.completed}</td>
                  <td className="py-2 pr-4 font-mono text-info">{row.inProgress}</td>
                  <td className="py-2 pr-4 font-mono text-destructive">{row.failed}</td>
                  <td className="py-2 pr-4">
                    <div className="flex items-center gap-2">
                      <div className="flex-1 h-1.5 rounded-full bg-surface-alt overflow-hidden min-w-[60px]">
                        <div
                          className={cn('h-full rounded-full transition-all', row.pct >= 80 ? 'bg-success' : row.pct >= 40 ? 'bg-warning' : 'bg-muted')}
                          style={{ width: `${row.pct}%` }}
                        />
                      </div>
                      <span className="font-mono text-muted-foreground w-8 text-right">{row.pct}%</span>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </GlassCard>

      {/* Section D – Recent Workflow Events */}
      <GlassCard>
        <GlassCardHeader>
          <GlassCardTitle>Recent Workflow Events</GlassCardTitle>
          <span className="text-xs text-muted-foreground">{recentEvents.length} events</span>
        </GlassCardHeader>
        <div className="overflow-x-auto px-4 pb-4">
          {recentEvents.length === 0 ? (
            <p className="text-sm text-muted-foreground py-4 text-center">No recent events</p>
          ) : (
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-glass-border">
                  {['Timestamp', 'Workflow', 'Stage', 'Status'].map((h) => (
                    <th key={h} className="text-left py-2 pr-4 text-muted-foreground font-medium uppercase tracking-wider">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {recentEvents.map((ev) => (
                  <tr key={ev.id} className="border-b border-glass-border/50 hover:bg-glass transition-colors">
                    <td className="py-2 pr-4 font-mono text-muted-foreground">{formatDate(ev.timestamp)}</td>
                    <td className="py-2 pr-4 font-mono text-foreground truncate max-w-[120px]">{ev.workflow_id}</td>
                    <td className="py-2 pr-4 text-foreground capitalize">{(ev.message ?? '').split(':')[0]}</td>
                    <td className="py-2 pr-4">
                      <span className={cn(
                        'px-2 py-0.5 rounded-full font-medium',
                        ev.type === 'placement' ? 'bg-success/15 text-success' :
                        ev.type === 'alert' ? 'bg-destructive/15 text-destructive' :
                        ev.type === 'approval' ? 'bg-warning/15 text-warning' :
                        'bg-info/15 text-info'
                      )}>
                        {ev.type}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </GlassCard>
    </div>
  )
}
