import { Outlet, Navigate } from 'react-router-dom'
import { useHealthCheck } from '@/hooks/use-foster'
import { useAuth } from '@/contexts/AuthContext'
import { Sidebar } from '@/components/sidebar'
import { ToastContainer } from '@/components/ui/toast'
import { Wifi, WifiOff, Loader2 } from 'lucide-react'

function ConnectionBar() {
  const { data: isConnected, isLoading, refetch } = useHealthCheck()

  return (
    <div
      className={`fixed top-0 right-0 z-50 flex items-center gap-2 px-3 py-1 text-xs font-medium rounded-bl-lg border border-t-0 border-r-0 transition-all cursor-pointer ${
        isLoading
          ? 'bg-muted/10 text-muted-foreground border-border'
          : isConnected
            ? 'bg-success/15 text-success border-success/20'
            : 'bg-destructive/15 text-destructive border-destructive/20'
      }`}
      onClick={() => refetch()}
      title={isLoading ? 'Checking...' : isConnected ? 'Backend connected' : 'Click to retry'}
    >
      {isLoading ? (
        <Loader2 size={12} className="animate-spin" />
      ) : isConnected ? (
        <Wifi size={12} />
      ) : (
        <WifiOff size={12} />
      )}
      {isLoading ? 'Checking...' : isConnected ? 'API Connected' : 'API Disconnected'}
    </div>
  )
}

export function DashboardLayout() {
  const { isAuthenticated } = useAuth()
  const API_URL = import.meta.env.VITE_API_URL || ''

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />
  }

  return (
    <div className="flex min-h-screen bg-background">
      <Sidebar />
      <main className="flex-1 ml-56 min-h-screen">
        <ConnectionBar />
        <div className="p-6 lg:p-8 max-w-7xl mx-auto">
          <div className="text-[10px] text-muted-foreground/50 mb-4 font-mono">
            API: {API_URL}
          </div>
          <Outlet />
        </div>
      </main>
      <ToastContainer />
    </div>
  )
}
