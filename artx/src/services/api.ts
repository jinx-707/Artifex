import axios from 'axios'

// Temporary logging to investigate production env injection
console.log('[api][env] import.meta.env:', import.meta.env)
console.log('[api][env] import.meta.env.VITE_API_URL:', import.meta.env.VITE_API_URL)

const API_URL = (() => {
  const v = import.meta.env.VITE_API_URL
  // In local dev we must not accidentally call the deployed Railway backend.
  // When VITE_API_URL is set (e.g. via artx/.env.production) and we run locally,
  // force it back to '' so Vite's dev proxy can route relative /children, etc.
  if (!v) return ''
  if (typeof window !== 'undefined' && window.location.hostname === 'localhost') return ''
  if (typeof window !== 'undefined' && window.location.hostname === '127.0.0.1') return ''
  return v
})() || ''

console.log(
  `[api] Initializing API client, baseURL: "${API_URL}" (computed from VITE_API_URL)`
)




export class ApiError extends Error {
  status: number | null
  constructor(message: string, status: number | null = null) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

export const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 15000,
})

// ── Request interceptor: inject Bearer token ──────────────────────────────────
api.interceptors.request.use(
  (config) => {
    // Inject stored JWT on every request (AuthContext also sets the default
    // header, but this interceptor acts as a safety net for requests made
    // before the context mounts or after a token refresh).
    const token = localStorage.getItem('artifex_token')
    if (token && !config.headers['Authorization']) {
      config.headers['Authorization'] = `Bearer ${token}`
    }

    console.log(
      `[api] ➡ ${config.method?.toUpperCase()} ${config.baseURL}${config.url}`,
      config.data ? { body: config.data } : ''
    )
    return config
  },
  (error) => {
    console.error('[api] Request setup error:', error)
    return Promise.reject(new ApiError(error.message))
  }
)

// ── Response interceptor: handle errors and 401 redirect ─────────────────────
api.interceptors.response.use(
  (response) => {
    console.log(`[api] ⬅ ${response.status} ${response.config.url}`, response.data)
    return response
  },
  (error) => {
    if (error.response) {
      const { status, data } = error.response

      // On 401, clear stored credentials and redirect to /login
      if (status === 401) {
        localStorage.removeItem('artifex_token')
        localStorage.removeItem('artifex_user')
        delete api.defaults.headers.common['Authorization']
        // Only redirect if not already on the login page to avoid loops
        if (!window.location.pathname.startsWith('/login')) {
          window.location.href = '/login'
        }
      }

      const detail = data?.detail
      const message = Array.isArray(detail)
        ? detail.map((d: { msg?: string }) => d.msg).join('; ')
        : detail || data?.message || data?.error || `Server error (${status})`
      console.error(`[api] ❌ ${status} ${error.config?.url}:`, data)
      return Promise.reject(new ApiError(message, status))
    }

    if (error.request) {
      const url = error.config?.url || 'unknown'
      console.error(`[api] ❌ Network error for ${url}:`, error.message)
      return Promise.reject(
        new ApiError(
          `Cannot reach backend at ${API_URL}${url}. Ensure the server is running. (${error.message})`
        )
      )
    }

    console.error('[api] ❌ Error:', error.message)
    return Promise.reject(new ApiError(error.message))
  }
)

export default api
