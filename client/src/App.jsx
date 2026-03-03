import { useEffect, useMemo, useState } from 'react'
import './App.css'
import { segments } from './segmentRegistry'
import { buildPromptByName } from './promptBuilder'

const ENV_API_BASE = import.meta.env.VITE_API_BASE_URL ?? ''
const STORAGE_API_BASE_KEY = 'songVideoApiBaseUrl'
const STORAGE_AUTH_TOKEN_KEY = 'songVideoAuthToken'
const STORAGE_AUTH_USER_KEY = 'songVideoAuthUser'

// Avoid collisions with other local dev backends that commonly use 8000.
const DEFAULT_DEV_API_BASE = 'http://127.0.0.1:8001'
const PROMPT_CHAR_SOFT_LIMIT = 2500

function normalizeApiBase(value) {
  const trimmed = String(value ?? '').trim()
  if (!trimmed) return ''
  return trimmed.endsWith('/') ? trimmed.slice(0, -1) : trimmed
}

function prettyLabel(value) {
  return String(value ?? '')
    .replaceAll('_', ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (m) => m.toUpperCase())
}

function formatBytes(bytes) {
  const size = Number(bytes || 0)
  if (!Number.isFinite(size) || size <= 0) return '—'
  const units = ['B', 'KB', 'MB', 'GB']
  let idx = 0
  let value = size
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024
    idx += 1
  }
  return `${value.toFixed(idx === 0 ? 0 : 1)} ${units[idx]}`
}

function App() {
  const [apiBaseUrl, setApiBaseUrl] = useState(() => {
    const stored = window.localStorage.getItem(STORAGE_API_BASE_KEY)
    const fallback = import.meta.env.DEV ? DEFAULT_DEV_API_BASE : ''
    return normalizeApiBase(ENV_API_BASE || stored || fallback)
  })
  const apiBase = normalizeApiBase(apiBaseUrl)

  const [backendStatus, setBackendStatus] = useState('unknown') // unknown | ok | error
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [error, setError] = useState('')

  const [authConfig, setAuthConfig] = useState({ auth_enabled: false, allow_register: false })
  const [authToken, setAuthToken] = useState(() => window.localStorage.getItem(STORAGE_AUTH_TOKEN_KEY) || '')
  const [authUser, setAuthUser] = useState(() => window.localStorage.getItem(STORAGE_AUTH_USER_KEY) || '')
  const [authMode, setAuthMode] = useState('login') // login | register
  const [authUsername, setAuthUsername] = useState('')
  const [authPassword, setAuthPassword] = useState('')
  const [promptHistory, setPromptHistory] = useState([])

  const [songFile, setSongFile] = useState(null)
  const [imageFiles, setImageFiles] = useState([])
  const [lyricsFile, setLyricsFile] = useState(null)
  const [songTitle, setSongTitle] = useState('')
  const [songArtist, setSongArtist] = useState('')
  const [job, setJob] = useState(null)

  const [defaultModelName, setDefaultModelName] = useState('')
  const [availableModels, setAvailableModels] = useState([])
  const [modelName, setModelName] = useState('')
  const [modelTouched, setModelTouched] = useState(false)
  const [segmentName, setSegmentName] = useState('cinematic_studio')
  const [lipSyncRequired, setLipSyncRequired] = useState(true)
  const [defaultLipSyncModelName, setDefaultLipSyncModelName] = useState('')
  const [defaultNonLipSyncModelName, setDefaultNonLipSyncModelName] = useState('')

  const [isSubmitting, setIsSubmitting] = useState(false)

  const authHeaders = useMemo(() => (authToken ? { Authorization: `Bearer ${authToken}` } : {}), [authToken])

  const promptText = useMemo(() => {
    try {
      return buildPromptByName(segmentName)
    } catch {
      return ''
    }
  }, [segmentName])

  const mixedContentRisk = useMemo(() => {
    if (typeof window === 'undefined') return false
    if (!apiBase) return false
    return window.location.protocol === 'https:' && apiBase.startsWith('http://')
  }, [apiBase])

  const imagePreviewUrls = useMemo(() => {
    if (!imageFiles?.length) return []
    return imageFiles.map((file) => URL.createObjectURL(file))
  }, [imageFiles])

  useEffect(() => {
    return () => imagePreviewUrls.forEach((url) => URL.revokeObjectURL(url))
  }, [imagePreviewUrls])

  useEffect(() => {
    let cancelled = false

    async function loadBootstrap() {
      setError('')
      if (!apiBase) return

      try {
        const health = await fetch(`${apiBase}/api/health`)
        if (!health.ok) throw new Error('Backend not reachable.')
        if (!cancelled) setBackendStatus('ok')
      } catch (e) {
        if (!cancelled) {
          setBackendStatus('error')
          setError(e.message || String(e))
        }
        return
      }

      try {
        const resp = await fetch(`${apiBase}/api/auth/config`)
        const payload = await resp.json()
        if (!cancelled && resp.ok) setAuthConfig(payload)
      } catch {
        // optional
      }

      try {
        const resp = await fetch(`${apiBase}/api/config`)
        const payload = await resp.json()
        if (!resp.ok) throw new Error(payload.detail || 'Unable to load backend config.')
        if (cancelled) return
        setDefaultModelName(payload.default_model_name ?? '')
        setModelName(payload.default_model_name ?? '')
        const nextDefaultSegment = payload.default_video_style ?? 'cinematic_studio'
        if (Object.prototype.hasOwnProperty.call(segments, nextDefaultSegment)) setSegmentName(nextDefaultSegment)
        setLipSyncRequired(Boolean(payload.default_lip_sync_required ?? true))
        setDefaultLipSyncModelName(payload.default_lipsync_model_name ?? '')
        setDefaultNonLipSyncModelName(payload.default_non_lipsync_model_name ?? '')
      } catch (e) {
        if (!cancelled) setError(e.message || String(e))
      }

      try {
        const resp = await fetch(`${apiBase}/api/models`)
        const payload = await resp.json()
        if (!resp.ok) throw new Error(payload.detail || 'Unable to load models.')
        if (!cancelled) setAvailableModels(payload.models ?? [])
      } catch (e) {
        if (!cancelled) setError(e.message || String(e))
      }
    }

    loadBootstrap()
    return () => {
      cancelled = true
    }
  }, [apiBase])

  useEffect(() => {
    if (!defaultLipSyncModelName && !defaultNonLipSyncModelName) return
    const recommended = lipSyncRequired ? defaultLipSyncModelName : defaultNonLipSyncModelName
    if (!modelTouched && recommended) setModelName(recommended)
  }, [defaultLipSyncModelName, defaultNonLipSyncModelName, lipSyncRequired, modelTouched])

  useEffect(() => {
    if (!apiBase || !authConfig.auth_enabled || !authToken) return
    let cancelled = false
    ;(async () => {
      try {
        const resp = await fetch(`${apiBase}/api/user/prompts?limit=10`, { headers: { ...authHeaders } })
        const payload = await resp.json()
        if (!resp.ok) return
        if (!cancelled) setPromptHistory(payload.prompts ?? [])
      } catch {
        // ignore
      }
    })()
    return () => {
      cancelled = true
    }
  }, [apiBase, authConfig.auth_enabled, authToken, authHeaders])

  useEffect(() => {
    if (!job || !['queued', 'processing'].includes(job.status)) return undefined
    const intervalId = window.setInterval(async () => {
      try {
        const resp = await fetch(`${apiBase}/api/jobs/${job.id}`, { headers: { ...authHeaders } })
        const payload = await resp.json()
        if (!resp.ok) throw new Error(payload.detail || 'Unable to refresh job status.')
        setJob(payload)
      } catch (e) {
        setError(e.message || String(e))
      }
    }, 5000)
    return () => window.clearInterval(intervalId)
  }, [apiBase, authHeaders, job])

  async function testBackendConnection() {
    const next = normalizeApiBase(apiBaseUrl)
    window.localStorage.setItem(STORAGE_API_BASE_KEY, next)
    setBackendStatus('unknown')
    setError('')
    try {
      const resp = await fetch(`${next}/api/health`)
      const payload = await resp.json()
      if (!resp.ok || payload.status !== 'ok') throw new Error('Backend health check failed.')
      setBackendStatus('ok')
    } catch (e) {
      setBackendStatus('error')
      setError(e.message || String(e))
    }
  }

  async function handleAuthSubmit(event) {
    event.preventDefault()
    setError('')
    if (!apiBase) {
      setError('Set a backend URL first.')
      setShowAdvanced(true)
      return
    }
    const path = authMode === 'register' ? '/api/auth/register' : '/api/auth/login'
    try {
      const resp = await fetch(`${apiBase}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: authUsername, password: authPassword }),
      })
      const payload = await resp.json()
      if (!resp.ok) throw new Error(payload.detail || 'Auth failed.')
      setAuthToken(payload.token)
      setAuthUser(payload.username)
      window.localStorage.setItem(STORAGE_AUTH_TOKEN_KEY, payload.token)
      window.localStorage.setItem(STORAGE_AUTH_USER_KEY, payload.username)
      setAuthPassword('')
    } catch (e) {
      setError(e.message || String(e))
    }
  }

  function signOut() {
    setAuthToken('')
    setAuthUser('')
    window.localStorage.removeItem(STORAGE_AUTH_TOKEN_KEY)
    window.localStorage.removeItem(STORAGE_AUTH_USER_KEY)
  }

  async function handleSubmit(event) {
    event.preventDefault()

    if (!apiBase) {
      setError('Set a backend URL first.')
      setShowAdvanced(true)
      return
    }
    if (authConfig.auth_enabled && !authToken) {
      setError('Sign in first.')
      setShowAdvanced(true)
      return
    }
    if (!songFile || !imageFiles.length) {
      setError('Choose an audio file and at least one reference image.')
      return
    }
    if (!promptText) {
      setError('Selected video concept is invalid.')
      return
    }

    setError('')
    setIsSubmitting(true)
    try {
      const formData = new FormData()
      formData.append('song', songFile)
      imageFiles.forEach((file) => formData.append('images', file))
      if (lyricsFile) formData.append('lyrics', lyricsFile)
      formData.append('song_title', songTitle.trim())
      formData.append('song_artist', songArtist.trim())
      formData.append('model_name', modelName || defaultModelName)
      formData.append('video_style', segmentName)
      formData.append('lip_sync_required', lipSyncRequired ? '1' : '0')
      formData.append('segment_name', segmentName)
      formData.append('prompt_override', promptText)

      const resp = await fetch(`${apiBase}/api/jobs`, { method: 'POST', body: formData, headers: { ...authHeaders } })
      const payload = await resp.json()
      if (!resp.ok) throw new Error(payload.detail || 'Unable to start video generation.')
      setJob(payload)
    } catch (e) {
      setError(e.message || String(e))
    } finally {
      setIsSubmitting(false)
    }
  }

  const modelOptions = useMemo(() => {
    if (!availableModels?.length) return []
    const filtered = availableModels.filter((m) => (lipSyncRequired ? m.requires_audio_input : !m.requires_audio_input))
    return filtered.length ? filtered : availableModels
  }, [availableModels, lipSyncRequired])

  const selectedModel = useMemo(() => availableModels.find((m) => m.name === modelName), [availableModels, modelName])
  const modelMismatch = Boolean(lipSyncRequired && selectedModel && !selectedModel.requires_audio_input)
  const videoUrl = job?.status === 'complete' ? `${apiBase}${job.download_url}` : ''

  return (
    <main className="app-shell">
      <section className="hero-panel">
        <p className="eyebrow">Song → Video</p>
        <h1>Turn an image + track into a stitched vertical music video.</h1>
        <p className="hero-copy">
          Local-first outputs, credit-saving caching, and an API-backed workflow that can run on your machine.
        </p>
      </section>

      <section className="workspace">
        <form className="upload-panel" onSubmit={handleSubmit}>
          <div className="panel-header">
            <h2>Create</h2>
            <span className={`status-pill ${backendStatus === 'ok' ? 'complete' : backendStatus === 'error' ? 'error' : 'queued'}`}>
              {backendStatus === 'ok' ? 'connected' : backendStatus === 'error' ? 'backend offline' : 'backend?'}
            </span>
          </div>

          <div className="field-card">
            <div className="field-card-title-row">
              <div>
                <div className="field-label">Backend</div>
                <p className="field-hint">If you see 404s, you are pointing at the wrong server/port.</p>
              </div>
              <button className="secondary-button" type="button" onClick={() => setShowAdvanced((v) => !v)}>
                {showAdvanced ? 'Hide' : 'Show'} settings
              </button>
            </div>

            {showAdvanced ? (
              <>
                <div className="backend-row">
                  <input
                    placeholder="http://127.0.0.1:8001"
                    type="text"
                    value={apiBaseUrl}
                    onChange={(e) => setApiBaseUrl(normalizeApiBase(e.target.value))}
                  />
                  <button className="secondary-button" type="button" onClick={testBackendConnection}>
                    Test
                  </button>
                </div>
                <div className={`backend-status ${backendStatus}`}>
                  {backendStatus === 'ok' ? `Connected to ${apiBaseUrl}` : backendStatus === 'error' ? 'Not connected' : 'Not checked'}
                </div>
                {mixedContentRisk ? (
                  <p className="warning-text">
                    This page is HTTPS but your backend is HTTP. Browsers will block requests. Use an HTTPS backend URL.
                  </p>
                ) : null}
              </>
            ) : (
              <div className={`backend-status ${backendStatus}`}>
                {backendStatus === 'ok' ? `Connected to ${apiBaseUrl}` : 'Click “Show settings” to configure.'}
              </div>
            )}
          </div>

          {authConfig.auth_enabled ? (
            <div className="field-card">
              <div className="field-card-title-row">
                <div>
                  <div className="field-label">Account</div>
                  <p className="field-hint">Required to save per-user settings and prompt history.</p>
                </div>
                {authToken ? (
                  <button className="secondary-button" type="button" onClick={signOut}>
                    Sign out
                  </button>
                ) : null}
              </div>

              {authToken ? (
                <div className="auth-row">
                  <span className="chip">Signed in as {authUser || '—'}</span>
                </div>
              ) : (
                <form className="auth-form" onSubmit={handleAuthSubmit}>
                  <div className="auth-mode">
                    <button
                      className={`secondary-button ${authMode === 'login' ? 'active' : ''}`}
                      type="button"
                      onClick={() => setAuthMode('login')}
                    >
                      Sign in
                    </button>
                    <button
                      className={`secondary-button ${authMode === 'register' ? 'active' : ''}`}
                      type="button"
                      disabled={!authConfig.allow_register}
                      onClick={() => setAuthMode('register')}
                    >
                      Create account
                    </button>
                  </div>
                  <div className="backend-row">
                    <input placeholder="Username" type="text" value={authUsername} onChange={(e) => setAuthUsername(e.target.value)} />
                    <input placeholder="Password" type="password" value={authPassword} onChange={(e) => setAuthPassword(e.target.value)} />
                  </div>
                  <button className="launch-button" type="submit">
                    {authMode === 'register' ? 'Create & Sign in' : 'Sign in'}
                  </button>
                </form>
              )}
            </div>
          ) : null}

          <label className="field-card">
            <span className="field-label">Audio</span>
            <input accept="audio/*,.mp3,.wav,.m4a" type="file" onChange={(e) => setSongFile(e.target.files?.[0] ?? null)} />
            <strong>{songFile ? `${songFile.name} · ${formatBytes(songFile.size)}` : 'No file selected'}</strong>
          </label>

          <label className="field-card">
            <span className="field-label">Reference images</span>
            <input
              accept="image/*,.png,.jpg,.jpeg,.webp"
              multiple
              type="file"
              onChange={(e) => setImageFiles(Array.from(e.target.files ?? []))}
            />
            <strong>{imageFiles.length ? `${imageFiles.length} image(s) selected` : 'No file selected'}</strong>
            {imagePreviewUrls.length ? (
              <div className="preview-card">
                <div className="preview-grid">
                  {imagePreviewUrls.slice(0, 4).map((url, idx) => (
                    <img alt={`Reference preview ${idx + 1}`} key={url} src={url} />
                  ))}
                </div>
              </div>
            ) : null}
          </label>

          <label className="field-card">
            <span className="field-label">Lyrics (optional)</span>
            <input accept=".txt,text/plain" type="file" onChange={(e) => setLyricsFile(e.target.files?.[0] ?? null)} />
            <strong>{lyricsFile ? `${lyricsFile.name} · ${formatBytes(lyricsFile.size)}` : 'No file selected'}</strong>
          </label>

          <label className="field-card">
            <span className="field-label">Track title</span>
            <input placeholder="Under My Skin" type="text" value={songTitle} onChange={(e) => setSongTitle(e.target.value)} />
          </label>

          <label className="field-card">
            <span className="field-label">Artist / Performer</span>
            <input placeholder="Artist name" type="text" value={songArtist} onChange={(e) => setSongArtist(e.target.value)} />
          </label>

          <label className="field-card">
            <span className="field-label">Video concept</span>
            <select value={segmentName} onChange={(e) => setSegmentName(e.target.value)}>
              {Object.keys(segments)
                .sort()
                .map((name) => (
                  <option key={name} value={name}>
                    {prettyLabel(name)}
                  </option>
                ))}
            </select>
            <p className="field-hint">Prompt length: {promptText.length} chars</p>
            {promptText.length > PROMPT_CHAR_SOFT_LIMIT ? (
              <p className="warning-text">Prompt exceeds {PROMPT_CHAR_SOFT_LIMIT} chars; the backend may trim it.</p>
            ) : null}
            <details className="prompt-preview">
              <summary>Preview prompt</summary>
              <pre className="prompt-preview-body">{promptText}</pre>
            </details>
          </label>

          <label className="field-card">
            <span className="field-label">Lip sync</span>
            <div className="toggle-row">
              <input checked={lipSyncRequired} id="lip-sync" onChange={(e) => setLipSyncRequired(e.target.checked)} type="checkbox" />
              <label htmlFor="lip-sync">Require audio-driven lip sync (recommended)</label>
            </div>
            <p className="field-hint">When off, the backend can pick a cheaper model and add your audio in post.</p>
          </label>

          <label className="field-card">
            <span className="field-label">Render model</span>
            <select
              value={modelName}
              onChange={(e) => {
                setModelName(e.target.value)
                setModelTouched(true)
              }}
            >
              {(modelOptions.length ? modelOptions.map((m) => m.name) : [defaultModelName || 'Loading...']).map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
            {modelMismatch ? (
              <p className="warning-text">This model does not accept audio input. Choose an audio-capable model to avoid wasting credits.</p>
            ) : null}
          </label>

          <button className="launch-button" disabled={isSubmitting || modelMismatch} type="submit">
            {isSubmitting ? 'Starting render...' : 'Generate video'}
          </button>

          {error ? <p className="error-text">{error}</p> : null}
        </form>

        <aside className="status-panel">
          <div className="panel-header">
            <h2>Status</h2>
            <span className={`status-pill ${job?.status ?? 'idle'}`}>{job?.status ?? 'idle'}</span>
          </div>

          <p className="status-copy">{job?.message ?? 'Start a render to upload assets, generate segments, and stitch the final cut.'}</p>

          {job ? (
            <div className="reuse-flags">
              <span className={`reuse-pill ${job.reused_cached_audio ? 'on' : 'off'}`}>{job.reused_cached_audio ? 'Reused cached audio' : 'Fresh audio split'}</span>
              <span className={`reuse-pill ${job.reused_image_asset ? 'on' : 'off'}`}>{job.reused_image_asset ? 'Reused image asset' : 'Uploaded new images'}</span>
            </div>
          ) : null}

          {job?.id ? (
            <dl className="meta-grid">
              <div>
                <dt>Job ID</dt>
                <dd>{job.id}</dd>
              </div>
              <div>
                <dt>Title</dt>
                <dd>{job.song_title || songTitle || 'Untitled'}</dd>
              </div>
              <div>
                <dt>Artist</dt>
                <dd>{job.song_artist || songArtist || 'Unknown'}</dd>
              </div>
              <div>
                <dt>Concept</dt>
                <dd>{prettyLabel(job.video_style || segmentName)}</dd>
              </div>
              <div>
                <dt>Model</dt>
                <dd>{job.model_name || modelName || defaultModelName}</dd>
              </div>
              <div>
                <dt>Updated</dt>
                <dd>{job.updated_at ? new Date(job.updated_at).toLocaleString() : '—'}</dd>
              </div>
            </dl>
          ) : null}

          {authConfig.auth_enabled && authToken ? (
            <div className="field-card">
              <div className="field-card-title-row">
                <div>
                  <div className="field-label">Prompt history</div>
                  <p className="field-hint">Click an entry to re-apply settings.</p>
                </div>
              </div>
              {promptHistory.length ? (
                <div className="history-list">
                  {promptHistory.slice(0, 6).map((entry) => (
                    <button
                      className="history-item"
                      key={`${entry.created_at}-${entry.job_id}`}
                      type="button"
                      onClick={() => {
                        if (entry.song_title) setSongTitle(entry.song_title)
                        if (entry.song_artist) setSongArtist(entry.song_artist)
                        if (entry.segment_name) setSegmentName(entry.segment_name)
                        if (entry.model_name) {
                          setModelName(entry.model_name)
                          setModelTouched(true)
                        }
                        if (typeof entry.lip_sync_required === 'boolean') setLipSyncRequired(entry.lip_sync_required)
                      }}
                    >
                      <div className="history-main">
                        <div className="history-title">{entry.song_title || 'Untitled'}</div>
                        <div className="history-sub">{entry.song_artist || 'Unknown'}</div>
                      </div>
                      <div className="history-meta">{prettyLabel(entry.segment_name || entry.video_style)}</div>
                    </button>
                  ))}
                </div>
              ) : (
                <p className="field-hint">No prompt history yet.</p>
              )}
            </div>
          ) : null}
        </aside>
      </section>

      <section className="output-panel">
        <div className="panel-header">
          <h2>Output</h2>
          {videoUrl ? (
            <a className="download-link" href={videoUrl}>
              Download final MP4
            </a>
          ) : null}
        </div>

        {videoUrl ? (
          <video className="result-video" controls src={videoUrl} />
        ) : (
          <div className="video-placeholder">The final stitched MP4 will appear here once rendering completes.</div>
        )}
      </section>
    </main>
  )
}

export default App

