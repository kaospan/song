import { useEffect, useMemo, useState } from 'react'
import './App.css'
import { segments } from './segmentRegistry'
import { buildPromptByName } from './promptBuilder'

const ENV_API_BASE = import.meta.env.VITE_API_BASE_URL ?? ''
const STORAGE_API_BASE_KEY = 'mirrorMouthApiBaseUrl'
const DEFAULT_DEV_API_BASE = 'http://127.0.0.1:8000'
const DEFAULT_SEGMENT_SECONDS = 8
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
    .replace(/\b\w/g, (match) => match.toUpperCase())
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

function formatDuration(seconds) {
  const s = Number(seconds)
  if (!Number.isFinite(s) || s <= 0) return '—'
  const total = Math.round(s)
  const mm = String(Math.floor(total / 60)).padStart(2, '0')
  const ss = String(total % 60).padStart(2, '0')
  return `${mm}:${ss}`
}

function DropZone({
  label,
  hint,
  accept,
  multiple,
  valueLabel,
  onFiles,
  children,
}) {
  const [isDragging, setIsDragging] = useState(false)

  function handleDrop(event) {
    event.preventDefault()
    setIsDragging(false)
    const files = Array.from(event.dataTransfer?.files ?? [])
    if (!files.length) return
    onFiles(files)
  }

  return (
    <div
      className={`dropzone ${isDragging ? 'dragging' : ''}`}
      onDragEnter={(event) => {
        event.preventDefault()
        setIsDragging(true)
      }}
      onDragOver={(event) => {
        event.preventDefault()
        setIsDragging(true)
      }}
      onDragLeave={(event) => {
        event.preventDefault()
        setIsDragging(false)
      }}
      onDrop={handleDrop}
      role="group"
      aria-label={label}
    >
      <div className="dropzone-header">
        <div>
          <div className="dropzone-label">{label}</div>
          {hint ? <p className="dropzone-hint">{hint}</p> : null}
        </div>
        <label className="dropzone-button">
          <input
            accept={accept}
            className="sr-only"
            multiple={multiple}
            type="file"
            onChange={(event) => {
              const files = Array.from(event.target.files ?? [])
              onFiles(files)
            }}
          />
          Choose file{multiple ? 's' : ''}
        </label>
      </div>

      <div className="dropzone-body">
        <div className="dropzone-value">{valueLabel}</div>
        {children}
      </div>
    </div>
  )
}

function App() {
  const [apiBaseUrl, setApiBaseUrl] = useState(() => {
    const stored = window.localStorage.getItem(STORAGE_API_BASE_KEY)
    const fallback = import.meta.env.DEV ? DEFAULT_DEV_API_BASE : ''
    return normalizeApiBase(ENV_API_BASE || stored || fallback)
  })
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
  const [backendStatus, setBackendStatus] = useState('unknown') // unknown | ok | error
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [error, setError] = useState('')
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [audioDurationSeconds, setAudioDurationSeconds] = useState(null)

  const apiBase = apiBaseUrl
  const apiUrl = (path) => `${apiBase}${path}`

  const imagePreviewUrls = useMemo(() => {
    if (!imageFiles?.length) return []
    return imageFiles.map((file) => URL.createObjectURL(file))
  }, [imageFiles])

  const promptText = useMemo(() => {
    try {
      return buildPromptByName(segmentName)
    } catch {
      return ''
    }
  }, [segmentName])

  useEffect(() => {
    let isMounted = true
    const base = normalizeApiBase(apiBaseUrl)

    async function loadConfig() {
      try {
        if (!base) {
          if (isMounted) {
            setBackendStatus('unknown')
          }
          return
        }

        const response = await fetch(`${base}/api/config`)
        const payload = await response.json()
        if (!response.ok) {
          throw new Error(payload.detail || 'Unable to load backend config.')
        }
        if (isMounted) {
          setDefaultModelName(payload.default_model_name ?? '')
          setModelName(payload.default_model_name ?? '')
          const nextDefaultSegment = payload.default_video_style ?? 'cinematic_studio'
          if (Object.prototype.hasOwnProperty.call(segments, nextDefaultSegment)) {
            setSegmentName(nextDefaultSegment)
          }
          setLipSyncRequired(Boolean(payload.default_lip_sync_required ?? true))
          setDefaultLipSyncModelName(payload.default_lipsync_model_name ?? '')
          setDefaultNonLipSyncModelName(payload.default_non_lipsync_model_name ?? '')
          setBackendStatus('ok')
        }
      } catch (configError) {
        if (isMounted) {
          setError(configError.message)
          setBackendStatus('error')
        }
      }
    }

    async function loadModels() {
      try {
        if (!base) {
          if (isMounted) {
            setAvailableModels([])
            setBackendStatus('unknown')
          }
          return
        }

        const response = await fetch(`${base}/api/models`)
        const payload = await response.json()
        if (!response.ok) {
          throw new Error(payload.detail || 'Unable to load models.')
        }
        if (isMounted) {
          setAvailableModels(payload.models ?? [])
          setBackendStatus('ok')
        }
      } catch (modelError) {
        if (isMounted) {
          setError(modelError.message)
          setBackendStatus('error')
        }
      }
    }

    setError('')
    loadConfig()
    loadModels()

    return () => {
      isMounted = false
    }
  }, [apiBaseUrl])

  useEffect(() => {
    if (!defaultLipSyncModelName && !defaultNonLipSyncModelName) return

    const recommended = lipSyncRequired ? defaultLipSyncModelName : defaultNonLipSyncModelName
    if (!modelTouched && recommended) {
      setModelName(recommended)
    }
  }, [defaultLipSyncModelName, defaultNonLipSyncModelName, lipSyncRequired, modelTouched])

  useEffect(() => {
    return () => {
      imagePreviewUrls.forEach((url) => URL.revokeObjectURL(url))
    }
  }, [imagePreviewUrls])

  useEffect(() => {
    if (!job || !['queued', 'processing'].includes(job.status)) {
      return undefined
    }

    const intervalId = window.setInterval(async () => {
      try {
        const response = await fetch(apiUrl(`/api/jobs/${job.id}`))
        const nextJob = await response.json()

        if (!response.ok) {
          throw new Error(nextJob.detail || 'Unable to refresh job status.')
        }

        setJob(nextJob)
      } catch (pollError) {
        setError(pollError.message)
      }
    }, 5000)

    return () => window.clearInterval(intervalId)
  }, [job])

  async function testBackendConnection(nextBaseUrl) {
    const base = normalizeApiBase(nextBaseUrl)
    setBackendStatus('unknown')
    setError('')

    try {
      const response = await fetch(`${base}/api/health`)
      const payload = await response.json()
      if (!response.ok || payload.status !== 'ok') {
        throw new Error('Backend health check failed.')
      }
      setBackendStatus('ok')
      return true
    } catch (backendError) {
      setBackendStatus('error')
      setError(backendError.message)
      return false
    }
  }

  async function handleSubmit(event) {
    event.preventDefault()

    if (!songFile || !imageFiles.length) {
      setError('Choose an audio file and at least one reference image before starting.')
      return
    }
    if (!promptText) {
      setError('Selected prompt segment is invalid.')
      return
    }

    setError('')
    setIsSubmitting(true)

    try {
      const formData = new FormData()
      formData.append('song', songFile)
      imageFiles.forEach((file) => formData.append('images', file))
      if (lyricsFile) {
        formData.append('lyrics', lyricsFile)
      }
      formData.append('song_title', songTitle.trim())
      formData.append('song_artist', songArtist.trim())
      formData.append('model_name', modelName || defaultModelName)
      formData.append('video_style', segmentName)
      formData.append('lip_sync_required', lipSyncRequired ? '1' : '0')
      formData.append('segment_name', segmentName)
      formData.append('prompt_override', promptText)

      const response = await fetch(apiUrl('/api/jobs'), {
        method: 'POST',
        body: formData,
      })
      const payload = await response.json()

      if (!response.ok) {
        throw new Error(payload.detail || 'Unable to start video generation.')
      }

      setJob(payload)
    } catch (submitError) {
      setError(submitError.message)
    } finally {
      setIsSubmitting(false)
    }
  }

  const videoUrl = job?.status === 'complete' ? `${apiBase}${job.download_url}` : ''
  const modelOptions = useMemo(() => {
    if (!availableModels?.length) return []
    const filtered = availableModels.filter((model) =>
      lipSyncRequired ? model.requires_audio_input : !model.requires_audio_input
    )
    return filtered.length ? filtered : availableModels
  }, [availableModels, lipSyncRequired])

  const mixedContentRisk = useMemo(() => {
    if (typeof window === 'undefined') return false
    if (!apiBase) return false
    return window.location.protocol === 'https:' && apiBase.startsWith('http://')
  }, [apiBase])

  return (
    <main className="app-shell">
      <section className="hero-panel">
        <p className="eyebrow">Mirror Mouth Studio</p>
        <h1>Turn a portrait and a track into a cinematic, lip‑synced performance.</h1>
        <p className="hero-copy">
          Upload a reference image and your mastered audio. The system segments the track,
          renders each performance beat, and delivers a single stitched vertical video ready
          for release.
        </p>
      </section>

      <section className="workspace">
        <form className="upload-panel" onSubmit={handleSubmit}>
          <div className="panel-header">
            <h2>Inputs</h2>
            <span className="chip">Model: {defaultModelName || 'Loading...'}</span>
          </div>

          <label className="field-card">
            <span className="field-label">Backend</span>
            <div className="backend-row">
              <input
                placeholder="https://your-backend.example.com"
                type="text"
                value={apiBaseUrl}
                onChange={(event) => setApiBaseUrl(normalizeApiBase(event.target.value))}
              />
              <button
                className="secondary-button"
                type="button"
                onClick={async () => {
                  const next = normalizeApiBase(apiBaseUrl)
                  window.localStorage.setItem(STORAGE_API_BASE_KEY, next)
                  const ok = await testBackendConnection(next)
                  if (ok) {
                    window.location.reload()
                  }
                }}
              >
                Test
              </button>
            </div>
            <p className="field-hint">
              Leave blank for local dev proxy. On GitHub Pages you must use an HTTPS backend URL.
            </p>
            <div className={`backend-status ${backendStatus}`}>
              {backendStatus === 'ok' ? 'Connected' : backendStatus === 'error' ? 'Not connected' : 'Not checked'}
            </div>
            {mixedContentRisk ? (
              <p className="warning-text">
                This page is HTTPS but your backend is HTTP. Browsers will block the requests. Use an HTTPS backend URL.
              </p>
            ) : null}
          </label>

          <label className="field-card">
            <span className="field-label">Audio file</span>
            <input
              accept="audio/*,.mp3,.wav,.m4a"
              type="file"
              onChange={(event) => setSongFile(event.target.files?.[0] ?? null)}
            />
            <strong>{songFile ? songFile.name : 'No file selected'}</strong>
          </label>

          <label className="field-card">
            <span className="field-label">Lyrics (optional)</span>
            <input
              accept=".txt,text/plain"
              type="file"
              onChange={(event) => setLyricsFile(event.target.files?.[0] ?? null)}
            />
            <strong>{lyricsFile ? lyricsFile.name : 'No file selected'}</strong>
            <p className="field-hint">
              Used to bias segment prompts. Lyrics are never rendered as on-screen text.
            </p>
          </label>

          <label className="field-card">
            <span className="field-label">Track title</span>
            <input
              placeholder="Enter the track title"
              type="text"
              value={songTitle}
              onChange={(event) => setSongTitle(event.target.value)}
            />
          </label>

          <label className="field-card">
            <span className="field-label">Artist / Performer</span>
            <input
              placeholder="Enter the artist or performer name"
              type="text"
              value={songArtist}
              onChange={(event) => setSongArtist(event.target.value)}
            />
          </label>

          <label className="field-card">
            <span className="field-label">Video concept</span>
            <select value={segmentName} onChange={(event) => setSegmentName(event.target.value)}>
              {Object.keys(segments)
                .sort()
                .map((name) => (
                  <option key={name} value={name}>
                    {name.replaceAll('_', ' ')}
                  </option>
                ))}
            </select>
            <p className="field-hint">Prompt length: {promptText.length} chars</p>
            {promptText.length > 2500 ? (
              <p className="warning-text">
                Prompt exceeds 2500 chars; the backend will trim it. Consider shortening the base prompt or segment text.
              </p>
            ) : null}
          </label>

          <label className="field-card">
            <span className="field-label">Lip sync</span>
            <div className="toggle-row">
              <input
                checked={lipSyncRequired}
                id="lip-sync-required"
                onChange={(event) => setLipSyncRequired(event.target.checked)}
                type="checkbox"
              />
              <label htmlFor="lip-sync-required">
                Require audio-driven lip sync (recommended)
              </label>
            </div>
            <p className="field-hint">
              When off, the backend picks a cheaper model and adds your audio in post.
            </p>
          </label>

          <label className="field-card">
            <span className="field-label">Render model</span>
            <select
              value={modelName}
              onChange={(event) => {
                setModelName(event.target.value)
                setModelTouched(true)
              }}
            >
              {(modelOptions.length ? modelOptions.map((model) => model.name) : [defaultModelName || 'Loading...']).map(
                (model) => (
                  <option key={model} value={model}>
                    {model}
                  </option>
                )
              )}
            </select>
          </label>

          <label className="field-card">
            <span className="field-label">Reference images</span>
            <input
              accept="image/*,.png,.jpg,.jpeg,.webp"
              multiple
              type="file"
              onChange={(event) => setImageFiles(Array.from(event.target.files ?? []))}
            />
            <strong>
              {imageFiles.length ? `${imageFiles.length} image(s) selected` : 'No file selected'}
            </strong>
          </label>

          <button className="launch-button" disabled={isSubmitting} type="submit">
            {isSubmitting ? 'Starting render...' : 'Generate performance video'}
          </button>

          {error ? <p className="error-text">{error}</p> : null}
        </form>

        <aside className="status-panel">
          <div className="panel-header">
            <h2>Run status</h2>
            <span className={`status-pill ${job?.status ?? 'idle'}`}>
              {job?.status ?? 'idle'}
            </span>
          </div>

          <p className="status-copy">
            {job?.message ??
              'Launch a render to upload assets, generate segments, and stitch the final performance cut.'}
          </p>

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
                <dt>Video type</dt>
                <dd>{job.video_style || segmentName}</dd>
              </div>
              <div>
                <dt>Model</dt>
                <dd>{job.model_name || modelName || defaultModelName}</dd>
              </div>
              <div>
                <dt>Lip sync</dt>
                <dd>{job.lip_sync_required ? 'required' : 'off'}</dd>
              </div>
              <div>
                <dt>Audio</dt>
                <dd>{job.audio_filename}</dd>
              </div>
              <div>
                <dt>Images</dt>
                <dd>
                  {job.image_filenames?.length
                    ? job.image_filenames.join(', ')
                    : job.image_filename ?? '—'}
                </dd>
              </div>
              <div>
                <dt>Updated</dt>
                <dd>{new Date(job.updated_at).toLocaleString()}</dd>
              </div>
            </dl>
          ) : null}

          {imagePreviewUrls.length ? (
            <div className="preview-card">
              <div className="preview-grid">
                {imagePreviewUrls.slice(0, 4).map((url, index) => (
                  <img alt={`Reference preview ${index + 1}`} key={url} src={url} />
                ))}
              </div>
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
          <div className="video-placeholder">
            The final performance cut will appear here once rendering completes.
          </div>
        )}
      </section>
    </main>
  )
}

export default App
