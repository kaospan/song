import { useEffect, useMemo, useState } from 'react'
import './App.css'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? ''

function App() {
  const [songFile, setSongFile] = useState(null)
  const [imageFile, setImageFile] = useState(null)
  const [songTitle, setSongTitle] = useState('')
  const [songArtist, setSongArtist] = useState('')
  const [job, setJob] = useState(null)
  const [defaultModelName, setDefaultModelName] = useState('')
  const [availableModels, setAvailableModels] = useState([])
  const [modelName, setModelName] = useState('')
  const [videoStyles, setVideoStyles] = useState([])
  const [videoStyle, setVideoStyle] = useState('cinematic_studio')
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [error, setError] = useState('')

  const imagePreviewUrl = useMemo(() => {
    if (!imageFile) return ''
    return URL.createObjectURL(imageFile)
  }, [imageFile])

  useEffect(() => {
    let isMounted = true

    async function loadConfig() {
      try {
        const response = await fetch(`${API_BASE}/api/config`)
        const payload = await response.json()
        if (!response.ok) {
          throw new Error(payload.detail || 'Unable to load backend config.')
        }
        if (isMounted) {
          setDefaultModelName(payload.default_model_name ?? '')
          setModelName(payload.default_model_name ?? '')
          setVideoStyles(payload.video_styles ?? [])
          setVideoStyle(payload.default_video_style ?? 'cinematic_studio')
        }
      } catch (configError) {
        if (isMounted) {
          setError(configError.message)
        }
      }
    }

    loadConfig()

    async function loadModels() {
      try {
        const response = await fetch(`${API_BASE}/api/models`)
        const payload = await response.json()
        if (!response.ok) {
          throw new Error(payload.detail || 'Unable to load models.')
        }
        if (isMounted) {
          setAvailableModels(payload.models ?? [])
        }
      } catch (modelError) {
        if (isMounted) {
          setError(modelError.message)
        }
      }
    }

    loadModels()

    return () => {
      isMounted = false
    }
  }, [])

  useEffect(() => {
    return () => {
      if (imagePreviewUrl) {
        URL.revokeObjectURL(imagePreviewUrl)
      }
    }
  }, [imagePreviewUrl])

  useEffect(() => {
    if (!job || !['queued', 'processing'].includes(job.status)) {
      return undefined
    }

    const intervalId = window.setInterval(async () => {
      try {
        const response = await fetch(`${API_BASE}/api/jobs/${job.id}`)
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

  async function handleSubmit(event) {
    event.preventDefault()

    if (!songFile || !imageFile) {
      setError('Choose both an audio file and a still image before starting.')
      return
    }

    setError('')
    setIsSubmitting(true)

    try {
      const formData = new FormData()
      formData.append('song', songFile)
      formData.append('image', imageFile)
      formData.append('song_title', songTitle.trim())
      formData.append('song_artist', songArtist.trim())
      formData.append('model_name', modelName || defaultModelName)
      formData.append('video_style', videoStyle)

      const response = await fetch(`${API_BASE}/api/jobs`, {
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

  const videoUrl = job?.status === 'complete' ? `${API_BASE}${job.download_url}` : ''

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
            <span className="field-label">Audio file</span>
            <input
              accept="audio/*,.mp3,.wav,.m4a"
              type="file"
              onChange={(event) => setSongFile(event.target.files?.[0] ?? null)}
            />
            <strong>{songFile ? songFile.name : 'No file selected'}</strong>
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
            <select value={videoStyle} onChange={(event) => setVideoStyle(event.target.value)}>
              {(videoStyles.length ? videoStyles : [{ value: 'cinematic_studio', label: 'Cinematic Studio' }]).map(
                (style) => (
                  <option key={style.value} value={style.value}>
                    {style.label}
                  </option>
                )
              )}
            </select>
          </label>

          <label className="field-card">
            <span className="field-label">Render model</span>
            <select value={modelName} onChange={(event) => setModelName(event.target.value)}>
              {(availableModels.length ? availableModels : [defaultModelName || 'Loading...']).map((model) => (
                <option key={model} value={model}>
                  {model}
                </option>
              ))}
            </select>
          </label>

          <label className="field-card">
            <span className="field-label">Reference image</span>
            <input
              accept="image/*,.png,.jpg,.jpeg,.webp"
              type="file"
              onChange={(event) => setImageFile(event.target.files?.[0] ?? null)}
            />
            <strong>{imageFile ? imageFile.name : 'No file selected'}</strong>
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

          {job ? (
            <div className="reuse-flags">
              <span className={`reuse-pill ${job.reused_cached_audio ? 'on' : 'off'}`}>
                {job.reused_cached_audio ? 'Reused cached audio' : 'Fresh audio split'}
              </span>
              <span className={`reuse-pill ${job.reused_image_asset ? 'on' : 'off'}`}>
                {job.reused_image_asset ? 'Reused image asset' : 'Uploaded new image'}
              </span>
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
                <dt>Video type</dt>
                <dd>{job.video_style || videoStyle}</dd>
              </div>
              <div>
                <dt>Model</dt>
                <dd>{job.model_name || modelName || defaultModelName}</dd>
              </div>
              <div>
                <dt>Audio</dt>
                <dd>{job.audio_filename}</dd>
              </div>
              <div>
                <dt>Image</dt>
                <dd>{job.image_filename}</dd>
              </div>
              <div>
                <dt>Updated</dt>
                <dd>{new Date(job.updated_at).toLocaleString()}</dd>
              </div>
            </dl>
          ) : null}

          {imagePreviewUrl ? (
            <div className="preview-card">
              <img alt="Reference preview" src={imagePreviewUrl} />
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
