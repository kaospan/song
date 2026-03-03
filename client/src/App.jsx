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
        }
      } catch (configError) {
        if (isMounted) {
          setError(configError.message)
        }
      }
    }

    loadConfig()

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
        <h1>Turn one still image and one song into a full lip-synced video.</h1>
        <p className="hero-copy">
          Upload the master track and reference portrait. The pipeline splits the song,
          generates timestamped segment runs, and returns one stitched vertical video using
          an audio-driven avatar model.
        </p>
      </section>

      <section className="workspace">
        <form className="upload-panel" onSubmit={handleSubmit}>
          <div className="panel-header">
            <h2>Inputs</h2>
            <span className="chip">Model: {defaultModelName || 'Loading...'}</span>
          </div>

          <label className="field-card">
            <span className="field-label">Song file</span>
            <input
              accept="audio/*,.mp3,.wav,.m4a"
              type="file"
              onChange={(event) => setSongFile(event.target.files?.[0] ?? null)}
            />
            <strong>{songFile ? songFile.name : 'No file selected'}</strong>
          </label>

          <label className="field-card">
            <span className="field-label">Song title</span>
            <input
              placeholder="Enter the song title"
              type="text"
              value={songTitle}
              onChange={(event) => setSongTitle(event.target.value)}
            />
          </label>

          <label className="field-card">
            <span className="field-label">Artist</span>
            <input
              placeholder="Enter the artist name"
              type="text"
              value={songArtist}
              onChange={(event) => setSongArtist(event.target.value)}
            />
          </label>

          <label className="field-card">
            <span className="field-label">Still image</span>
            <input
              accept="image/*,.png,.jpg,.jpeg,.webp"
              type="file"
              onChange={(event) => setImageFile(event.target.files?.[0] ?? null)}
            />
            <strong>{imageFile ? imageFile.name : 'No file selected'}</strong>
          </label>

          <button className="launch-button" disabled={isSubmitting} type="submit">
            {isSubmitting ? 'Starting job...' : 'Generate full video'}
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
              'Start a job to upload assets, generate segments, and stitch the final music video.'}
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
                <dt>Song</dt>
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
              Download MP4
            </a>
          ) : null}
        </div>

        {videoUrl ? (
          <video className="result-video" controls src={videoUrl} />
        ) : (
          <div className="video-placeholder">
            The final video player appears here when the backend marks the job complete.
          </div>
        )}
      </section>
    </main>
  )
}

export default App
