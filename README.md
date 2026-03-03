# song

Tooling to turn a still image + track into a stitched vertical video (Hedra generation + ffmpeg stitching).

**Quick start**
- Create `.env` (or `.venv/.env`) with `API_KEY`, `FFMPEG_PATH`, `FFPROBE_PATH`, `PROMPT_FILE`.
- Run pipeline: `.\.venv\Scripts\python.exe .\mirror_mouth_pipeline.py`
- Run API server: `.\.venv\Scripts\python.exe .\api_server.py`
- Run client: `cd client; npm ci; npm run dev`

**Credit-saving knobs** (all optional)
- `RESUME_SESSION=video_segments/<session_folder>`: resume a partial run without re-generating completed segments.
- `REUSE_COMPLETE_SESSIONS=1`: if a prior session has an identical fingerprint (same audio/image/prompt/model), hardlink/copy its `video_###.mp4` files into the new session and stitch without contacting Hedra.
- `REUSE_SEGMENTS_HARDLINK=1`: prefer hardlinks when reusing segments (fast, minimal disk).
- `REUSE_IMAGE_ASSET=1` / `FORCE_IMAGE_UPLOAD=1`: reuse or force reupload of the reference image asset.
- `REUSE_AUDIO_ASSET=1` / `FORCE_AUDIO_UPLOAD=1`: reuse or force reupload of audio segment assets.
- `REQUEST_DELAY_SECONDS=60`: rate limit buffer between segments.
- `QUEUED_STATUS_SLEEP_SECONDS=30` / `STATUS_POLL_INTERVAL_SECONDS=10`: status poll backoff.

**Model selection**
- Use `LIP_SYNC_REQUIRED=1` to require an audio-capable model (lip sync).
- Leave `HEDRA_MODEL_NAME` blank and set defaults:
  - `DEFAULT_LIPSYNC_MODEL_NAME=Kling AI Avatar v2 Standard`
  - `DEFAULT_NON_LIPSYNC_MODEL_NAME=<a cheaper non-audio model name>`
