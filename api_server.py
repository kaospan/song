#!/usr/bin/env python3
import shutil
import threading
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from mirror_mouth_pipeline import (
    API_BASE,
    DEFAULT_LIPSYNC_MODEL_NAME,
    DEFAULT_NON_LIPSYNC_MODEL_NAME,
    HEDRA_MODEL_NAME,
    PROMPT_FILE,
    get_headers,
    run_pipeline,
)

BASE_DIR = Path(__file__).resolve().parent
JOB_ROOT = BASE_DIR / "job_runs"
JOB_ROOT.mkdir(exist_ok=True)
PROMPT_PATH = Path(PROMPT_FILE)
if not PROMPT_PATH.is_absolute():
    PROMPT_PATH = (BASE_DIR / PROMPT_PATH).resolve()

jobs = {}
jobs_lock = threading.Lock()

app = FastAPI(title="Mirror Mouth Video API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def serialize_job(job):
    serialized = dict(job)
    serialized["download_url"] = None
    if serialized["status"] == "complete":
        serialized["download_url"] = f"/api/jobs/{serialized['id']}/video"
    return serialized


def update_job(job_id, **updates):
    with jobs_lock:
        if job_id not in jobs:
            raise KeyError(job_id)
        jobs[job_id].update(updates)
        jobs[job_id]["updated_at"] = iso_now()
        return dict(jobs[job_id])


def save_upload(upload, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as buffer:
        shutil.copyfileobj(upload.file, buffer)


def load_prompt_history():
    if not PROMPT_HISTORY_PATH.exists():
        return []
    try:
        return json.loads(PROMPT_HISTORY_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []


def save_prompt_history(entries):
    PROMPT_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROMPT_HISTORY_PATH.write_text(json.dumps(entries, indent=2), encoding="utf-8")


def append_prompt_history(entry):
    entries = load_prompt_history()
    entries.append(entry)
    save_prompt_history(entries)


def build_prompt(base_prompt, video_style):
    preset = VIDEO_STYLE_PRESETS.get(video_style)
    if not preset:
        return base_prompt
    return f"{base_prompt}\n\n{preset['text']}"

def remove_lipsync_instructions(prompt_text):
    lines = (prompt_text or "").splitlines()
    filtered = []
    for line in lines:
        if line.strip().lower().startswith("lip sync:"):
            continue
        filtered.append(line)

    prompt_text = "\n".join(filtered).strip()
    prompt_text = prompt_text.replace("lip-synced", "music video")
    prompt_text = prompt_text.replace("lip‑synced", "music video")
    prompt_text += "\n\nAudio: do not generate vocals; the final audio track will be added in post."
    return prompt_text


def build_prompt_with_options(base_prompt, video_style, lip_sync_required):
    prompt_text = build_prompt(base_prompt, video_style)
    if not lip_sync_required:
        prompt_text = remove_lipsync_instructions(prompt_text)
    return prompt_text


def fetch_models_metadata():
    try:
        headers = get_headers()
        resp = requests.get(f"{API_BASE}/models", headers=headers, timeout=30)
        if not resp.ok:
            return []
        models = resp.json()
        video_models = [
            {
                "name": model.get("name", ""),
                "id": model.get("id"),
                "type": model.get("type"),
                "requires_audio_input": bool(model.get("requires_audio_input")),
            }
            for model in models
            if model.get("type") == "video"
        ]
        return sorted(video_models, key=lambda model: model.get("name", "").lower())
    except requests.RequestException:
        return []
    except Exception:
        return []


def pick_default_model_name(models, require_audio_input):
    preferred = (
        DEFAULT_LIPSYNC_MODEL_NAME if require_audio_input else DEFAULT_NON_LIPSYNC_MODEL_NAME
    )
    preferred = (preferred or "").strip()

    if preferred:
        for model in models:
            if model.get("name", "").strip().lower() == preferred.lower():
                return model["name"]

    for model in models:
        if bool(model.get("requires_audio_input")) == bool(require_audio_input):
            return model.get("name") or preferred

    return preferred or (DEFAULT_LIPSYNC_MODEL_NAME if require_audio_input else DEFAULT_NON_LIPSYNC_MODEL_NAME)


def run_job(
    job_id,
    audio_path,
    image_paths,
    lyrics_path,
    prompt_file,
    model_name,
    segment_length_seconds,
    song_title,
    song_artist,
    video_style,
    lip_sync_required,
    segment_name,
    prompt_override,
):
    job_dir = JOB_ROOT / job_id
    final_video_path = job_dir / "final_music_video.mp4"
    audio_cache_root = JOB_ROOT / "audio_cache"

    update_job(
        job_id,
        status="processing",
        message="Uploading assets and generating video segments.",
    )

    try:
        lyrics_text = None
        if lyrics_path and Path(lyrics_path).exists():
            lyrics_text = Path(lyrics_path).read_text(encoding="utf-8", errors="replace")

        if prompt_override:
            prompt_text = str(prompt_override)
        else:
            base_prompt = PROMPT_PATH.read_text(encoding="utf-8")
            prompt_text = build_prompt_with_options(base_prompt, video_style, lip_sync_required)
        result = run_pipeline(
            input_mp3=str(audio_path),
            reference_images=[str(path) for path in image_paths],
            segment_length_seconds=segment_length_seconds,
            output_audio_folder=str(job_dir / "segments"),
            output_video_root=str(job_dir / "video_segments"),
            final_video_name=str(final_video_path),
            prompt_file=prompt_file,
            model_name=model_name,
            song_title=song_title,
            song_artist=song_artist,
            audio_cache_root=str(audio_cache_root),
            prompt_override=prompt_text,
            lip_sync_required=lip_sync_required,
            lyrics_text=lyrics_text,
        )
    except Exception as exc:
        update_job(
            job_id,
            status="error",
            message="Video generation failed.",
            error=str(exc),
            traceback=traceback.format_exc(),
        )
        return

    append_prompt_history(
        {
            "job_id": job_id,
            "created_at": iso_now(),
            "song_title": song_title,
            "song_artist": song_artist,
            "model_name": model_name,
            "video_style": video_style,
            "lip_sync_required": lip_sync_required,
            "segment_name": segment_name,
            "prompt": prompt_text,
        }
    )

    update_job(
        job_id,
        status="complete",
        message="Final video is ready.",
        final_video=result["final_video"],
        session_video_folder=result["session_video_folder"],
        video_segments=result["video_segments"],
    )


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/config")
def config():
    models = fetch_models_metadata()
    default_lipsync_model_name = pick_default_model_name(models, require_audio_input=True) if models else DEFAULT_LIPSYNC_MODEL_NAME
    default_non_lipsync_model_name = pick_default_model_name(models, require_audio_input=False) if models else DEFAULT_NON_LIPSYNC_MODEL_NAME

    return {
        "default_model_name": default_lipsync_model_name or HEDRA_MODEL_NAME,
        "default_lipsync_model_name": default_lipsync_model_name,
        "default_non_lipsync_model_name": default_non_lipsync_model_name,
        "default_lip_sync_required": True,
        "prompt_file": str(PROMPT_PATH),
    }


@app.get("/api/models")
def models():
    return {"models": fetch_models_metadata()}


@app.post("/api/jobs", status_code=202)
def create_job(
    song: UploadFile = File(...),
    image: UploadFile = File(None),
    images: list[UploadFile] = File(None),
    lyrics: UploadFile = File(None),
    model_name: str = Form(""),
    segment_length_seconds: int = Form(8),
    song_title: str = Form(""),
    song_artist: str = Form(""),
    video_style: str = Form("cinematic_studio"),
    lip_sync_required: str = Form("1"),
    segment_name: str = Form(""),
    prompt_override: str = Form(""),
):
    lip_sync = str(lip_sync_required).strip().lower() in ("1", "true", "yes", "on")
    resolved_model_name = (model_name or "").strip()
    if not resolved_model_name:
        models = fetch_models_metadata()
        resolved_model_name = pick_default_model_name(models, require_audio_input=lip_sync) if models else (
            DEFAULT_LIPSYNC_MODEL_NAME
            if lip_sync
            else (DEFAULT_NON_LIPSYNC_MODEL_NAME or DEFAULT_LIPSYNC_MODEL_NAME)
        )

    job_id = uuid.uuid4().hex
    job_dir = JOB_ROOT / job_id
    uploads_dir = job_dir / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)

    audio_name = Path(song.filename or "song.mp3").name
    audio_path = uploads_dir / audio_name

    save_upload(song, audio_path)

    image_uploads = []
    if images:
        image_uploads.extend(images)
    if image is not None:
        image_uploads.append(image)

    if not image_uploads:
        raise HTTPException(status_code=422, detail="At least one reference image is required.")

    image_paths = []
    for upload in image_uploads:
        image_name = Path(upload.filename or "image.png").name
        image_path = uploads_dir / image_name
        save_upload(upload, image_path)
        image_paths.append(image_path)

    lyrics_path = None
    lyrics_name = None
    if lyrics is not None:
        lyrics_name = Path(lyrics.filename or "lyrics.txt").name
        lyrics_path = uploads_dir / lyrics_name
        save_upload(lyrics, lyrics_path)

    primary_image_name = image_paths[0].name

    with jobs_lock:
        jobs[job_id] = {
            "id": job_id,
            "status": "queued",
            "message": "Job queued.",
            "error": None,
            "traceback": None,
            "created_at": iso_now(),
            "updated_at": iso_now(),
            "audio_filename": audio_name,
            "image_filename": primary_image_name,
            "image_filenames": [path.name for path in image_paths],
            "lyrics_filename": lyrics_name,
            "song_title": song_title,
            "song_artist": song_artist,
            "video_style": video_style,
            "lip_sync_required": lip_sync,
            "segment_name": segment_name,
            "model_name": resolved_model_name,
            "segment_length_seconds": segment_length_seconds,
            "final_video": None,
            "session_video_folder": None,
            "video_segments": [],
        }

    thread = threading.Thread(
        target=run_job,
        args=(
            job_id,
            audio_path,
            image_paths,
            lyrics_path,
            str(PROMPT_PATH),
            resolved_model_name,
            segment_length_seconds,
            song_title,
            song_artist,
            video_style,
            lip_sync,
            segment_name,
            prompt_override,
        ),
        daemon=True,
    )
    thread.start()

    return serialize_job(jobs[job_id])


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return serialize_job(job)


@app.get("/api/jobs/{job_id}/video")
def download_video(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "complete" or not job["final_video"]:
        raise HTTPException(status_code=409, detail="Video is not ready")

    video_path = Path(job["final_video"])
    if not video_path.exists():
        raise HTTPException(status_code=404, detail="Output video not found")

    return FileResponse(video_path, media_type="video/mp4", filename=video_path.name)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True)
