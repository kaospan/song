#!/usr/bin/env python3
import json
import shutil
import threading
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import requests

from mirror_mouth_pipeline import API_BASE, HEDRA_MODEL_NAME, PROMPT_FILE, get_headers, run_pipeline

BASE_DIR = Path(__file__).resolve().parent
JOB_ROOT = BASE_DIR / "job_runs"
JOB_ROOT.mkdir(exist_ok=True)
PROMPT_PATH = Path(PROMPT_FILE)
if not PROMPT_PATH.is_absolute():
    PROMPT_PATH = (BASE_DIR / PROMPT_PATH).resolve()
PROMPT_HISTORY_PATH = JOB_ROOT / "prompt_history.json"

VIDEO_STYLE_PRESETS = {
    "cinematic_studio": {
        "label": "Cinematic Studio",
        "text": "Video type: cinematic studio close-up performance. Keep lighting and framing consistent.",
    },
    "intimate_acoustic": {
        "label": "Intimate Acoustic",
        "text": "Video type: intimate acoustic performance. Softer expression and gentler emotion while keeping strict lip sync.",
    },
    "high_energy": {
        "label": "High Energy",
        "text": "Video type: high-energy performance. Increase intensity only where the audio clearly drives it; no extra acting.",
    },
    "noir_minimal": {
        "label": "Noir Minimal",
        "text": "Video type: minimalist noir. Even more restrained motion and expression; keep the mood austere.",
    },
}

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


def fetch_available_models():
    try:
        resp = requests.get(f"{API_BASE}/models", headers=get_headers(), timeout=30)
        if not resp.ok:
            return []
        models = resp.json()
        return sorted(
            [
                model.get("name", "")
                for model in models
                if model.get("type") == "video"
            ]
        )
    except requests.RequestException:
        return []


def run_job(
    job_id,
    audio_path,
    image_path,
    prompt_file,
    model_name,
    segment_length_seconds,
    song_title,
    song_artist,
    video_style,
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
        base_prompt = PROMPT_PATH.read_text(encoding="utf-8")
        prompt_text = build_prompt(base_prompt, video_style)
        result = run_pipeline(
            input_mp3=str(audio_path),
            reference_image=str(image_path),
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
        reused_cached_audio=result.get("reused_cached_audio", False),
        reused_image_asset=result.get("reused_image_asset", False),
    )


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/config")
def config():
    return {
        "default_model_name": HEDRA_MODEL_NAME,
        "prompt_file": str(PROMPT_PATH),
        "video_styles": [
            {"value": key, "label": preset["label"]}
            for key, preset in VIDEO_STYLE_PRESETS.items()
        ],
        "default_video_style": "cinematic_studio",
    }


@app.get("/api/models")
def models():
    return {"models": fetch_available_models()}


@app.post("/api/jobs", status_code=202)
def create_job(
    song: UploadFile = File(...),
    image: UploadFile = File(...),
    model_name: str = Form(HEDRA_MODEL_NAME),
    segment_length_seconds: int = Form(8),
    song_title: str = Form(""),
    song_artist: str = Form(""),
    video_style: str = Form("cinematic_studio"),
):
    job_id = uuid.uuid4().hex
    job_dir = JOB_ROOT / job_id
    uploads_dir = job_dir / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)

    audio_name = Path(song.filename or "song.mp3").name
    image_name = Path(image.filename or "image.png").name
    audio_path = uploads_dir / audio_name
    image_path = uploads_dir / image_name

    save_upload(song, audio_path)
    save_upload(image, image_path)

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
            "image_filename": image_name,
            "song_title": song_title,
            "song_artist": song_artist,
            "video_style": video_style,
            "model_name": model_name,
            "segment_length_seconds": segment_length_seconds,
            "final_video": None,
            "session_video_folder": None,
            "video_segments": [],
            "reused_cached_audio": False,
            "reused_image_asset": False,
        }

    thread = threading.Thread(
        target=run_job,
        args=(
            job_id,
            audio_path,
            image_path,
            str(PROMPT_PATH),
            model_name,
            segment_length_seconds,
            song_title,
            song_artist,
            video_style,
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
