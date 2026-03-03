#!/usr/bin/env python3
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import threading
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import requests

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
USERS_DB_PATH = JOB_ROOT / "users.json"
USERS_DIR = JOB_ROOT / "users"
USERS_DIR.mkdir(exist_ok=True)
PROMPT_PATH = Path(PROMPT_FILE)
if not PROMPT_PATH.is_absolute():
    PROMPT_PATH = (BASE_DIR / PROMPT_PATH).resolve()
PROMPT_HISTORY_PATH = JOB_ROOT / "prompt_history.json"

VIDEO_STYLE_PRESETS = {
    "cinematic_studio": {
        "label": "Cinematic Studio Close-Up",
        "description": "Clean, controlled, high-end studio performance. Stable framing, premium lighting, minimal movement.",
        "direction": (
            "Setting: controlled studio or void background.\n"
            "Lighting: cinematic key + soft fill, stable exposure, no flicker.\n"
            "Camera: locked or very subtle stabilized motion; keep subject size consistent.\n"
            "Texture: photoreal skin detail, mild film grain.\n"
            "Editing rhythm: steady, confident, performance-first."
        ),
    },
    "noir_minimal": {
        "label": "Noir Minimal",
        "description": "Moody, high-contrast shadows with restrained motion and psychological tension. Minimal set dressing.",
        "direction": (
            "Setting: minimalist dark environment, sparse background.\n"
            "Lighting: hard key, deep shadow, controlled highlights; no exposure drift.\n"
            "Camera: locked tripod close-up; no reframing.\n"
            "Texture: crisp contrast, subtle grain.\n"
            "Editing rhythm: slow-burn, tension builds through micro-changes."
        ),
    },
    "neon_rain": {
        "label": "Neon Rain",
        "description": "Modern neon nightlife vibe: wet reflections, colored bokeh, glossy highlights, cinematic contrast.",
        "direction": (
            "Setting: night city bokeh, wet surfaces, practical neon.\n"
            "Lighting: colored rim lights (cyan/magenta/amber), controlled specular highlights.\n"
            "Camera: stabilized glide or gentle orbit; keep continuity.\n"
            "Texture: shallow depth of field, atmospheric haze.\n"
            "Editing rhythm: slightly quicker on choruses, calmer on verses."
        ),
    },
    "surreal_sketch": {
        "label": "Surreal Sketch Overlay",
        "description": "Photoreal base with subtle, tasteful pencil/ink overlays that escalate with the song’s intensity.",
        "direction": (
            "Setting: real environment kept simple so overlays read clearly.\n"
            "Lighting: cinematic HDR, stable exposure.\n"
            "Camera: stabilized motion; avoid jitter so overlays track cleanly.\n"
            "Anomalies: 1–2 frame sketch artifacts, ink veins, outline flickers; never text-like.\n"
            "Editing rhythm: anomalies peak on hooks, recede on quiet lines."
        ),
    },
    "high_fashion_editorial": {
        "label": "High Fashion Editorial",
        "description": "Premium editorial look: clean compositions, confident pacing, iconic lighting, luxury texture.",
        "direction": (
            "Setting: minimalist set, architectural shapes, premium styling.\n"
            "Lighting: soft key + strong edge/rim, glossy highlights, controlled shadows.\n"
            "Camera: deliberate dolly pushes or slow orbit; no shake.\n"
            "Texture: high detail, tasteful grain.\n"
            "Editing rhythm: clean, punchy cuts at musical accents."
        ),
    },
    "documentary_grit": {
        "label": "Documentary Grit",
        "description": "Raw and intimate realism. Natural light, subtle handheld energy, grounded emotional read.",
        "direction": (
            "Setting: real locations, imperfect textures.\n"
            "Lighting: naturalistic, motivated practicals.\n"
            "Camera: gentle handheld or shoulder-rig feel (minimal jitter), human proximity.\n"
            "Texture: slight noise, authentic contrast.\n"
            "Editing rhythm: reactive to phrasing, breath, and pauses."
        ),
    },
    "dream_haze": {
        "label": "Dream Haze",
        "description": "Ethereal, emotional, soft bloom and haze. Feels like a memory: warm highlights, gentle motion.",
        "direction": (
            "Setting: abstracted, softened backgrounds; dreamy atmosphere.\n"
            "Lighting: bloom-y highlights, gentle falloff; no clipping.\n"
            "Camera: slow drift, stabilized; no sharp movements.\n"
            "Texture: soft film grain, subtle halation.\n"
            "Editing rhythm: breathy, lyrical, dissolves feel natural."
        ),
    },
    "glitch_modern": {
        "label": "Modern Glitch (Tasteful)",
        "description": "Clean modern base with rare, tasteful glitch moments timed to impact. No on-screen text artifacts.",
        "direction": (
            "Setting: modern, graphic shapes and light sources.\n"
            "Lighting: crisp, controlled contrast.\n"
            "Camera: stabilized; keep geometry consistent.\n"
            "Anomalies: micro-glitches (1–2 frames), chromatic split, frame stutter; never captions/timecode.\n"
            "Editing rhythm: impact moments only; do not overuse."
        ),
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

AUTH_ENABLED = str(os.environ.get("AUTH_ENABLED", "0")).strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
AUTH_ALLOW_REGISTER = str(os.environ.get("AUTH_ALLOW_REGISTER", "1")).strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
AUTH_TOKEN_TTL_HOURS = int(os.environ.get("AUTH_TOKEN_TTL_HOURS", "168"))
DEFAULT_USER = "default"

sessions = {}
sessions_lock = threading.Lock()


class AuthRequest(BaseModel):
    username: str
    password: str


class SettingsPayload(BaseModel):
    settings: dict


def normalize_username(value: str) -> str:
    username = (value or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{3,32}", username):
        raise HTTPException(
            status_code=422,
            detail="Username must be 3-32 characters of letters, numbers, underscore, or hyphen.",
        )
    return username


def ensure_users_db():
    if not USERS_DB_PATH.exists():
        USERS_DB_PATH.write_text(json.dumps({"users": {}}, indent=2), encoding="utf-8")


def load_users_db():
    ensure_users_db()
    try:
        return json.loads(USERS_DB_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"users": {}}


def save_users_db(db):
    USERS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = USERS_DB_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(db, indent=2), encoding="utf-8")
    tmp.replace(USERS_DB_PATH)


def user_dir(username: str) -> Path:
    return USERS_DIR / username


def user_settings_path(username: str) -> Path:
    return user_dir(username) / "settings.json"


def user_prompt_history_path(username: str) -> Path:
    return user_dir(username) / "prompt_history.json"


def pbkdf2_hash_password(password: str, salt_hex: str, iterations: int = 200_000) -> str:
    salt = bytes.fromhex(salt_hex)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return derived.hex()


def verify_password(password: str, salt_hex: str, expected_hash_hex: str) -> bool:
    calculated = pbkdf2_hash_password(password, salt_hex)
    return hmac.compare_digest(calculated, expected_hash_hex)


def create_session(username: str) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc).timestamp() + (AUTH_TOKEN_TTL_HOURS * 3600)
    with sessions_lock:
        sessions[token] = {"username": username, "expires_at": expires_at}
    return token


def get_current_user(
    authorization: str | None = Header(default=None),
) -> str | None:
    if not AUTH_ENABLED:
        return None

    auth = (authorization or "").strip()
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token.")

    token = auth.split(None, 1)[1].strip()
    with sessions_lock:
        session = sessions.get(token)

    if not session:
        raise HTTPException(status_code=401, detail="Invalid or expired token.")

    if datetime.now(timezone.utc).timestamp() > float(session.get("expires_at", 0)):
        with sessions_lock:
            sessions.pop(token, None)
        raise HTTPException(status_code=401, detail="Invalid or expired token.")

    return session["username"]


def require_user(user: str | None = Depends(get_current_user)) -> str:
    if AUTH_ENABLED and not user:
        raise HTTPException(status_code=401, detail="Authentication required.")
    return user or DEFAULT_USER


def load_user_settings(username: str) -> dict:
    path = user_settings_path(username)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_user_settings(username: str, settings: dict):
    d = user_dir(username)
    d.mkdir(parents=True, exist_ok=True)
    path = user_settings_path(username)
    path.write_text(json.dumps(settings, indent=2), encoding="utf-8")


def load_user_prompt_history(username: str) -> list[dict]:
    path = user_prompt_history_path(username)
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []


def append_user_prompt_history(username: str, entry: dict):
    d = user_dir(username)
    d.mkdir(parents=True, exist_ok=True)
    entries = load_user_prompt_history(username)
    entries.append(entry)
    user_prompt_history_path(username).write_text(
        json.dumps(entries, indent=2), encoding="utf-8"
    )


@app.get("/api/auth/config")
def auth_config():
    return {
        "auth_enabled": AUTH_ENABLED,
        "allow_register": AUTH_ALLOW_REGISTER,
        "token_ttl_hours": AUTH_TOKEN_TTL_HOURS,
    }


@app.post("/api/auth/register")
def auth_register(payload: AuthRequest):
    if not AUTH_ENABLED:
        raise HTTPException(status_code=409, detail="Auth is disabled on this server.")
    if not AUTH_ALLOW_REGISTER:
        raise HTTPException(
            status_code=403, detail="Registration is disabled on this server."
        )

    username = normalize_username(payload.username)
    password = str(payload.password or "")
    if len(password) < 8:
        raise HTTPException(
            status_code=422, detail="Password must be at least 8 characters."
        )

    db = load_users_db()
    users = db.setdefault("users", {})
    if username in users:
        raise HTTPException(status_code=409, detail="Username already exists.")

    salt_hex = secrets.token_hex(16)
    users[username] = {
        "created_at": iso_now(),
        "salt": salt_hex,
        "password_hash": pbkdf2_hash_password(password, salt_hex),
    }
    save_users_db(db)

    # Initialize per-user files
    save_user_settings(username, {})
    user_prompt_history_path(username).write_text("[]", encoding="utf-8")

    token = create_session(username)
    return {"token": token, "username": username}


@app.post("/api/auth/login")
def auth_login(payload: AuthRequest):
    if not AUTH_ENABLED:
        raise HTTPException(status_code=409, detail="Auth is disabled on this server.")

    username = normalize_username(payload.username)
    password = str(payload.password or "")

    db = load_users_db()
    user = (db.get("users") or {}).get(username)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password.")

    if not verify_password(password, user.get("salt", ""), user.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="Invalid username or password.")

    token = create_session(username)
    return {"token": token, "username": username}


@app.get("/api/user")
def get_user(username: str = Depends(require_user)):
    return {"username": username, "settings": load_user_settings(username)}


@app.get("/api/user/settings")
def get_user_settings(username: str = Depends(require_user)):
    return {"settings": load_user_settings(username)}


@app.put("/api/user/settings")
def put_user_settings(payload: SettingsPayload, username: str = Depends(require_user)):
    settings = payload.settings or {}
    if not isinstance(settings, dict):
        raise HTTPException(status_code=422, detail="settings must be an object.")
    save_user_settings(username, settings)
    return {"settings": settings}


@app.get("/api/user/prompts")
def get_user_prompts(limit: int = 50, username: str = Depends(require_user)):
    limit = max(1, min(int(limit or 50), 500))
    entries = load_user_prompt_history(username)
    return {"prompts": list(reversed(entries[-limit:]))}


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


def build_master_prompt(video_style: str, lip_sync_required: bool) -> str:
    """
    Content-agnostic "master direction" prompt assembled from the selected concept.
    This is intentionally generic: identity and composition are driven by uploaded reference images.
    """
    preset = VIDEO_STYLE_PRESETS.get(video_style) or VIDEO_STYLE_PRESETS["cinematic_studio"]
    label = preset.get("label", "Video concept")
    description = preset.get("description", "")
    direction = preset.get("direction", "").strip()

    lipsync_block = ""
    if lip_sync_required:
        lipsync_block = (
            "Lip sync: strict audio-driven lip sync. Every audible lyric/syllable must match visible mouth articulation. "
            "No invented words, no invented singing, and do not desync on sustained notes."
        )
    else:
        lipsync_block = (
            "Audio: do not generate vocals. The original audio track will be added in post. "
            "Do not animate speech-like mouth motion unless clearly motivated by the intended scene."
        )

    return (
        f"VIDEO CONCEPT: {label}\n"
        f"{description}\n\n"
        "Identity & continuity: use the provided reference image(s) as the exact identity and composition anchor. "
        "Preserve the same person and framing across segments; no resets.\n"
        "Text/artifacts: no subtitles, lyrics, captions, logos, watermarks, letters, numbers, UI overlays, or gibberish marks.\n"
        f"{lipsync_block}\n\n"
        f"Direction (global):\n{direction}\n\n"
        "Segment continuity: the first frame of each segment must seamlessly continue from the last frame of the previous segment."
    ).strip()


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
    username,
    job_id,
    audio_path,
    image_paths,
    lyrics_path,
    lyrics_text_input,
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
        lyrics_text = (lyrics_text_input or "").strip() or None
        if lyrics_text is None and lyrics_path and Path(lyrics_path).exists():
            lyrics_text = Path(lyrics_path).read_text(encoding="utf-8", errors="replace")

        if prompt_override:
            prompt_text = str(prompt_override)
        else:
            prompt_text = build_master_prompt(video_style, lip_sync_required)
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

    append_user_prompt_history(
        username,
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
        reused_cached_audio=result.get("reused_cached_audio", False),
        reused_image_asset=result.get("reused_image_asset", False),
        creative_assets=result.get("creative_assets"),
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
        "video_styles": [
            {
                "value": key,
                "label": preset.get("label", key),
                "description": preset.get("description", ""),
            }
            for key, preset in VIDEO_STYLE_PRESETS.items()
        ],
        "default_video_style": "cinematic_studio",
    }


@app.get("/api/models")
def models():
    return {"models": fetch_models_metadata()}


@app.post("/api/jobs", status_code=202)
def create_job(
    username: str = Depends(require_user),
    song: UploadFile = File(...),
    image: UploadFile = File(None),
    images: list[UploadFile] = File(None),
    lyrics: UploadFile = File(None),
    lyrics_text: str = Form(""),
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
    lyrics_text_value = (lyrics_text or "").strip()
    if lyrics_text_value:
        lyrics_name = "lyrics.txt"
        lyrics_path = uploads_dir / lyrics_name
        lyrics_path.write_text(lyrics_text_value + "\n", encoding="utf-8")

    primary_image_name = image_paths[0].name

    with jobs_lock:
        jobs[job_id] = {
            "id": job_id,
            "username": username,
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
            "reused_cached_audio": False,
            "reused_image_asset": False,
            "creative_assets": None,
        }

    thread = threading.Thread(
        target=run_job,
        args=(
            username,
            job_id,
            audio_path,
            image_paths,
            lyrics_path,
            lyrics_text_value,
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
def get_job(job_id: str, username: str = Depends(require_user)):
    with jobs_lock:
        job = jobs.get(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("username") != username:
        raise HTTPException(status_code=404, detail="Job not found")

    return serialize_job(job)


@app.get("/api/jobs/{job_id}/video")
def download_video(job_id: str, username: str = Depends(require_user)):
    with jobs_lock:
        job = jobs.get(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("username") != username:
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
