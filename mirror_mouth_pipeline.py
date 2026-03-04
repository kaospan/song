#!/usr/bin/env python3
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime

import requests
from dotenv import load_dotenv

# Content-agnostic prompt/segment packaging.
from generic_video_pipeline import (
    PipelineInputs as GenericPipelineInputs,
    PromptEngine as GenericPromptEngine,
    SegmentInfo as GenericSegmentInfo,
    VideoPipelineConfig as GenericVideoPipelineConfig,
    build_prompt_package as build_generic_prompt_package,
)

# =====================================================
# ENV
# =====================================================

load_dotenv()
load_dotenv(os.path.join(".venv", ".env"), override=True)


def normalize_env_path(value):
    if not value:
        return None

    value = value.strip().strip('"').strip("'")
    escape_map = str.maketrans(
        {
            "\a": r"\a",
            "\b": r"\b",
            "\f": r"\f",
            "\n": r"\n",
            "\r": r"\r",
            "\t": r"\t",
            "\v": r"\v",
        }
    )
    value = value.translate(escape_map)
    return os.path.normpath(value)


def _now_stamp():
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")


def _remove_dir_if_empty(path):
    if not path or not os.path.isdir(path):
        return
    try:
        with os.scandir(path) as it:
            for _ in it:
                return
        os.rmdir(path)
    except OSError:
        return


def sanitize_filename_component(value):
    if value is None:
        return None

    # Drop control chars entirely, then replace filesystem-unsafe characters.
    sanitized = re.sub(r"[\\x00-\\x1f]", "", value)
    sanitized = re.sub(r'[<>:"/\\\\|?*]', "_", sanitized)
    sanitized = re.sub(r"\\s+", " ", sanitized).strip()
    sanitized = sanitized.strip("._- ")
    return sanitized or None


def hash_file(path, chunk_size=1024 * 1024):
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def sha256_text(value):
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def prepend_tool_dir_to_path(*tool_paths):
    tool_dirs = []
    for tool_path in tool_paths:
        if tool_path and os.path.exists(tool_path):
            tool_dir = os.path.dirname(tool_path)
            if tool_dir and tool_dir not in tool_dirs:
                tool_dirs.append(tool_dir)

    if tool_dirs:
        current_path = os.environ.get("PATH", "")
        os.environ["PATH"] = os.pathsep.join(tool_dirs + [current_path])


FFMPEG_PATH = normalize_env_path(os.environ.get("FFMPEG_PATH"))
FFPROBE_PATH = normalize_env_path(os.environ.get("FFPROBE_PATH"))
prepend_tool_dir_to_path(FFMPEG_PATH, FFPROBE_PATH)

from pydub import AudioSegment

if FFMPEG_PATH:
    AudioSegment.converter = FFMPEG_PATH
    AudioSegment.ffmpeg = FFMPEG_PATH

# =====================================================
# CONFIG
# =====================================================

INPUT_MP3 = normalize_env_path(os.environ.get("INPUT_MP3")) or "full_song.mp3"
REFERENCE_IMAGE = normalize_env_path(os.environ.get("REFERENCE_IMAGE")) or "reference_image.png"
REFERENCE_IMAGES = [
    normalize_env_path(value)
    for value in os.environ.get("REFERENCE_IMAGES", "").split(",")
    if normalize_env_path(value)
]
SEGMENT_LENGTH_SECONDS = int(os.environ.get("SEGMENT_LENGTH_SECONDS", "8"))
OUTPUT_AUDIO_FOLDER = normalize_env_path(os.environ.get("OUTPUT_AUDIO_FOLDER")) or "segments"
OUTPUT_VIDEO_FOLDER = normalize_env_path(os.environ.get("OUTPUT_VIDEO_FOLDER")) or "video_segments"
FINAL_VIDEO_NAME = normalize_env_path(os.environ.get("FINAL_VIDEO_NAME")) or "final_music_video.mp4"
FINAL_VIDEO_ARCHIVE_FOLDER = (
    normalize_env_path(os.environ.get("FINAL_VIDEO_ARCHIVE_FOLDER")) or "final_videos"
)
PROMPT_FILE = normalize_env_path(os.environ.get("PROMPT_FILE")) or "mirror_mouth_prompt.txt"
LYRICS_FILE = normalize_env_path(os.environ.get("LYRICS_FILE"))
LYRICS_SCENE_MODE = os.environ.get("LYRICS_SCENE_MODE", "1").strip().lower() in ("1", "true", "yes")
LYRICS_MAX_LINES_PER_SEGMENT = int(os.environ.get("LYRICS_MAX_LINES_PER_SEGMENT", "4"))
HEDRA_MODEL_NAME = os.environ.get("HEDRA_MODEL_NAME", "Kling AI Avatar v2 Standard").strip()
DEFAULT_LIPSYNC_MODEL_NAME = os.environ.get(
    "DEFAULT_LIPSYNC_MODEL_NAME", "Kling AI Avatar v2 Standard"
).strip()
DEFAULT_NON_LIPSYNC_MODEL_NAME = os.environ.get("DEFAULT_NON_LIPSYNC_MODEL_NAME", "").strip()
LIP_SYNC_REQUIRED = os.environ.get("LIP_SYNC_REQUIRED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
)
SONG_TITLE = os.environ.get("SONG_TITLE")
SONG_ARTIST = os.environ.get("SONG_ARTIST")
IMAGE_ASSET_CACHE_PATH = (
    normalize_env_path(os.environ.get("IMAGE_ASSET_CACHE_PATH")) or "job_runs/asset_cache.json"
)
PROMPT_CACHE_ROOT = normalize_env_path(os.environ.get("PROMPT_CACHE_ROOT")) or "job_runs/prompt_cache"
AUDIO_CACHE_ROOT = normalize_env_path(os.environ.get("AUDIO_CACHE_ROOT"))
REUSE_IMAGE_ASSET = os.environ.get("REUSE_IMAGE_ASSET", "1").strip().lower() not in ("0", "false", "no")
FORCE_IMAGE_UPLOAD = os.environ.get("FORCE_IMAGE_UPLOAD", "0").strip().lower() in ("1", "true", "yes")
REUSE_AUDIO_ASSET = os.environ.get("REUSE_AUDIO_ASSET", "1").strip().lower() not in ("0", "false", "no")
FORCE_AUDIO_UPLOAD = os.environ.get("FORCE_AUDIO_UPLOAD", "0").strip().lower() in ("1", "true", "yes")
REUSE_COMPLETE_SESSIONS = os.environ.get("REUSE_COMPLETE_SESSIONS", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)
REUSE_SEGMENTS_HARDLINK = os.environ.get("REUSE_SEGMENTS_HARDLINK", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)
REQUEST_DELAY_SECONDS = float(os.environ.get("REQUEST_DELAY_SECONDS", "60"))
STATUS_POLL_INTERVAL_SECONDS = float(os.environ.get("STATUS_POLL_INTERVAL_SECONDS", "10"))
QUEUED_STATUS_SLEEP_SECONDS = float(os.environ.get("QUEUED_STATUS_SLEEP_SECONDS", "120"))
STATUS_TIMEOUT_SECONDS = float(os.environ.get("STATUS_TIMEOUT_SECONDS", "60"))
STATUS_RETRY_LIMIT = int(os.environ.get("STATUS_RETRY_LIMIT", "10"))
STATUS_RETRY_BACKOFF_SECONDS = float(os.environ.get("STATUS_RETRY_BACKOFF_SECONDS", "10"))
REQUEST_RETRY_LIMIT = int(os.environ.get("REQUEST_RETRY_LIMIT", "5"))
REQUEST_RETRY_BACKOFF_SECONDS = float(os.environ.get("REQUEST_RETRY_BACKOFF_SECONDS", "5"))
DOWNLOAD_TIMEOUT_SECONDS = float(os.environ.get("DOWNLOAD_TIMEOUT_SECONDS", "120"))
DOWNLOAD_RETRY_LIMIT = int(os.environ.get("DOWNLOAD_RETRY_LIMIT", "5"))
DOWNLOAD_RETRY_BACKOFF_SECONDS = float(os.environ.get("DOWNLOAD_RETRY_BACKOFF_SECONDS", "10"))
PROMPT_CHAR_LIMIT = int(os.environ.get("PROMPT_CHAR_LIMIT", "2500"))
RESUME_SESSION = normalize_env_path(os.environ.get("RESUME_SESSION"))
SESSION_LABEL = os.environ.get("SESSION_LABEL")

API_BASE = "https://api.hedra.com/web-app/public"
API_KEY = os.environ.get("API_KEY")
_thread_local = threading.local()


def http_session():
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        _thread_local.session = session
    return session


def get_headers(content_type=None):
    if not API_KEY:
        raise ValueError("API_KEY environment variable not set")

    headers = {
        "X-API-Key": API_KEY,
        "Authorization": f"Bearer {API_KEY}",
    }
    if content_type:
        headers["Content-Type"] = content_type
    return headers


def _is_presigned_s3_url(url):
    if not url:
        return False
    from urllib.parse import urlparse, parse_qs

    parsed = urlparse(url)
    params = {k.lower(): v for k, v in parse_qs(parsed.query).items()}
    presign_keys = {
        "x-amz-algorithm",
        "x-amz-signature",
        "x-amz-credential",
        "x-amz-security-token",
        "awsaccesskeyid",
        "signature",
    }
    return bool(presign_keys & set(params.keys()))


def raise_with_body(resp: requests.Response):
    try:
        body = resp.text
    except Exception:
        body = "<unreadable body>"
    print(f"Request failed: {resp.status_code} {body}")
    resp.raise_for_status()


def load_prompt_text(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Prompt file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        prompt_text = f.read().strip()

    if not prompt_text:
        raise ValueError(f"Prompt file is empty: {path}")

    return validate_prompt_text(prompt_text)


def load_lyrics_text(path):
    if not path:
        return None
    if not os.path.exists(path):
        raise FileNotFoundError(f"Lyrics file not found: {path}")
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read().strip()
    return text or None


class StaticMasterPromptEngine(GenericPromptEngine):
    """
    Uses a provided master prompt verbatim and generates per-segment prompts via config templates.
    This avoids format-string collisions with curly braces inside the user prompt.
    """

    def __init__(self, config: GenericVideoPipelineConfig, master_prompt_text: str):
        super().__init__(config)
        self._master_prompt_text = (master_prompt_text or "").strip()

    def build_master_prompt(self, context):
        return self._master_prompt_text


def format_time_range(idx, segment_length_seconds):
    start = idx * segment_length_seconds
    end = (idx + 1) * segment_length_seconds
    return f"{start:02d}s-{end:02d}s"


def validate_prompt_text(prompt_text):
    prompt_text = (prompt_text or "").strip()
    if not prompt_text:
        raise ValueError("Prompt text is empty.")
    if len(prompt_text) > PROMPT_CHAR_LIMIT:
        raise ValueError(
            f"Prompt file exceeds {PROMPT_CHAR_LIMIT} characters: {len(prompt_text)}"
        )

    return prompt_text


def load_json_file(path):
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def write_json_file(path, payload):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def build_segment_infos_from_durations_ms(durations_ms):
    segments = []
    cursor = 0
    for idx, dur in enumerate(durations_ms):
        dur = max(0, int(dur))
        start = cursor
        end = cursor + dur
        segments.append(GenericSegmentInfo(index=idx, start_ms=start, end_ms=end))
        cursor = end
    return segments


def write_text_file(path, text):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write((text or "").rstrip() + "\n")


@dataclass(frozen=True)
class PromptAssets:
    run_dir: str
    master_prompt_path: str | None
    script_path: str | None
    timestamps_path: str
    segments_path: str
    prompt_bundle_path: str | None


def _prompt_cache_fingerprint(
    master_prompt_text,
    lyrics_text,
    segment_length_seconds,
    segment_durations_ms,
    song_title,
    song_artist,
    input_mp3_hash,
    segment_prompt_template,
    lyrics_max_lines_per_segment,
):
    payload = {
        "master_prompt_text": master_prompt_text or "",
        "lyrics_text": lyrics_text or "",
        "segment_length_seconds": float(segment_length_seconds),
        "segment_durations_ms": [int(ms) for ms in (segment_durations_ms or [])],
        "song_title": song_title or "",
        "song_artist": song_artist or "",
        "input_mp3_hash": input_mp3_hash or "",
        "segment_prompt_template": segment_prompt_template or "",
        "lyrics_max_lines_per_segment": int(lyrics_max_lines_per_segment),
    }
    return sha256_text(json.dumps(payload, sort_keys=True))


def _prompt_cache_dir(prompt_cache_root, output_stem, fingerprint):
    return os.path.join(prompt_cache_root, output_stem, fingerprint)


def _prompt_cache_valid(cache_dir):
    required = ["segments.json", "timestamps.json", "prompt_bundle.json"]
    return all(os.path.exists(os.path.join(cache_dir, name)) for name in required)


def _prompt_assets_from_dir(run_dir):
    def _path(name):
        path = os.path.join(run_dir, name)
        return os.path.abspath(path) if os.path.exists(path) else None

    return PromptAssets(
        run_dir=os.path.abspath(run_dir),
        master_prompt_path=_path("master_prompt.txt"),
        script_path=_path("script.txt"),
        timestamps_path=os.path.abspath(os.path.join(run_dir, "timestamps.json")),
        segments_path=os.path.abspath(os.path.join(run_dir, "segments.json")),
        prompt_bundle_path=_path("prompt_bundle.json"),
    )


def build_or_load_prompt_assets(
    *,
    prompt_cache_root,
    output_stem,
    master_prompt_text,
    lyrics_text,
    segment_length_seconds,
    segment_durations_ms,
    song_title,
    song_artist,
    input_mp3_hash,
    segment_prompt_template,
    lyrics_max_lines_per_segment,
    segments_override,
):
    if not prompt_cache_root:
        prompt_cache_root = None

    fingerprint = _prompt_cache_fingerprint(
        master_prompt_text=master_prompt_text,
        lyrics_text=lyrics_text,
        segment_length_seconds=segment_length_seconds,
        segment_durations_ms=segment_durations_ms,
        song_title=song_title,
        song_artist=song_artist,
        input_mp3_hash=input_mp3_hash,
        segment_prompt_template=segment_prompt_template,
        lyrics_max_lines_per_segment=lyrics_max_lines_per_segment,
    )

    cache_dir = None
    if prompt_cache_root:
        cache_dir = _prompt_cache_dir(prompt_cache_root, output_stem, fingerprint)
        if _prompt_cache_valid(cache_dir):
            return _prompt_assets_from_dir(cache_dir)

    run_dir = cache_dir or os.path.join("runs", _now_stamp())
    os.makedirs(run_dir, exist_ok=True)

    generic_config = GenericVideoPipelineConfig(
        segment_duration_seconds=float(segment_length_seconds),
        narrative_mode="music_video",
        script_max_lines_per_segment=max(1, int(lyrics_max_lines_per_segment)),
        segment_prompt_template=segment_prompt_template,
        export_prompt_bundle=True,
    )
    generic_engine = StaticMasterPromptEngine(generic_config, master_prompt_text)
    generic_outputs = build_generic_prompt_package(
        GenericPipelineInputs(
            media_path=None,
            script_text=lyrics_text or "",
            metadata={
                "song_title": song_title or "",
                "song_artist": song_artist or "",
            },
        ),
        config=generic_config,
        engine=generic_engine,
        run_dir=run_dir,
        segments_override=segments_override,
        media_duration_ms_override=sum(segment_durations_ms or []),
        media_sha256_override=input_mp3_hash,
    )

    if cache_dir:
        write_json_file(
            os.path.join(cache_dir, "prompt_manifest.json"),
            {
                "fingerprint": fingerprint,
                "created_at": datetime.now().isoformat(),
                "output_stem": output_stem,
                "segment_count": len(segment_durations_ms or []),
            },
        )

    return PromptAssets(
        run_dir=generic_outputs.run_dir,
        master_prompt_path=generic_outputs.master_prompt_path,
        script_path=generic_outputs.script_path,
        timestamps_path=generic_outputs.timestamps_path,
        segments_path=generic_outputs.segments_path,
        prompt_bundle_path=generic_outputs.prompt_bundle_path,
    )


def sync_prompt_assets_to_session(session_video_folder, prompt_assets):
    prompt_assets_dir = os.path.join(session_video_folder, "creative")
    os.makedirs(prompt_assets_dir, exist_ok=True)

    for filename in [
        "master_prompt.txt",
        "script.txt",
        "timestamps.json",
        "segments.json",
        "prompt_bundle.json",
    ]:
        src = os.path.join(prompt_assets.run_dir, filename)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(prompt_assets_dir, filename))

    return prompt_assets_dir
def fetch_models():
    url = f"{API_BASE}/models"
    resp = http_session().get(url, headers=get_headers(), timeout=30)
    if not resp.ok:
        raise_with_body(resp)
    return resp.json()


def pick_fallback_model(models, require_audio_input):
    normalized = [
        {**model, "_name": str(model.get("name", "")).strip().lower()}
        for model in models
        if model.get("type") == "video"
    ]

    if require_audio_input:
        preferred = [
            "kling ai avatar v2 standard",
            "kling ai avatar v2 pro",
            "hedra avatar",
        ]
        for pref in preferred:
            for model in normalized:
                if model.get("requires_audio_input") and model["_name"] == pref:
                    return model
        for model in normalized:
            if model.get("requires_audio_input") and "avatar" in model["_name"]:
                return model
        for model in normalized:
            if model.get("requires_audio_input"):
                return model
        return None

    preferred = [
        "kling 1.6 i2v",
        "kling o3 standard i2v",
        "kling v3 standard i2v",
    ]
    for pref in preferred:
        for model in normalized:
            if not model.get("requires_audio_input") and model["_name"] == pref:
                return model
    for model in normalized:
        if not model.get("requires_audio_input") and "kling" in model["_name"] and "i2v" in model["_name"]:
            return model
    for model in normalized:
        if not model.get("requires_audio_input"):
            return model
    return None


def resolve_model(model_name, require_audio_input=True, models=None):
    models = models or fetch_models()
    normalized_name = model_name.strip().lower()

    for model in models:
        if str(model.get("name", "")).strip().lower() == normalized_name:
            if require_audio_input and not model.get("requires_audio_input"):
                audio_model_names = sorted(
                    m.get("name", "<unnamed>")
                    for m in models
                    if m.get("type") == "video" and m.get("requires_audio_input")
                )
                raise ValueError(
                    f"Selected model '{model_name}' does not accept audio input, so it will not use your MP3 segments. "
                    f"Use an audio-capable model instead. Available audio-capable video models: {', '.join(audio_model_names)}"
                )
            return model

    available_names = ", ".join(sorted(model.get("name", "<unnamed>") for model in models))
    raise ValueError(
        f"HEDRA_MODEL_NAME '{model_name}' not found. Available models: {available_names}"
    )


def create_asset_record(name, type_="image"):
    url = f"{API_BASE}/assets"
    payload = {"name": name, "type": type_}
    headers = get_headers("application/json")

    resp = http_session().post(url, headers=headers, json=payload, timeout=30)
    if not resp.ok:
        raise_with_body(resp)

    return resp.json()


def upload_file_to_asset(asset_id, path, mime="application/octet-stream"):
    url = f"{API_BASE}/assets/{asset_id}/upload"

    with open(path, "rb") as f:
        files = {"file": (os.path.basename(path), f, mime)}
        resp = http_session().post(url, headers=get_headers(), files=files, timeout=120)

    if not resp.ok:
        raise_with_body(resp)

    return resp.json()


def upload_asset(path, name=None, mime=None, type_="image"):
    name = name or os.path.basename(path)

    if mime is None:
        if type_ == "image":
            mime = "image/png"
        elif type_ == "audio":
            mime = "audio/mpeg"
        else:
            mime = "application/octet-stream"

    print(f"Creating asset record for: {name}")
    asset_resp = create_asset_record(name, type_=type_)
    asset_id = asset_resp.get("id")
    upload_url = asset_resp.get("upload_url")

    if not asset_id:
        raise Exception(f"No asset id returned: {asset_resp}")

    if upload_url:
        print(f"Uploading to presigned URL for asset {asset_id}")
        with open(path, "rb") as f:
            put_resp = http_session().put(upload_url, data=f, timeout=120)
        if not put_resp.ok:
            raise_with_body(put_resp)
    else:
        print(f"Uploading via Hedra endpoint for asset {asset_id}")
        upload_file_to_asset(asset_id, path, mime=mime)

    return asset_id


def build_session_video_folder(output_video_root, session_label=None, resume_session=None):
    os.makedirs(output_video_root, exist_ok=True)

    if resume_session:
        normalized_resume = os.path.normpath(resume_session)
        normalized_output_root = os.path.normpath(output_video_root)

        if os.path.isabs(normalized_resume):
            session_folder = normalized_resume
        elif (
            normalized_resume == normalized_output_root
            or normalized_resume.startswith(normalized_output_root + os.sep)
            or os.path.exists(normalized_resume)
        ):
            session_folder = normalized_resume
        else:
            session_folder = os.path.join(output_video_root, normalized_resume)
        os.makedirs(session_folder, exist_ok=True)
        return session_folder

    session_name = session_label or datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
    session_folder = os.path.join(output_video_root, session_name)
    os.makedirs(session_folder, exist_ok=True)
    return session_folder


def get_latest_session_folder(output_video_root):
    if not os.path.isdir(output_video_root):
        return None

    session_dirs = []
    for entry in os.scandir(output_video_root):
        if entry.is_dir():
            session_dirs.append(entry)

    if not session_dirs:
        return None

    latest = max(session_dirs, key=lambda d: d.stat().st_mtime)
    return latest.path


def read_run_manifest(session_folder):
    return load_json_file(os.path.join(session_folder, "run_manifest.json")) or {}


def write_run_manifest(session_folder, manifest):
    write_json_file(os.path.join(session_folder, "run_manifest.json"), manifest)


def list_session_folders(output_video_root):
    if not os.path.isdir(output_video_root):
        return []
    entries = [entry for entry in os.scandir(output_video_root) if entry.is_dir()]
    entries.sort(key=lambda entry: entry.stat().st_mtime, reverse=True)
    return [entry.path for entry in entries]


def link_or_copy(src, dst, prefer_hardlink=True):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if os.path.exists(dst) and os.path.getsize(dst) > 0:
        return
    if prefer_hardlink:
        try:
            os.link(src, dst)
            return
        except OSError:
            pass
    shutil.copy2(src, dst)


def reuse_complete_session_segments(output_video_root, session_video_folder, fingerprint, segment_count):
    if not REUSE_COMPLETE_SESSIONS:
        return False

    for candidate in list_session_folders(output_video_root):
        if os.path.normpath(candidate) == os.path.normpath(session_video_folder):
            continue
        candidate_manifest = read_run_manifest(candidate)
        if not candidate_manifest:
            continue
        if candidate_manifest.get("fingerprint") != fingerprint:
            continue
        if not has_all_video_segments(candidate, segment_count):
            continue

        print("Reusing completed segments from:", candidate)
        for idx in range(segment_count):
            src = os.path.join(candidate, f"video_{idx:03}.mp4")
            dst = os.path.join(session_video_folder, f"video_{idx:03}.mp4")
            link_or_copy(src, dst, prefer_hardlink=REUSE_SEGMENTS_HARDLINK)
        return True

    return False


def build_timestamped_final_video_name(final_video_name):
    base_dir = os.path.dirname(final_video_name)
    base_name = os.path.basename(final_video_name)
    stem, ext = os.path.splitext(base_name)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    if not ext:
        ext = ".mp4"

    timestamped_name = f"{stem}_{timestamp}{ext}"
    return os.path.join(base_dir, timestamped_name) if base_dir else timestamped_name


def build_output_stem(song_title, song_artist, fallback_stem):
    title = sanitize_filename_component(song_title)
    artist = sanitize_filename_component(song_artist)
    if title and artist:
        return f"{title} - {artist}"
    if title:
        return title
    if artist:
        return artist
    return sanitize_filename_component(fallback_stem) or fallback_stem


def build_named_final_video_path(final_video_name, song_title=None, song_artist=None):
    base_dir = os.path.dirname(final_video_name)
    base_name = os.path.basename(final_video_name)
    stem, ext = os.path.splitext(base_name)
    if not ext:
        ext = ".mp4"

    output_stem = build_output_stem(song_title, song_artist, stem)
    named = f"{output_stem}{ext}"
    return os.path.join(base_dir, named) if base_dir else named


def save_versioned_video_copy(final_video_path, input_mp3, archive_folder, song_title=None, song_artist=None):
    os.makedirs(archive_folder, exist_ok=True)

    base_title = sanitize_filename_component(song_title) or os.path.splitext(os.path.basename(input_mp3))[0]
    artist = sanitize_filename_component(song_artist)
    if artist:
        base_title = f"{base_title} - {artist}"
    _, ext = os.path.splitext(final_video_path)
    if not ext:
        ext = ".mp4"

    version_pattern = re.compile(rf"^{re.escape(base_title)}\.v(\d+){re.escape(ext)}$")
    next_version = 1

    for existing_name in os.listdir(archive_folder):
        match = version_pattern.match(existing_name)
        if match:
            next_version = max(next_version, int(match.group(1)) + 1)

    versioned_filename = f"{base_title}.v{next_version}{ext}"
    versioned_path = os.path.join(archive_folder, versioned_filename)
    shutil.copy2(final_video_path, versioned_path)
    return versioned_path


def archive_run_inputs(run_folder, input_mp3, reference_images):
    inputs_dir = os.path.join(run_folder, "inputs")
    os.makedirs(inputs_dir, exist_ok=True)

    shutil.copy2(input_mp3, os.path.join(inputs_dir, os.path.basename(input_mp3)))
    images_dir = os.path.join(inputs_dir, "reference_images")
    os.makedirs(images_dir, exist_ok=True)
    for path in reference_images:
        shutil.copy2(path, os.path.join(images_dir, os.path.basename(path)))


def archive_run_outputs(run_folder, final_video_path):
    os.makedirs(run_folder, exist_ok=True)
    target = os.path.join(run_folder, os.path.basename(final_video_path))
    if os.path.normpath(target) == os.path.normpath(final_video_path):
        return target
    shutil.copy2(final_video_path, target)
    return target


def load_cached_audio_segments(output_audio_folder, input_mp3_hash, segment_length_seconds):
    manifest_path = os.path.join(output_audio_folder, "segments_manifest.json")
    manifest = load_json_file(manifest_path)
    if not manifest:
        return None

    if manifest.get("input_mp3_hash") != input_mp3_hash:
        return None
    if manifest.get("segment_length_seconds") != segment_length_seconds:
        return None

    segments = manifest.get("segments", [])
    if not segments:
        return None

    audio_files = []
    for segment in segments:
        segment_file = segment.get("file")
        duration_ms = segment.get("duration_ms")
        if not segment_file or duration_ms is None:
            return None

        segment_path = os.path.join(output_audio_folder, segment_file)
        if not os.path.exists(segment_path) or os.path.getsize(segment_path) == 0:
            return None
        audio_files.append((segment_path, duration_ms))

    print("Using cached audio segments from:", output_audio_folder)
    return audio_files


def write_segments_manifest(output_audio_folder, input_mp3, input_mp3_hash, segment_length_seconds, audio_files):
    manifest_path = os.path.join(output_audio_folder, "segments_manifest.json")
    manifest = {
        "input_mp3": os.path.basename(input_mp3),
        "input_mp3_hash": input_mp3_hash,
        "segment_length_seconds": segment_length_seconds,
        "segment_count": len(audio_files),
        "segments": [
            {"file": os.path.basename(path), "duration_ms": duration_ms}
            for path, duration_ms in audio_files
        ],
        "created_at": datetime.now().isoformat(),
    }
    write_json_file(manifest_path, manifest)


def split_audio(input_mp3, output_audio_folder, segment_length_seconds):
    os.makedirs(output_audio_folder, exist_ok=True)

    print("Splitting audio...")
    audio = AudioSegment.from_mp3(input_mp3)
    duration_ms = len(audio)
    segment_length_ms = segment_length_seconds * 1000
    num_segments = math.ceil(duration_ms / segment_length_ms)

    audio_files = []

    for i in range(num_segments):
        start = i * segment_length_ms
        end = min((i + 1) * segment_length_ms, duration_ms)

        segment = audio[start:end]
        filename = os.path.join(output_audio_folder, f"segment{i:03}.mp3")
        segment.export(filename, format="mp3", bitrate="320k")
        audio_files.append((filename, len(segment)))

    print(f"{len(audio_files)} segments created.")
    return audio_files


def get_expected_video_paths(session_video_folder, segment_count):
    return [
        os.path.join(session_video_folder, f"video_{idx:03}.mp4")
        for idx in range(segment_count)
    ]


def has_all_video_segments(session_video_folder, segment_count):
    for idx in range(segment_count):
        video_path = os.path.join(session_video_folder, f"video_{idx:03}.mp4")
        if not os.path.exists(video_path) or os.path.getsize(video_path) == 0:
            return False
    return True


def load_asset_cache(cache_path):
    cache = load_json_file(cache_path)
    if not cache:
        return {"images": {}, "audio": {}}
    if "images" not in cache or not isinstance(cache["images"], dict):
        cache["images"] = {}
    if "audio" not in cache or not isinstance(cache["audio"], dict):
        cache["audio"] = {}
    return cache


def save_asset_cache(cache_path, cache):
    write_json_file(cache_path, cache)


def load_segment_jobs(session_video_folder):
    return load_json_file(os.path.join(session_video_folder, "segment_jobs.json")) or {}


def save_segment_jobs(session_video_folder, segment_jobs):
    write_json_file(os.path.join(session_video_folder, "segment_jobs.json"), segment_jobs)


def save_lyrics_segments(session_video_folder, segment_length_seconds, lyrics_by_segment):
    payload = {
        "segment_length_seconds": segment_length_seconds,
        "segments": [
            {"index": idx, "time_range": format_time_range(idx, segment_length_seconds), "lyrics": lyrics}
            for idx, lyrics in enumerate(lyrics_by_segment)
            if lyrics
        ],
        "created_at": datetime.now().isoformat(),
    }
    write_json_file(os.path.join(session_video_folder, "lyrics_segments.json"), payload)


def stitch_video(video_files, manifest_path, final_video_name, audio_track_path=None):
    ffmpeg_bin = FFMPEG_PATH or "ffmpeg"

    with open(manifest_path, "w", encoding="utf-8") as f:
        for video_file in video_files:
            f.write(f"file '{os.path.abspath(video_file)}'\n")

    def _run(cmd, label):
        print(label)
        subprocess.run(cmd, check=True)

    concat_input = [
        ffmpeg_bin,
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        manifest_path,
    ]

    if audio_track_path:
        fast_cmd = concat_input + [
            "-i",
            audio_track_path,
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-shortest",
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-movflags",
            "+faststart",
            final_video_name,
        ]
        slow_cmd = concat_input + [
            "-i",
            audio_track_path,
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-shortest",
            "-c:v",
            "libx264",
            "-crf",
            "23",
            "-preset",
            "medium",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-movflags",
            "+faststart",
            final_video_name,
        ]
    else:
        fast_cmd = concat_input + [
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            final_video_name,
        ]
        slow_cmd = concat_input + [
            "-c:v",
            "libx264",
            "-crf",
            "23",
            "-preset",
            "medium",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-movflags",
            "+faststart",
            final_video_name,
        ]

    try:
        _run(fast_cmd, "Running ffmpeg (fast concat)...")
    except subprocess.CalledProcessError:
        _run(slow_cmd, "Fast concat failed; re-encoding...")


def extract_last_frame(video_path, frame_path):
    ffmpeg_bin = FFMPEG_PATH or "ffmpeg"
    os.makedirs(os.path.dirname(frame_path), exist_ok=True)

    ffmpeg_cmd = [
        ffmpeg_bin,
        "-y",
        "-sseof",
        "-0.10",
        "-i",
        video_path,
        "-update",
        "1",
        "-frames:v",
        "1",
        frame_path,
    ]

    subprocess.run(ffmpeg_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def upload_continuity_frame(video_path, frame_index, session_video_folder):
    continuity_folder = os.path.join(session_video_folder, "continuity_frames")
    frame_path = os.path.join(continuity_folder, f"frame_{frame_index:03}.png")

    print("Extracting continuity frame:", frame_path)
    extract_last_frame(video_path, frame_path)

    print("Uploading continuity frame:", frame_path)
    return upload_asset(
        frame_path,
        name=os.path.basename(frame_path),
        mime="image/png",
        type_="image",
    )


def poll_generation_status(status_url):
    consecutive_failures = 0

    while True:
        try:
            status_resp = http_session().get(
                status_url,
                headers=get_headers(),
                timeout=STATUS_TIMEOUT_SECONDS,
            )
        except requests.exceptions.RequestException as exc:
            consecutive_failures += 1
            if consecutive_failures > STATUS_RETRY_LIMIT:
                raise RuntimeError(
                    f"Status polling failed {consecutive_failures} times in a row: {exc}"
                ) from exc

            retry_delay = min(
                STATUS_RETRY_BACKOFF_SECONDS * consecutive_failures,
                60,
            )
            print(
                "Status poll failed "
                f"({consecutive_failures}/{STATUS_RETRY_LIMIT}): {exc}. "
                f"Retrying in {retry_delay} seconds..."
            )
            time.sleep(retry_delay)
            continue

        status_resp.raise_for_status()
        status_json = status_resp.json()

        consecutive_failures = 0
        status_json = status_resp.json()
        status = status_json.get("status")
        print("Status:", status)

        if status == "complete":
            return status_json
        if status in ("failed", "error"):
            raise Exception(f"Generation failed: {json.dumps(status_json, indent=2)}")

        if status == "queued":
            print(f"Queued. Sleeping {QUEUED_STATUS_SLEEP_SECONDS} seconds before retrying...")
            time.sleep(QUEUED_STATUS_SLEEP_SECONDS)
        else:
            time.sleep(STATUS_POLL_INTERVAL_SECONDS)


def download_video_file(video_url, video_path):
    temp_path = f"{video_path}.part"
    download_headers = {} if _is_presigned_s3_url(video_url) else get_headers()

    for attempt in range(1, DOWNLOAD_RETRY_LIMIT + 1):
        try:
            with http_session().get(
                video_url,
                headers=download_headers,
                stream=True,
                timeout=DOWNLOAD_TIMEOUT_SECONDS,
            ) as vid_resp:
                if 400 <= vid_resp.status_code < 500:
                    raise_with_body(vid_resp)
                if not vid_resp.ok:
                    raise requests.HTTPError(
                        f"Unexpected status {vid_resp.status_code} while downloading video",
                        response=vid_resp,
                    )

                with open(temp_path, "wb") as f:
                    for chunk in vid_resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

            os.replace(temp_path, video_path)
            return
        except requests.exceptions.RequestException as exc:
            if os.path.exists(temp_path):
                os.remove(temp_path)

            if attempt >= DOWNLOAD_RETRY_LIMIT:
                raise RuntimeError(
                    f"Video download failed after {DOWNLOAD_RETRY_LIMIT} attempts: {exc}"
                ) from exc

            retry_delay = min(DOWNLOAD_RETRY_BACKOFF_SECONDS * attempt, 60)
            print(
                "Video download failed "
                f"({attempt}/{DOWNLOAD_RETRY_LIMIT}): {exc}. "
                f"Retrying in {retry_delay} seconds..."
            )
            time.sleep(retry_delay)


def run_pipeline(
    input_mp3=INPUT_MP3,
    reference_image=REFERENCE_IMAGE,
    reference_images=None,
    segment_length_seconds=SEGMENT_LENGTH_SECONDS,
    output_audio_folder=OUTPUT_AUDIO_FOLDER,
    output_video_root=OUTPUT_VIDEO_FOLDER,
    final_video_name=FINAL_VIDEO_NAME,
    final_video_archive_folder=FINAL_VIDEO_ARCHIVE_FOLDER,
    prompt_file=PROMPT_FILE,
    model_name=HEDRA_MODEL_NAME,
    request_delay_seconds=REQUEST_DELAY_SECONDS,
    resume_session=RESUME_SESSION,
    session_label=SESSION_LABEL,
    song_title=SONG_TITLE,
    song_artist=SONG_ARTIST,
    audio_cache_root=AUDIO_CACHE_ROOT,
    image_asset_cache_path=IMAGE_ASSET_CACHE_PATH,
    reuse_image_asset=REUSE_IMAGE_ASSET,
    force_image_upload=FORCE_IMAGE_UPLOAD,
    reuse_audio_asset=REUSE_AUDIO_ASSET,
    force_audio_upload=FORCE_AUDIO_UPLOAD,
    prompt_override=None,
    lyrics_text=None,
    lyrics_file=LYRICS_FILE,
    lyrics_scene_mode=LYRICS_SCENE_MODE,
    lyrics_max_lines_per_segment=LYRICS_MAX_LINES_PER_SEGMENT,
    lip_sync_required=LIP_SYNC_REQUIRED,
    default_lipsync_model_name=DEFAULT_LIPSYNC_MODEL_NAME,
    default_non_lipsync_model_name=DEFAULT_NON_LIPSYNC_MODEL_NAME,
):
    completed_successfully = False
    run_archive_dir = None
    try:
        if not os.path.exists(input_mp3):
            raise FileNotFoundError(f"Input MP3 not found: {input_mp3}")
        if reference_images is None:
            reference_images = REFERENCE_IMAGES or ([reference_image] if reference_image else [])
        reference_images = [path for path in reference_images if path]
        if not reference_images:
            raise ValueError("No reference images provided.")
        missing = [path for path in reference_images if not os.path.exists(path)]
        if missing:
            raise FileNotFoundError(f"Reference image(s) not found: {', '.join(missing)}")

        input_mp3_hash = hash_file(input_mp3)
        output_stem = build_output_stem(
            song_title,
            song_artist,
            os.path.splitext(os.path.basename(input_mp3))[0],
        )
        if audio_cache_root:
            output_audio_folder = os.path.join(audio_cache_root, output_stem, input_mp3_hash)
        else:
            output_audio_folder = os.path.join(output_audio_folder, output_stem, input_mp3_hash)

        # Keep local runs organized by title. API server passes per-job output roots and won't hit this.
        if output_video_root == OUTPUT_VIDEO_FOLDER:
            output_video_root = os.path.join(output_video_root, output_stem)
        if final_video_archive_folder == FINAL_VIDEO_ARCHIVE_FOLDER:
            final_video_archive_folder = os.path.join(final_video_archive_folder, output_stem)

        run_stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
        run_archive_dir = (
            os.path.join(final_video_archive_folder, "runs", run_stamp)
            if final_video_archive_folder
            else None
        )
        if run_archive_dir:
            os.makedirs(run_archive_dir, exist_ok=True)

        session_video_folder = build_session_video_folder(
            output_video_root,
            session_label=session_label,
            resume_session=resume_session,
        )
        final_video_name = build_named_final_video_path(final_video_name, song_title, song_artist)
        final_video_name = build_timestamped_final_video_name(final_video_name)
        final_video_dir = os.path.dirname(final_video_name)
        if not final_video_dir and run_archive_dir:
            final_video_dir = run_archive_dir
            final_video_name = os.path.join(final_video_dir, os.path.basename(final_video_name))
        videos_manifest_path = os.path.join(session_video_folder, "videos.txt")

        if resume_session:
            print("Resuming session folder:", session_video_folder)
        else:
            print("Session video folder:", session_video_folder)

        audio_files = load_cached_audio_segments(
            output_audio_folder,
            input_mp3_hash,
            segment_length_seconds,
        )
        reused_cached_audio = bool(audio_files)
        if not audio_files:
            audio_files = split_audio(input_mp3, output_audio_folder, segment_length_seconds)
            write_segments_manifest(
                output_audio_folder,
                input_mp3,
                input_mp3_hash,
                segment_length_seconds,
                audio_files,
            )
        segment_count = len(audio_files)

        if lyrics_text is None:
            lyrics_text = load_lyrics_text(lyrics_file)

        # MASTER_PROMPT is the locked style template. Segment prompts are derived from lyrics and written
        # to disk so a full run can be stitched without touching Hedra again.
        master_prompt_text = (
            validate_prompt_text(prompt_override) if prompt_override else load_prompt_text(prompt_file)
        )
        segment_durations_ms = [dur for _, dur in audio_files]
        segments_override = build_segment_infos_from_durations_ms(segment_durations_ms)
        segment_prompt_template = (
            "SEGMENT: {segment_number:03d} ({timestamp})\n"
            "Script / lyrics (do not render as on-screen text):\n"
            "{script}\n\n"
            "Direction:\n"
            "- Follow MASTER_PROMPT for style, identity, camera, lighting, and framing.\n"
            "- Continuity: first frame continues from previous segment; no reset.\n"
            "- No on-screen text, subtitles, captions, logos, watermarks, or gibberish artifacts.\n"
            "- Escalation: intensity {intensity:.2f} (0..1) guides subtle evolution only when supported by the audio.\n"
        )
        prompt_assets = build_or_load_prompt_assets(
            prompt_cache_root=PROMPT_CACHE_ROOT,
            output_stem=output_stem,
            master_prompt_text=master_prompt_text,
            lyrics_text=lyrics_text or "",
            segment_length_seconds=segment_length_seconds,
            segment_durations_ms=segment_durations_ms,
            song_title=song_title,
            song_artist=song_artist,
            input_mp3_hash=input_mp3_hash,
            segment_prompt_template=segment_prompt_template,
            lyrics_max_lines_per_segment=lyrics_max_lines_per_segment,
            segments_override=segments_override,
        )
        prompt_assets_dir = sync_prompt_assets_to_session(session_video_folder, prompt_assets)

        generic_segments_payload = (
            load_json_file(os.path.join(prompt_assets_dir, "segments.json")) or {}
        )
        segment_prompt_rows = list(generic_segments_payload.get("segments", []) or [])
        segment_prompt_texts = [row.get("segmentPrompt", "").strip() for row in segment_prompt_rows]
        lyrics_by_segment = [row.get("scriptLines", "") for row in segment_prompt_rows]

        write_text_file(os.path.join(session_video_folder, "master_prompt.txt"), master_prompt_text)
        write_text_file(os.path.join(session_video_folder, "lyrics.txt"), lyrics_text or "")
        timestamps_payload = (
            load_json_file(os.path.join(prompt_assets_dir, "timestamps.json")) or []
        )
        timestamp_map = [
            {
                "segment": entry.get("segment"),
                "start": entry.get("start"),
                "end": entry.get("end"),
                "timestamp": entry.get("timestamp"),
            }
            for entry in timestamps_payload
        ]
        write_json_file(os.path.join(session_video_folder, "timestamp_map.json"), timestamp_map)
        write_json_file(
            os.path.join(session_video_folder, "segments_prompts.json"),
            {
                "MASTER_PROMPT": master_prompt_text.strip(),
                "segment_length_seconds": float(segment_length_seconds),
                "segment_count": int(segment_count),
                "segments": [
                    {
                        "segmentNumber": row.get("segmentNumber"),
                        "timestamp": row.get("timestamp"),
                        "lyricsLines": row.get("scriptLines", ""),
                        "scenePrompt": row.get("segmentPrompt", ""),
                    }
                    for row in segment_prompt_rows
                ],
            },
        )
        creative_assets = {
            "prompt_assets_dir": os.path.abspath(prompt_assets_dir),
            "segments_json": os.path.abspath(os.path.join(prompt_assets_dir, "segments.json")),
            "timestamps_json": os.path.abspath(os.path.join(prompt_assets_dir, "timestamps.json")),
            "prompt_bundle_json": (
                os.path.abspath(os.path.join(prompt_assets_dir, "prompt_bundle.json"))
                if os.path.exists(os.path.join(prompt_assets_dir, "prompt_bundle.json"))
                else None
            ),
        }

        if not resume_session and not session_label:
            latest_session_folder = get_latest_session_folder(output_video_root)
            if latest_session_folder and has_all_video_segments(latest_session_folder, segment_count):
                if os.path.normpath(latest_session_folder) != os.path.normpath(session_video_folder):
                    print(
                        "Latest session already has all segments. "
                        "Starting a new session folder:", session_video_folder
                    )
    
        if has_all_video_segments(session_video_folder, segment_count):
            print(
                "All segments already exist in this session. "
                "Skipping Hedra and stitching immediately."
            )
            existing_manifest = read_run_manifest(session_video_folder)
            existing_fingerprint = existing_manifest.get("fingerprint", {}) if existing_manifest else {}
            existing_lip_sync = bool(existing_fingerprint.get("lip_sync_required", True))
            expected_videos = get_expected_video_paths(session_video_folder, segment_count)
            stitch_video(
                expected_videos,
                videos_manifest_path,
                final_video_name,
                audio_track_path=input_mp3 if not existing_lip_sync else None,
            )
            if run_archive_dir:
                archive_run_inputs(run_archive_dir, input_mp3, reference_images)
                archive_run_outputs(run_archive_dir, final_video_name)
            versioned_copy_path = save_versioned_video_copy(
                final_video_name,
                input_mp3,
                final_video_archive_folder,
                song_title=song_title,
                song_artist=song_artist,
            )
            print("\nDONE. Final video created:", final_video_name)
            print("Versioned archive copy:", versioned_copy_path)
            completed_successfully = True
            return {
                "final_video": os.path.abspath(final_video_name),
                "versioned_final_video": os.path.abspath(versioned_copy_path),
                "session_video_folder": os.path.abspath(session_video_folder),
                "videos_manifest": os.path.abspath(videos_manifest_path),
                "video_segments": [os.path.abspath(path) for path in expected_videos],
                "reused_cached_audio": reused_cached_audio,
                "reused_image_asset": False,
                "creative_assets": creative_assets,
            }
    
        effective_model_name = (model_name or "").strip()
        if not effective_model_name:
            effective_model_name = (
                default_lipsync_model_name if lip_sync_required else default_non_lipsync_model_name
            )
    
        prompt_text = master_prompt_text
        models = fetch_models()
        try:
            model = resolve_model(
                effective_model_name,
                require_audio_input=lip_sync_required,
                models=models,
            )
        except ValueError as exc:
            message = str(exc)
            if "not found. Available models:" not in message:
                raise
            fallback = pick_fallback_model(models, require_audio_input=lip_sync_required)
            if not fallback:
                raise
            print("Requested model not found:", effective_model_name)
            print("Falling back to model:", fallback.get("name"))
            effective_model_name = str(fallback.get("name", effective_model_name))
            model = fallback
        model_id = model["id"]
        model_requires_audio = bool(model.get("requires_audio_input"))
        print("Using prompt file:", prompt_file)
        print("Using model:", effective_model_name, model_id)
        print("Lip sync required:", bool(lip_sync_required))
    
        reference_image_hashes = [hash_file(path) for path in reference_images]
        reference_image_hashes_sorted = sorted(reference_image_hashes)
        fingerprint = {
            "input_mp3_hash": input_mp3_hash,
            "reference_image_hashes": reference_image_hashes_sorted,
            "prompt_hash": sha256_text(prompt_text),
            "segment_length_seconds": segment_length_seconds,
            "segment_count": segment_count,
            "model_id": model_id,
            "model_name": effective_model_name,
            "lip_sync_required": bool(lip_sync_required),
            "generated_video_inputs": {
                "aspect_ratio": "9:16",
                "resolution": "720p",
                "enhance_prompt": False,
            },
        }
        write_run_manifest(
            session_video_folder,
            {
                "created_at": datetime.now().isoformat(),
                "fingerprint": fingerprint,
            },
        )
        if any((row or "").strip() for row in lyrics_by_segment):
            save_lyrics_segments(session_video_folder, segment_length_seconds, lyrics_by_segment)
    
        if not resume_session:
            reused_segments = reuse_complete_session_segments(
                output_video_root,
                session_video_folder,
                fingerprint,
                segment_count,
            )
            if reused_segments and has_all_video_segments(session_video_folder, segment_count):
                print("Segments reused. Stitching without contacting Hedra...")
                expected_videos = get_expected_video_paths(session_video_folder, segment_count)
                stitch_video(
                    expected_videos,
                    videos_manifest_path,
                    final_video_name,
                    audio_track_path=input_mp3 if not model_requires_audio else None,
                )
                if run_archive_dir:
                    archive_run_inputs(run_archive_dir, input_mp3, reference_images)
                    archive_run_outputs(run_archive_dir, final_video_name)
                versioned_copy_path = save_versioned_video_copy(
                    final_video_name,
                    input_mp3,
                    final_video_archive_folder,
                    song_title=song_title,
                    song_artist=song_artist,
                )
                print("\nDONE. Final video created:", final_video_name)
                print("Versioned archive copy:", versioned_copy_path)
                completed_successfully = True
                return {
                    "final_video": os.path.abspath(final_video_name),
                    "versioned_final_video": os.path.abspath(versioned_copy_path),
                    "session_video_folder": os.path.abspath(session_video_folder),
                    "videos_manifest": os.path.abspath(videos_manifest_path),
                    "video_segments": [os.path.abspath(path) for path in expected_videos],
                    "reused_cached_audio": reused_cached_audio,
                    "reused_image_asset": False,
                    "creative_assets": creative_assets,
                }
    
        asset_cache = load_asset_cache(image_asset_cache_path) if image_asset_cache_path else None
        asset_cache_dirty = False
    
        image_asset_ids = []
        reused_image_asset = False
        for idx, (image_path, image_hash) in enumerate(zip(reference_images, reference_image_hashes)):
            image_asset_id = None
            if reuse_image_asset and asset_cache and not force_image_upload:
                cached_entry = asset_cache.get("images", {}).get(image_hash)
                if cached_entry and cached_entry.get("asset_id"):
                    image_asset_id = cached_entry["asset_id"]
                    reused_image_asset = True
                    print("Reusing cached image asset id:", image_asset_id)
    
            if not image_asset_id:
                print("Uploading reference image:", os.path.basename(image_path))
                image_asset_id = upload_asset(
                    image_path,
                    name=os.path.basename(image_path),
                    mime="image/png",
                    type_="image",
                )
                print("Reference image asset id:", image_asset_id)
                if reuse_image_asset and asset_cache and not force_image_upload:
                    asset_cache["images"][image_hash] = {
                        "asset_id": image_asset_id,
                        "filename": os.path.basename(image_path),
                        "updated_at": datetime.now().isoformat(),
                    }
                    asset_cache_dirty = True
    
            image_asset_ids.append(image_asset_id)
    
        video_files = []
        current_start_keyframe_id = image_asset_ids[0]
        segment_jobs = load_segment_jobs(session_video_folder)
    
        for idx, (audio_file, audio_duration_ms) in enumerate(audio_files):
            print(f"\nProcessing segment {idx + 1}/{len(audio_files)}")
            video_path = os.path.join(session_video_folder, f"video_{idx:03}.mp4")
    
            if os.path.exists(video_path) and os.path.getsize(video_path) > 0:
                print("Skipping existing video segment:", video_path)
                video_files.append(video_path)
                if idx < len(audio_files) - 1:
                    current_start_keyframe_id = upload_continuity_frame(
                        video_path,
                        idx,
                        session_video_folder,
                    )
                continue
    
            segment_key = f"{idx:03}"
            job_entry = segment_jobs.get(segment_key)
            if job_entry and job_entry.get("job_id"):
                print("Found existing job for segment", segment_key, "- checking status...")
                status_url = f"{API_BASE}/generations/{job_entry['job_id']}/status"
                try:
                    status_json = poll_generation_status(status_url)
                    output = status_json.get("output", {})
                    video_url = (
                        output.get("video_url")
                        or status_json.get("download_url")
                        or status_json.get("url")
                        or status_json.get("streaming_url")
                    )
                    if not video_url:
                        raise RuntimeError("No video_url returned for existing job")
    
                    print("Downloading video for existing job...")
                    download_video_file(video_url, video_path)
                    video_files.append(video_path)
                    print("Saved:", video_path)
    
                    segment_jobs[segment_key]["status"] = "complete"
                    segment_jobs[segment_key]["video_path"] = video_path
                    save_segment_jobs(session_video_folder, segment_jobs)
    
                    if idx < len(audio_files) - 1:
                        current_start_keyframe_id = upload_continuity_frame(
                            video_path,
                            idx,
                            session_video_folder,
                        )
                    if request_delay_seconds > 0:
                        print(f"Waiting {request_delay_seconds} seconds (rate limit buffer)...")
                        time.sleep(request_delay_seconds)
                    continue
                except Exception as exc:
                    print("Existing job could not be used, will create a new one:", exc)
                    segment_jobs.pop(segment_key, None)
                    save_segment_jobs(session_video_folder, segment_jobs)
    
            audio_asset_id = None
            audio_hash = None
            reused_audio_asset = False
            if model_requires_audio:
                if reuse_audio_asset and asset_cache and not force_audio_upload:
                    audio_hash = hash_file(audio_file)
                    cached_audio = asset_cache.get("audio", {}).get(audio_hash)
                    if cached_audio and cached_audio.get("asset_id"):
                        audio_asset_id = cached_audio["asset_id"]
                        reused_audio_asset = True
                        print("Reusing cached audio asset id:", audio_asset_id)
    
                if not audio_asset_id:
                    print("Uploading audio segment:", audio_file)
                    audio_asset_id = upload_asset(
                        audio_file,
                        name=os.path.basename(audio_file),
                        mime="audio/mpeg",
                        type_="audio",
                    )
                    print("Audio asset id:", audio_asset_id)
                    if reuse_audio_asset and asset_cache and not force_audio_upload:
                        if audio_hash is None:
                            audio_hash = hash_file(audio_file)
                        asset_cache["audio"][audio_hash] = {
                            "asset_id": audio_asset_id,
                            "filename": os.path.basename(audio_file),
                            "updated_at": datetime.now().isoformat(),
                        }
                        asset_cache_dirty = True
            else:
                print("Model does not require audio; skipping audio upload.")
    
            include_reference_images = (idx == 0 and len(image_asset_ids) > 1)
    
            segment_prompt = (
                segment_prompt_texts[idx] if idx < len(segment_prompt_texts) else prompt_text
            )
            segment_prompt = validate_prompt_text(segment_prompt)
    
            generation_payload = {
                "type": "video",
                "ai_model_id": model_id,
                "generated_video_inputs": {
                    "text_prompt": segment_prompt,
                    "duration_ms": audio_duration_ms,
                    "aspect_ratio": "9:16",
                    "resolution": "720p",
                    "enhance_prompt": False,
                },
            }
            if include_reference_images:
                generation_payload["reference_image_ids"] = image_asset_ids
            else:
                generation_payload["start_keyframe_id"] = current_start_keyframe_id
            if audio_asset_id:
                generation_payload["audio_id"] = audio_asset_id
    
            print("Starting generation...")
            gen_url = f"{API_BASE}/generations"
            resp = http_session().post(
                gen_url,
                headers=get_headers("application/json"),
                json=generation_payload,
                timeout=60,
            )
    
            if include_reference_images and resp.status_code == 422:
                try:
                    body = resp.json()
                    messages = " ".join(body.get("messages", []))
                except Exception:
                    messages = resp.text
                if "reference_image_ids" in messages and ("start/end keyframe" in messages or "start keyframe" in messages):
                    print("Multiple reference images rejected with keyframe rules; retrying without reference_image_ids.")
                    generation_payload.pop("reference_image_ids", None)
                    generation_payload["start_keyframe_id"] = current_start_keyframe_id
                    resp = http_session().post(
                        gen_url,
                        headers=get_headers("application/json"),
                        json=generation_payload,
                        timeout=60,
                    )
    
            if not resp.ok:
                if resp.status_code == 402:
                    try:
                        error_body = resp.json()
                        error_messages = error_body.get("messages", [])
                        error_message = "; ".join(error_messages) if error_messages else resp.text
                    except Exception:
                        error_message = resp.text
    
                    print(f"Stopping generation because Hedra returned 402: {error_message}")
                    break
                raise_with_body(resp)
    
            generation_data = resp.json()
            job_id = generation_data.get("id") or generation_data.get("generation_id")
    
            if not job_id:
                raise Exception(f"No job_id returned: {generation_data}")
    
            print("Generation started. Job ID:", job_id)
            segment_jobs[segment_key] = {
                "job_id": job_id,
                "audio_file": os.path.abspath(audio_file),
                "audio_asset_id": audio_asset_id,
                "duration_ms": audio_duration_ms,
                "model_id": model_id,
                "requires_audio_input": model_requires_audio,
                "created_at": datetime.now().isoformat(),
                "status": "submitted",
            }
            save_segment_jobs(session_video_folder, segment_jobs)
    
            status_url = f"{API_BASE}/generations/{job_id}/status"
    
            status_json = poll_generation_status(status_url)
    
            output = status_json.get("output", {})
            video_url = (
                output.get("video_url")
                or status_json.get("download_url")
                or status_json.get("url")
                or status_json.get("streaming_url")
            )
    
            if not video_url:
                raise Exception("No video_url returned")
    
            print("Downloading video...")
            download_video_file(video_url, video_path)
    
            video_files.append(video_path)
            print("Saved:", video_path)
            segment_jobs[segment_key]["status"] = "complete"
            segment_jobs[segment_key]["video_path"] = video_path
            save_segment_jobs(session_video_folder, segment_jobs)
    
            if idx < len(audio_files) - 1:
                current_start_keyframe_id = upload_continuity_frame(
                    video_path,
                    idx,
                    session_video_folder,
                )
    
            if request_delay_seconds > 0:
                print(f"Waiting {request_delay_seconds} seconds (rate limit buffer)...")
                time.sleep(request_delay_seconds)
    
        if asset_cache_dirty and image_asset_cache_path and asset_cache:
            save_asset_cache(image_asset_cache_path, asset_cache)
    
        if not video_files:
            raise RuntimeError("No video segments were generated. Check credits and API errors.")
    
        if len(video_files) < len(audio_files):
            print(
                f"\nStitching partial video from {len(video_files)}/{len(audio_files)} completed segments..."
            )
        else:
            print("\nStitching final video...")
    
        stitch_video(
            video_files,
            videos_manifest_path,
            final_video_name,
            audio_track_path=input_mp3 if not model_requires_audio else None,
        )
        if run_archive_dir:
            archive_run_inputs(run_archive_dir, input_mp3, reference_images)
            archive_run_outputs(run_archive_dir, final_video_name)
        versioned_copy_path = save_versioned_video_copy(
            final_video_name,
            input_mp3,
            final_video_archive_folder,
            song_title=song_title,
            song_artist=song_artist,
        )
    
        print("\nDONE. Final video created:", final_video_name)
        print("Versioned archive copy:", versioned_copy_path)
        completed_successfully = True
        return {
            "final_video": os.path.abspath(final_video_name),
            "versioned_final_video": os.path.abspath(versioned_copy_path),
            "session_video_folder": os.path.abspath(session_video_folder),
            "videos_manifest": os.path.abspath(videos_manifest_path),
            "video_segments": [os.path.abspath(path) for path in video_files],
            "reused_cached_audio": reused_cached_audio,
            "reused_image_asset": reused_image_asset,
            "creative_assets": creative_assets,
        }
    finally:
        if not completed_successfully:
            _remove_dir_if_empty(run_archive_dir)


def main():
    run_pipeline()


if __name__ == "__main__":
    main()
