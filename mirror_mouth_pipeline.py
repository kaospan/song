#!/usr/bin/env python3
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import time
from datetime import datetime

import requests
from dotenv import load_dotenv

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


def sanitize_filename_component(value):
    if value is None:
        return None

    sanitized = re.sub(r'[<>:"/\\\\|?*\\x00-\\x1f]', "_", value)
    sanitized = re.sub(r"\\s+", " ", sanitized).strip()
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
SEGMENT_LENGTH_SECONDS = int(os.environ.get("SEGMENT_LENGTH_SECONDS", "8"))
OUTPUT_AUDIO_FOLDER = normalize_env_path(os.environ.get("OUTPUT_AUDIO_FOLDER")) or "segments"
OUTPUT_VIDEO_FOLDER = normalize_env_path(os.environ.get("OUTPUT_VIDEO_FOLDER")) or "video_segments"
FINAL_VIDEO_NAME = normalize_env_path(os.environ.get("FINAL_VIDEO_NAME")) or "final_music_video.mp4"
FINAL_VIDEO_ARCHIVE_FOLDER = (
    normalize_env_path(os.environ.get("FINAL_VIDEO_ARCHIVE_FOLDER")) or "final_videos"
)
PROMPT_FILE = normalize_env_path(os.environ.get("PROMPT_FILE")) or "mirror_mouth_prompt.txt"
HEDRA_MODEL_NAME = os.environ.get("HEDRA_MODEL_NAME", "Kling AI Avatar v2 Standard").strip()
SONG_TITLE = os.environ.get("SONG_TITLE")
SONG_ARTIST = os.environ.get("SONG_ARTIST")
IMAGE_ASSET_CACHE_PATH = (
    normalize_env_path(os.environ.get("IMAGE_ASSET_CACHE_PATH")) or "job_runs/asset_cache.json"
)
AUDIO_CACHE_ROOT = normalize_env_path(os.environ.get("AUDIO_CACHE_ROOT"))
REUSE_IMAGE_ASSET = os.environ.get("REUSE_IMAGE_ASSET", "1").strip().lower() not in ("0", "false", "no")
FORCE_IMAGE_UPLOAD = os.environ.get("FORCE_IMAGE_UPLOAD", "0").strip().lower() in ("1", "true", "yes")
REQUEST_DELAY_SECONDS = float(os.environ.get("REQUEST_DELAY_SECONDS", "60"))
STATUS_POLL_INTERVAL_SECONDS = float(os.environ.get("STATUS_POLL_INTERVAL_SECONDS", "10"))
QUEUED_STATUS_SLEEP_SECONDS = float(os.environ.get("QUEUED_STATUS_SLEEP_SECONDS", "120"))
STATUS_TIMEOUT_SECONDS = float(os.environ.get("STATUS_TIMEOUT_SECONDS", "60"))
STATUS_RETRY_LIMIT = int(os.environ.get("STATUS_RETRY_LIMIT", "10"))
STATUS_RETRY_BACKOFF_SECONDS = float(os.environ.get("STATUS_RETRY_BACKOFF_SECONDS", "10"))
DOWNLOAD_TIMEOUT_SECONDS = float(os.environ.get("DOWNLOAD_TIMEOUT_SECONDS", "120"))
DOWNLOAD_RETRY_LIMIT = int(os.environ.get("DOWNLOAD_RETRY_LIMIT", "5"))
DOWNLOAD_RETRY_BACKOFF_SECONDS = float(os.environ.get("DOWNLOAD_RETRY_BACKOFF_SECONDS", "10"))
PROMPT_CHAR_LIMIT = int(os.environ.get("PROMPT_CHAR_LIMIT", "2500"))
RESUME_SESSION = normalize_env_path(os.environ.get("RESUME_SESSION"))
SESSION_LABEL = os.environ.get("SESSION_LABEL")

API_BASE = "https://api.hedra.com/web-app/public"
API_KEY = os.environ.get("API_KEY")


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
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(url)
    params = {k.lower(): v for k, v in parse_qs(parsed.query).items()}
    presign_keys = {"x-amz-algorithm", "x-amz-signature", "x-amz-credential",
                    "x-amz-security-token", "awsaccesskeyid", "signature"}
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


def resolve_model(model_name):
    url = f"{API_BASE}/models"
    resp = requests.get(url, headers=get_headers(), timeout=30)
    if not resp.ok:
        raise_with_body(resp)

    models = resp.json()
    normalized_name = model_name.strip().lower()

    for model in models:
        if str(model.get("name", "")).strip().lower() == normalized_name:
            if not model.get("requires_audio_input"):
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

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if not resp.ok:
        raise_with_body(resp)

    return resp.json()


def upload_file_to_asset(asset_id, path, mime="application/octet-stream"):
    url = f"{API_BASE}/assets/{asset_id}/upload"

    with open(path, "rb") as f:
        files = {"file": (os.path.basename(path), f, mime)}
        resp = requests.post(url, headers=get_headers(), files=files, timeout=120)

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
            put_resp = requests.put(upload_url, data=f, timeout=120)
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
        return {"images": {}}
    if "images" not in cache or not isinstance(cache["images"], dict):
        cache["images"] = {}
    return cache


def save_asset_cache(cache_path, cache):
    write_json_file(cache_path, cache)


def stitch_video(video_files, manifest_path, final_video_name):
    ffmpeg_bin = FFMPEG_PATH or "ffmpeg"

    with open(manifest_path, "w", encoding="utf-8") as f:
        for video_file in video_files:
            f.write(f"file '{os.path.abspath(video_file)}'\n")

    ffmpeg_cmd = [
        ffmpeg_bin,
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        manifest_path,
        "-c:v",
        "libx264",
        "-crf",
        "23",
        "-preset",
        "medium",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        final_video_name,
    ]

    print("Running ffmpeg...")
    subprocess.run(ffmpeg_cmd, check=True)


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
            status_resp = requests.get(
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

        if not status_resp.ok:
            raise_with_body(status_resp)

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
            with requests.get(
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
):
    if not os.path.exists(input_mp3):
        raise FileNotFoundError(f"Input MP3 not found: {input_mp3}")
    if not os.path.exists(reference_image):
        raise FileNotFoundError(f"Reference image not found: {reference_image}")

    input_mp3_hash = hash_file(input_mp3)
    if audio_cache_root:
        output_audio_folder = os.path.join(audio_cache_root, input_mp3_hash)

    session_video_folder = build_session_video_folder(
        output_video_root,
        session_label=session_label,
        resume_session=resume_session,
    )
    final_video_name = build_named_final_video_path(final_video_name, song_title, song_artist)
    final_video_name = build_timestamped_final_video_name(final_video_name)
    videos_manifest_path = os.path.join(session_video_folder, "videos.txt")

    if resume_session:
        print("Resuming session folder:", session_video_folder)
    else:
        print("Session video folder:", session_video_folder)

    reused_cached_audio = False
    audio_files = load_cached_audio_segments(
        output_audio_folder,
        input_mp3_hash,
        segment_length_seconds,
    )
    if audio_files:
        reused_cached_audio = True
    else:
        audio_files = split_audio(input_mp3, output_audio_folder, segment_length_seconds)
        write_segments_manifest(
            output_audio_folder,
            input_mp3,
            input_mp3_hash,
            segment_length_seconds,
            audio_files,
        )
    segment_count = len(audio_files)

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
        expected_videos = get_expected_video_paths(session_video_folder, segment_count)
        stitch_video(expected_videos, videos_manifest_path, final_video_name)
        versioned_copy_path = save_versioned_video_copy(
            final_video_name,
            input_mp3,
            final_video_archive_folder,
            song_title=song_title,
            song_artist=song_artist,
        )
        print("\nDONE. Final video created:", final_video_name)
        print("Versioned archive copy:", versioned_copy_path)
        return {
            "final_video": os.path.abspath(final_video_name),
            "versioned_final_video": os.path.abspath(versioned_copy_path),
            "session_video_folder": os.path.abspath(session_video_folder),
            "videos_manifest": os.path.abspath(videos_manifest_path),
            "video_segments": [os.path.abspath(path) for path in expected_videos],
            "reused_cached_audio": reused_cached_audio,
            "reused_image_asset": False,
        }

    image_asset_id = None
    image_hash = None
    reused_image_asset = False
    if reuse_image_asset and image_asset_cache_path and not force_image_upload:
        image_hash = hash_file(reference_image)
        cache = load_asset_cache(image_asset_cache_path)
        cached_entry = cache.get("images", {}).get(image_hash)
        if cached_entry and cached_entry.get("asset_id"):
            image_asset_id = cached_entry["asset_id"]
            reused_image_asset = True
            print("Reusing cached image asset id:", image_asset_id)

    if not image_asset_id:
        print("Uploading reference image...")
        image_asset_id = upload_asset(
            reference_image,
            name=os.path.basename(reference_image),
            mime="image/png",
            type_="image",
        )
        print("Reference image asset id:", image_asset_id)
        if reuse_image_asset and image_asset_cache_path and not force_image_upload:
            if image_hash is None:
                image_hash = hash_file(reference_image)
            cache = load_asset_cache(image_asset_cache_path)
            cache["images"][image_hash] = {
                "asset_id": image_asset_id,
                "filename": os.path.basename(reference_image),
                "updated_at": datetime.now().isoformat(),
            }
            save_asset_cache(image_asset_cache_path, cache)

    prompt_text = load_prompt_text(prompt_file)
    model = resolve_model(model_name)
    model_id = model["id"]
    print("Using prompt file:", prompt_file)
    print("Using model:", model_name, model_id)

    video_files = []
    current_start_keyframe_id = image_asset_id

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

        print("Uploading audio segment:", audio_file)
        audio_asset_id = upload_asset(
            audio_file,
            name=os.path.basename(audio_file),
            mime="audio/mpeg",
            type_="audio",
        )
        print("Audio asset id:", audio_asset_id)

        generation_payload = {
            "type": "video",
            "start_keyframe_id": current_start_keyframe_id,
            "audio_id": audio_asset_id,
            "ai_model_id": model_id,
            "generated_video_inputs": {
                "text_prompt": prompt_text,
                "duration_ms": audio_duration_ms,
                "aspect_ratio": "9:16",
                "resolution": "720p",
                "enhance_prompt": False,
            },
        }

        print("Starting generation...")
        gen_url = f"{API_BASE}/generations"
        resp = requests.post(
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

        if idx < len(audio_files) - 1:
            current_start_keyframe_id = upload_continuity_frame(
                video_path,
                idx,
                session_video_folder,
            )

        if request_delay_seconds > 0:
            print(f"Waiting {request_delay_seconds} seconds (rate limit buffer)...")
            time.sleep(request_delay_seconds)

    if not video_files:
        raise RuntimeError("No video segments were generated. Check credits and API errors.")

    if len(video_files) < len(audio_files):
        print(
            f"\nStitching partial video from {len(video_files)}/{len(audio_files)} completed segments..."
        )
    else:
        print("\nStitching final video...")

    stitch_video(video_files, videos_manifest_path, final_video_name)
    versioned_copy_path = save_versioned_video_copy(
        final_video_name,
        input_mp3,
        final_video_archive_folder,
        song_title=song_title,
        song_artist=song_artist,
    )

    print("\nDONE. Final video created:", final_video_name)
    print("Versioned archive copy:", versioned_copy_path)
    return {
        "final_video": os.path.abspath(final_video_name),
        "versioned_final_video": os.path.abspath(versioned_copy_path),
        "session_video_folder": os.path.abspath(session_video_folder),
        "videos_manifest": os.path.abspath(videos_manifest_path),
        "video_segments": [os.path.abspath(path) for path in video_files],
        "reused_cached_audio": reused_cached_audio,
        "reused_image_asset": reused_image_asset,
    }


def main():
    run_pipeline()


if __name__ == "__main__":
    main()
