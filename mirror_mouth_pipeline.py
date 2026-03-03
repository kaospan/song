#!/usr/bin/env python3
import json
import math
import os
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
PROMPT_FILE = normalize_env_path(os.environ.get("PROMPT_FILE")) or "mirror_mouth_prompt.txt"
HEDRA_MODEL_NAME = os.environ.get("HEDRA_MODEL_NAME", "Kling V3 Standard I2V").strip()
REQUEST_DELAY_SECONDS = float(os.environ.get("REQUEST_DELAY_SECONDS", "60"))
STATUS_POLL_INTERVAL_SECONDS = float(os.environ.get("STATUS_POLL_INTERVAL_SECONDS", "10"))
QUEUED_STATUS_SLEEP_SECONDS = float(os.environ.get("QUEUED_STATUS_SLEEP_SECONDS", "120"))
STATUS_TIMEOUT_SECONDS = float(os.environ.get("STATUS_TIMEOUT_SECONDS", "60"))
STATUS_RETRY_LIMIT = int(os.environ.get("STATUS_RETRY_LIMIT", "10"))
STATUS_RETRY_BACKOFF_SECONDS = float(os.environ.get("STATUS_RETRY_BACKOFF_SECONDS", "10"))
DOWNLOAD_TIMEOUT_SECONDS = float(os.environ.get("DOWNLOAD_TIMEOUT_SECONDS", "120"))
DOWNLOAD_RETRY_LIMIT = int(os.environ.get("DOWNLOAD_RETRY_LIMIT", "5"))
DOWNLOAD_RETRY_BACKOFF_SECONDS = float(os.environ.get("DOWNLOAD_RETRY_BACKOFF_SECONDS", "10"))
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

    return prompt_text


def resolve_model_id(model_name):
    url = f"{API_BASE}/models"
    resp = requests.get(url, headers=get_headers(), timeout=30)
    if not resp.ok:
        raise_with_body(resp)

    models = resp.json()
    normalized_name = model_name.strip().lower()

    for model in models:
        if str(model.get("name", "")).strip().lower() == normalized_name:
            return model["id"]

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
        if os.path.isabs(resume_session):
            session_folder = resume_session
        else:
            session_folder = os.path.join(output_video_root, resume_session)
        os.makedirs(session_folder, exist_ok=True)
        return session_folder

    session_name = session_label or datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
    session_folder = os.path.join(output_video_root, session_name)
    os.makedirs(session_folder, exist_ok=True)
    return session_folder


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

        status_response.raise_for_status()
        status_json = status_response.json()

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

    for attempt in range(1, DOWNLOAD_RETRY_LIMIT + 1):
        try:
            with requests.get(video_url, stream=True, timeout=DOWNLOAD_TIMEOUT_SECONDS) as vid_resp:
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
    prompt_file=PROMPT_FILE,
    model_name=HEDRA_MODEL_NAME,
    request_delay_seconds=REQUEST_DELAY_SECONDS,
    resume_session=RESUME_SESSION,
    session_label=SESSION_LABEL,
):
    if not os.path.exists(input_mp3):
        raise FileNotFoundError(f"Input MP3 not found: {input_mp3}")
    if not os.path.exists(reference_image):
        raise FileNotFoundError(f"Reference image not found: {reference_image}")

    session_video_folder = build_session_video_folder(
        output_video_root,
        session_label=session_label,
        resume_session=resume_session,
    )
    videos_manifest_path = os.path.join(session_video_folder, "videos.txt")

    print("Session video folder:", session_video_folder)

    audio_files = split_audio(input_mp3, output_audio_folder, segment_length_seconds)

    print("Uploading reference image...")
    image_asset_id = upload_asset(
        reference_image,
        name=os.path.basename(reference_image),
        mime="image/png",
        type_="image",
    )
    print("Reference image asset id:", image_asset_id)

    prompt_text = load_prompt_text(prompt_file)
    model_id = resolve_model_id(model_name)
    print("Using prompt file:", prompt_file)
    print("Using model:", model_name, model_id)

    video_files = []

    for idx, (audio_file, audio_duration_ms) in enumerate(audio_files):
        print(f"\nProcessing segment {idx + 1}/{len(audio_files)}")
        video_path = os.path.join(session_video_folder, f"video_{idx:03}.mp4")

        if os.path.exists(video_path) and os.path.getsize(video_path) > 0:
            print("Skipping existing video segment:", video_path)
            video_files.append(video_path)
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
            "start_keyframe_id": image_asset_id,
            "audio_id": audio_asset_id,
            "ai_model_id": model_id,
            "generated_video_inputs": {
                "text_prompt": prompt_text,
                "duration_ms": audio_duration_ms,
                "aspect_ratio": "9:16",
                "resolution": "720p",
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

        if request_delay_seconds > 0:
            print(f"Waiting {request_delay_seconds} seconds (rate limit buffer)...")
            time.sleep(request_delay_seconds)

    print("\nStitching final video...")
    stitch_video(video_files, videos_manifest_path, final_video_name)

    print("\nDONE. Final video created:", final_video_name)
    return {
        "final_video": os.path.abspath(final_video_name),
        "session_video_folder": os.path.abspath(session_video_folder),
        "videos_manifest": os.path.abspath(videos_manifest_path),
        "video_segments": [os.path.abspath(path) for path in video_files],
    }


def main():
    run_pipeline()


if __name__ == "__main__":
    main()
