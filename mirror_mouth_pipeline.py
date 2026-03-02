
import os
import math
import time
import requests
import subprocess
from datetime import datetime
from tqdm import tqdm
from dotenv import load_dotenv

# =====================================================
# ENV
# =====================================================

# Load repo-root .env first, then prefer .venv/.env if present
load_dotenv()
load_dotenv(os.path.join(".venv", ".env"), override=True)

# Repair Windows backslash paths that were quoted in .env and parsed with escapes.
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


# Configure ffmpeg/ffprobe paths for pydub if provided
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

INPUT_MP3 = "full_song.mp3"
REFERENCE_IMAGE = "reference_image.png"

SEGMENT_LENGTH_SECONDS = 8
OUTPUT_AUDIO_FOLDER = "segments"
OUTPUT_VIDEO_FOLDER = "video_segments"
FINAL_VIDEO_NAME = "final_music_video.mp4"
PROMPT_FILE = normalize_env_path(os.environ.get("PROMPT_FILE")) or "mirror_mouth_prompt.txt"
HEDRA_MODEL_NAME = os.environ.get("HEDRA_MODEL_NAME", "Kling V3 Standard I2V").strip()

API_BASE = "https://api.hedra.com/web-app/public"

# Use environment variable for security
API_KEY = os.environ.get("API_KEY")

if not API_KEY:
    raise ValueError("API_KEY environment variable not set")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}

# =====================================================
# HELPERS
# =====================================================


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
    resp = requests.get(url, headers=HEADERS, timeout=30)
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
    headers = {**HEADERS, "Content-Type": "application/json"}

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if not resp.ok:
        raise_with_body(resp)

    return resp.json()


def upload_file_to_asset(asset_id, path, mime="application/octet-stream"):
    url = f"{API_BASE}/assets/{asset_id}/upload"

    with open(path, "rb") as f:
        files = {"file": (os.path.basename(path), f, mime)}
        resp = requests.post(url, headers=HEADERS, files=files, timeout=120)

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


# =====================================================
# CREATE FOLDERS
# =====================================================

os.makedirs(OUTPUT_AUDIO_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_VIDEO_FOLDER, exist_ok=True)

session_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
session_video_folder = os.path.join(OUTPUT_VIDEO_FOLDER, session_timestamp)
os.makedirs(session_video_folder, exist_ok=True)
videos_manifest_path = os.path.join(session_video_folder, "videos.txt")

print("Session video folder:", session_video_folder)

print("Splitting audio...")

audio = AudioSegment.from_mp3(INPUT_MP3)
duration_ms = len(audio)
segment_length_ms = SEGMENT_LENGTH_SECONDS * 1000
num_segments = math.ceil(duration_ms / segment_length_ms)

audio_files = []

for i in range(num_segments):
    start = i * segment_length_ms
    end = min((i + 1) * segment_length_ms, duration_ms)
    segment = audio[start:end]

    audio_files.append((filename, len(segment)))

print(f"{len(audio_files)} segments created.")

# =====================================================
# PROCESS EACH SEGMENT SEQUENTIALLY
# =====================================================

prompt_text = load_prompt_text(PROMPT_FILE)
model_id = resolve_model_id(HEDRA_MODEL_NAME)
print("Using prompt file:", PROMPT_FILE)
print("Using model:", HEDRA_MODEL_NAME, model_id)

video_files = []

for idx, (audio_file, audio_duration_ms) in enumerate(audio_files):
    print(f"\nProcessing segment {idx + 1}/{len(audio_files)}")

    # ---- Upload Image ----
    with open(REFERENCE_IMAGE, "rb") as img:
        img_upload = requests.post(
            f"{API_BASE}/assets",
            headers=HEADERS,
            files={"file": ("image.png", img, "image/png")}
        )

    # Prepare payload
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

    # ---- Upload Audio ----
    with open(audio_file, "rb") as aud:
        audio_upload = requests.post(
            f"{API_BASE}/assets",
            headers=HEADERS,
            files={"file": ("audio.mp3", aud, "audio/mpeg")}
        )

    audio_upload.raise_for_status()
    audio_id = audio_upload.json().get("id")
    print("Audio uploaded:", audio_id)

    # ---- Start Generation ----
    generation_response = requests.post(
        f"{API_BASE}/generations",
        headers={**HEADERS, "Content-Type": "application/json"},
        json={
            "start_keyframe_id": image_id,
            "audio_id": audio_id,
            "reference_image_ids": [image_id],
            "ai_model_id": "d1dd37a3-e39a-4854-a298-6510289f9cf2",
            "generated_video_inputs": {
                "text_prompt": PROMPT_TEXT,
                "duration_ms": SEGMENT_LENGTH_SECONDS * 1000,
                "aspect_ratio": "9:16",
                "resolution": "720p"
            }
        }
    )

    generation_response.raise_for_status()
    generation_data = generation_response.json()

    job_id = generation_data.get("id") or generation_data.get("generation_id")

    if not job_id:
        raise Exception(f"No job_id returned: {generation_data}")

    print("Generation started. Job ID:", job_id)

    # Poll status
    status_url = f"{API_BASE}/generations/{job_id}/status"

    while True:
        status_response = requests.get(
            f"{API_BASE}/generations/{job_id}",
            headers=HEADERS
        )

        status_response.raise_for_status()
        status_json = status_response.json()

        status = status_json.get("status")
        print("Status:", status)

        if status == "complete":
            break
        if status in ("failed", "error"):
            raise Exception(f"Generation failed: {json.dumps(status_json, indent=2)}")

        time.sleep(10)

    # Download video
    output = status_json.get("output", {})
    video_url = (
        output.get("video_url")
        or status_json.get("download_url")
        or status_json.get("url")
        or status_json.get("streaming_url")
    )

    if not video_url:
        raise Exception("No video_url returned")

    video_path = os.path.join(session_video_folder, f"video_{idx:03}.mp4")

    print("Downloading video...")

    video_data = requests.get(video_url).content
    video_path = os.path.join(OUTPUT_VIDEO_FOLDER, f"video_{idx:03}.mp4")

    with open(video_path, "wb") as f:
        f.write(video_data)

    video_files.append(video_path)

    print("Segment saved:", video_path)

    # ---- Rate Limit Buffer ----
    print("Waiting 60 seconds to avoid rate limit...")
    time.sleep(60)

# =====================================================
# STITCH FINAL VIDEO
# =====================================================

print("\nStitching final video...")

with open(videos_manifest_path, "w") as f:
    for v in video_files:
        f.write(f"file '{os.path.abspath(v)}'\n")

ffmpeg_bin = FFMPEG_PATH or "ffmpeg"
ffmpeg_cmd = [
    ffmpeg_bin,
    "-f",
    "concat",
    "-safe",
    "0",
    "-i",
    videos_manifest_path,
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
    FINAL_VIDEO_NAME,
]

print("DONE.")

    
