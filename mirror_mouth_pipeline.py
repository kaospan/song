#!/usr/bin/env python3
import os
import math
import time
import json
import requests
import subprocess
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

API_BASE = "https://api.hedra.com/web-app/public"

# Use environment variable for security
API_KEY = os.environ.get("API_KEY")

if not API_KEY:
    raise ValueError("API_KEY environment variable not set")

HEADERS = {
    "X-API-Key": API_KEY,
    "Authorization": f"Bearer {API_KEY}",
}

PROMPT_TEXT = """Create a realistic close-up lip-synced performance using the provided reference image and provided audio segment.

STYLE:
Dark cinematic tone.
Very dark background.
High-contrast lighting with a single strong key light from camera-left.
Opposite side in deep shadow.
Cool subtle blue tint in shadows.
Lighting remains stable and unchanging from first frame to last frame.

CAMERA:
True locked tripod close-up.
No zoom.
No push-in.
No crop.
No reframing.
No auto-centering.
Subject size remains constant.
Framing matches the reference image exactly.

VISUAL ELEMENT:
A vertical luminous seam divides the face down the center.
The seam is a fixed visual design element.
It remains constant in brightness, thickness, and position.
No fading.
No pulsing.
No brightness changes.

The left eye contains a subtle steady internal glow.
The glow remains constant and stable.
No flicker.
No activation.
No intensity change.

FACIAL EXPRESSION:
Left half: calm, serious, minimal expression.
Right half: slightly more expressive but still controlled.
No smiling.
No exaggerated asymmetry.
No dramatic emotional spikes.

PERFORMANCE:
Highly accurate lip sync driven strictly by the audio.
Natural vowel articulation.
Minimal head movement.
Natural blinking.
Controlled breathing.

During sustained notes, the mouth remains naturally open for the full duration of the note and closes only when the audio ends.
Jaw remains steady during long vowels.
"""

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
# MAIN
# =====================================================

os.makedirs(OUTPUT_AUDIO_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_VIDEO_FOLDER, exist_ok=True)

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
    filename = os.path.join(OUTPUT_AUDIO_FOLDER, f"segment{i:03}.mp3")
    segment.export(filename, format="mp3", bitrate="320k")

    audio_files.append((filename, len(segment)))

print(f"{len(audio_files)} segments created.")

# Upload reference image once
print("Uploading reference image...")
image_asset_id = upload_asset(
    REFERENCE_IMAGE,
    name=os.path.basename(REFERENCE_IMAGE),
    mime="image/png",
    type_="image",
)
print("Reference image asset id:", image_asset_id)

video_files = []

for idx, (audio_file, audio_duration_ms) in enumerate(audio_files):
    print(f"\nProcessing segment {idx + 1}/{len(audio_files)}")

    # Upload audio
    print("Uploading audio segment:", audio_file)
    audio_asset_id = upload_asset(
        audio_file,
        name=os.path.basename(audio_file),
        mime="audio/mpeg",
        type_="audio",
    )
    print("Audio asset id:", audio_asset_id)

    # Prepare payload
    generation_payload = {
        "type": "video",
        "start_keyframe_id": image_asset_id,
        "audio_id": audio_asset_id,
        "ai_model_id": "d1dd37a3-e39a-4854-a298-6510289f9cf2",
        "generated_video_inputs": {
            "text_prompt": PROMPT_TEXT,
            "duration_ms": audio_duration_ms,
            "aspect_ratio": "9:16",
            "resolution": "720p",
        },
    }

    print("Starting generation...")
    gen_url = f"{API_BASE}/generations"

    resp = requests.post(
        gen_url,
        headers={**HEADERS, "Content-Type": "application/json"},
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

    # Poll status
    status_url = f"{API_BASE}/generations/{job_id}/status"

    while True:
        status_resp = requests.get(status_url, headers=HEADERS, timeout=30)

        if not status_resp.ok:
            raise_with_body(status_resp)

        status_json = status_resp.json()
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

    video_path = os.path.join(OUTPUT_VIDEO_FOLDER, f"video_{idx:03}.mp4")

    print("Downloading video...")
    with requests.get(video_url, stream=True, timeout=120) as vid_resp:
        if not vid_resp.ok:
            raise_with_body(vid_resp)

        with open(video_path, "wb") as f:
            for chunk in vid_resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    video_files.append(video_path)
    print("Saved:", video_path)

    print("Waiting 60 seconds (rate limit buffer)...")
    time.sleep(60)

# =====================================================
# STITCH FINAL VIDEO
# =====================================================

print("\nStitching final video...")

with open("videos.txt", "w") as f:
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
    "videos.txt",
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

print("Running ffmpeg...")
subprocess.run(ffmpeg_cmd, check=True)

print("\nDONE. Final video created:", FINAL_VIDEO_NAME)
