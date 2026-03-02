
import os
import math
import time
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
    "Authorization": f"Bearer {API_KEY}"
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

SEGMENT_LENGTH_SECONDS = 8
OUTPUT_AUDIO_FOLDER = "segments"
OUTPUT_VIDEO_FOLDER = "video_segments"
FINAL_VIDEO_NAME = "final_music_video.mp4"
API_BASE = "https://api.hedra.com/web-app/public"
API_URL = "https://api.hedra.com/web-app/public"
API_KEY = os.environ.get("API_KEY")
HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}

# =====================================================
# CREATE FOLDERS
# =====================================================

os.makedirs(OUTPUT_AUDIO_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_VIDEO_FOLDER, exist_ok=True)

# =====================================================
# SPLIT AUDIO
# =====================================================

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
        "ai_model_id": "d1dd37a3-e39a-4854-a298-6510289f9cf2",
        "generated_video_inputs": {
            "text_prompt": PROMPT_TEXT,
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
        raise Exception(f"No video URL returned: {status_json}")

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

with open("videos.txt", "w") as f:
    for video in video_files:
        f.write(f"file '{os.path.abspath(video)}'\n")

subprocess.run([
    "ffmpeg",
    "-f", "concat",
    "-safe", "0",
    "-i", "videos.txt",
    "-c", "copy",
    FINAL_VIDEO_NAME
])

print("\nDONE. Final video created:", FINAL_VIDEO_NAME)


# # =============================
# STITCH VIDEOS
# =============================

print("Stitching final video...")

with open("videos.txt", "w") as f:
    for v in video_files:
        f.write(f"file '{os.path.abspath(v)}'\n")

subprocess.run([
    "ffmpeg",
    "-f", "concat",
    "-safe", "0",
    "-i", "videos.txt",
    "-c", "copy",
    FINAL_VIDEO_NAME
])

print("DONE.")

    
