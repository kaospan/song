
import os
import math
import time
import requests
import subprocess
from pydub import AudioSegment
from tqdm import tqdm

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

def masked_key(key):
    """Return a partially-masked key string safe for diagnostic output."""
    if not key:
        return "<empty>"
    if len(key) < 8:
        return "<too short>"
    return f"{key[:4]}...{key[-4:]}"

if not API_KEY:
    raise ValueError("API_KEY environment variable not set")

print(f"[config] API_KEY loaded: {masked_key(API_KEY)}")

HEADERS = {
    "X-API-Key": API_KEY,          # required by Hedra API
    "Authorization": f"Bearer {API_KEY}",  # optional fallback
}

# =====================================================
# UPLOAD HELPERS  (Hedra two-step asset upload)
# =====================================================

def _initiate_upload(filename: str, media_type: str) -> tuple:
    """Step 1: ask Hedra for a pre-signed upload URL.

    Returns (asset_id, upload_url).
    """
    resp = requests.post(
        f"{API_BASE}/assets",
        headers={**HEADERS, "Content-Type": "application/json"},
        json={"file_name": filename, "media_type": media_type},
        timeout=30,
    )
    print(f"  [init_upload] status={resp.status_code}")
    resp.raise_for_status()
    data = resp.json()
    asset_id = data.get("id")
    upload_url = data.get("upload_url") or data.get("signed_url")
    missing = [f for f, v in [("id", asset_id), ("upload_url/signed_url", upload_url)] if not v]
    if missing:
        raise ValueError(f"Upload-init response missing fields {missing}; keys present: {list(data.keys())}")
    return asset_id, upload_url


def _put_to_signed_url(upload_url: str, file_path: str, media_type: str) -> None:
    """Step 2: PUT the raw file bytes to the pre-signed URL."""
    file_size = os.path.getsize(file_path)
    print(f"  [put_upload] uploading {os.path.basename(file_path)} "
          f"({file_size} bytes) to signed URL…")
    with open(file_path, "rb") as fh:
        resp = requests.put(
            upload_url,
            data=fh,
            headers={"Content-Type": media_type, "Content-Length": str(file_size)},
            timeout=120,
        )
    print(f"  [put_upload] status={resp.status_code}")
    resp.raise_for_status()


def upload_asset(file_path: str, media_type: str) -> str:
    """Two-step Hedra asset upload.

    1. POST /assets (JSON) → pre-signed URL + asset ID
    2. PUT file bytes to the pre-signed URL

    Returns the asset ID string.
    """
    filename = os.path.basename(file_path)
    print(f"  [upload_asset] initiating upload: {filename} ({media_type})")
    asset_id, upload_url = _initiate_upload(filename, media_type)
    print(f"  [upload_asset] asset_id={asset_id}")
    _put_to_signed_url(upload_url, file_path, media_type)
    print(f"  [upload_asset] upload complete: {filename} → {asset_id}")
    return asset_id

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
# VALIDATE INPUTS
# =====================================================

if not os.path.isfile(INPUT_MP3):
    raise FileNotFoundError(f"Input audio file not found: {INPUT_MP3}")

if not os.path.isfile(REFERENCE_IMAGE):
    raise FileNotFoundError(f"Reference image not found: {REFERENCE_IMAGE}")

print(f"Inputs validated: {INPUT_MP3}, {REFERENCE_IMAGE}")

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

    filename = os.path.join(OUTPUT_AUDIO_FOLDER, f"segment_{i:03}.mp3")
    segment.export(filename, format="mp3", bitrate="320k")
    audio_files.append(filename)

print(f"{len(audio_files)} segments created.")

# =====================================================
# PROCESS EACH SEGMENT SEQUENTIALLY
# =====================================================

video_files = []

for idx, audio_file in enumerate(audio_files):
    print(f"\nProcessing segment {idx+1}/{len(audio_files)}")

    # ---- Upload Image (two-step) ----
    image_id = upload_asset(REFERENCE_IMAGE, "image/png")
    print("Image uploaded:", image_id)

    # ---- Upload Audio (two-step) ----
    audio_id = upload_asset(audio_file, "audio/mpeg")
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
        },
        timeout=60
    )

    generation_response.raise_for_status()
    generation_data = generation_response.json()

    job_id = generation_data.get("id") or generation_data.get("generation_id")

    if not job_id:
        raise Exception(f"No job_id returned: {generation_data}")

    print("Generation started. Job ID:", job_id)

    # ---- Poll Until Completed ----
    while True:
        status_response = requests.get(
            f"{API_BASE}/generations/{job_id}",
            headers=HEADERS,
            timeout=30
        )

        status_response.raise_for_status()
        status_json = status_response.json()

        status = status_json.get("status")
        print("Status:", status)

        if status == "completed":
            break
        elif status in ["failed", "error"]:
            raise Exception(f"Generation failed: {status_json}")

        time.sleep(10)

    # ---- Download Video ----
    video_url = status_json.get("output", {}).get("video_url")

    if not video_url:
        raise Exception(f"No video URL returned: {status_json}")

    print("Downloading video...")

    video_path = os.path.join(OUTPUT_VIDEO_FOLDER, f"video_{idx:03}.mp4")
    with requests.get(video_url, stream=True, timeout=120) as video_resp:
        video_resp.raise_for_status()
        with open(video_path, "wb") as f:
            for chunk in video_resp.iter_content(chunk_size=8192):
                f.write(chunk)

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

# Re-encode to H.264/AAC to normalize codecs and avoid concat copy issues.
subprocess.run([
    "ffmpeg",
    "-f", "concat",
    "-safe", "0",
    "-i", "videos.txt",
    "-c:v", "libx264",
    "-crf", "23",
    "-preset", "medium",
    "-c:a", "aac",
    "-b:a", "128k",
    FINAL_VIDEO_NAME
], check=True)

print("\nDONE. Final video created:", FINAL_VIDEO_NAME)

