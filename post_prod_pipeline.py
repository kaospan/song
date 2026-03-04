import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
load_dotenv(os.path.join(".venv", ".env"), override=True)

INPUT_VIDEO = os.environ.get("POSTPROD_INPUT_VIDEO", "final_music_video.mp4")
WORK_DIR = os.environ.get("POSTPROD_WORK_DIR", "post_prod_pipeline")
OUTPUT_MASTER = os.environ.get("POSTPROD_OUTPUT_MASTER", "post_master_video.mov")
OUTPUT_FINAL = os.environ.get("POSTPROD_OUTPUT_FINAL", "post_final_video.mp4")
FPS = int(os.environ.get("POSTPROD_FPS", "24"))
SCALE = int(os.environ.get("POSTPROD_SCALE", "2"))


def main():
    input_path = Path(INPUT_VIDEO).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input video not found: {input_path}")

    script_path = Path(__file__).resolve().parent / "ai_music_video_finisher.py"
    if not script_path.exists():
        raise FileNotFoundError(f"Finisher script not found: {script_path}")

    cmd = [
        sys.executable,
        str(script_path),
        "--input",
        str(input_path),
        "--work-dir",
        str(Path(WORK_DIR).resolve()),
        "--output-master",
        str(Path(OUTPUT_MASTER).resolve()),
        "--output-final",
        str(Path(OUTPUT_FINAL).resolve()),
        "--fps",
        str(FPS),
        "--scale",
        str(SCALE),
    ]
    print("Running post-prod pipeline:", " ".join(cmd))
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
