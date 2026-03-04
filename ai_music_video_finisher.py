import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

import cv2
import numpy as np


def run(cmd):
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)


def ensure_tool(name: str) -> str:
    path = shutil.which(name)
    if not path:
        raise RuntimeError(f"Required tool not found on PATH: {name}")
    return path


def extract_frames(input_video: Path, frames_dir: Path):
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(input_video),
        "-qscale:v",
        "1",
        str(frames_dir / "frame_%06d.png"),
    ]
    run(cmd)


def upscale_frames(frames_dir: Path, upscaled_dir: Path, scale: int):
    cmd = [
        "realesrgan-ncnn-vulkan",
        "-i",
        str(frames_dir),
        "-o",
        str(upscaled_dir),
        "-n",
        "realesrgan-x4plus",
        "-s",
        str(scale),
    ]
    run(cmd)


def temporal_smoothing(input_dir: Path, output_dir: Path):
    frame_files = sorted(input_dir.glob("*.png"))
    prev_frame = None
    for i, frame_path in enumerate(frame_files):
        img = cv2.imread(str(frame_path))
        if img is None:
            raise RuntimeError(f"Failed to read frame: {frame_path}")
        if prev_frame is not None:
            img = cv2.addWeighted(img, 0.7, prev_frame, 0.3, 0)
        out_path = output_dir / f"frame_{i:06d}.png"
        cv2.imwrite(str(out_path), img)
        prev_frame = img


def motion_blur(input_dir: Path, output_dir: Path):
    frame_files = sorted(input_dir.glob("*.png"))
    kernel = np.zeros((15, 15), dtype=np.float32)
    kernel[int((15 - 1) / 2), :] = 1.0
    kernel /= kernel.sum()
    for i, frame_path in enumerate(frame_files):
        img = cv2.imread(str(frame_path))
        if img is None:
            raise RuntimeError(f"Failed to read frame: {frame_path}")
        img_blur = cv2.filter2D(img, -1, kernel)
        out_path = output_dir / f"frame_{i:06d}.png"
        cv2.imwrite(str(out_path), img_blur)


def add_film_grain(input_dir: Path, output_dir: Path):
    frame_files = sorted(input_dir.glob("*.png"))
    for i, frame_path in enumerate(frame_files):
        img = cv2.imread(str(frame_path))
        if img is None:
            raise RuntimeError(f"Failed to read frame: {frame_path}")
        grain = np.random.normal(0, 10, img.shape).astype(np.float32)
        img_float = img.astype(np.float32)
        img_grain = np.clip(img_float * 0.9 + grain * 0.1, 0, 255).astype(np.uint8)
        out_path = output_dir / f"frame_{i:06d}.png"
        cv2.imwrite(str(out_path), img_grain)


def assemble_video(grain_dir: Path, output_master: Path, fps: int):
    cmd = [
        "ffmpeg",
        "-y",
        "-framerate",
        str(fps),
        "-i",
        str(grain_dir / "frame_%06d.png"),
        "-c:v",
        "prores_ks",
        "-profile:v",
        "3",
        str(output_master),
    ]
    run(cmd)


def export_final(output_master: Path, output_final: Path):
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(output_master),
        "-c:v",
        "libx264",
        "-preset",
        "slow",
        "-crf",
        "15",
        "-b:v",
        "50M",
        str(output_final),
    ]
    run(cmd)


def main():
    parser = argparse.ArgumentParser(description="AI music video finishing pipeline")
    parser.add_argument("--input", default="input_video.mp4")
    parser.add_argument("--work-dir", default="video_pipeline")
    parser.add_argument("--output-master", default="master_video.mov")
    parser.add_argument("--output-final", default="final_video.mp4")
    parser.add_argument("--fps", type=int, default=24)
    parser.add_argument("--scale", type=int, default=2)
    args = parser.parse_args()

    ensure_tool("ffmpeg")
    ensure_tool("realesrgan-ncnn-vulkan")

    input_video = Path(args.input).resolve()
    if not input_video.exists():
        raise FileNotFoundError(f"Input video not found: {input_video}")

    work_dir = Path(args.work_dir).resolve()
    frames_dir = work_dir / "frames"
    upscaled_dir = work_dir / "frames_upscaled"
    temporal_dir = work_dir / "frames_temporal"
    blur_dir = work_dir / "frames_blur"
    grain_dir = work_dir / "frames_grain"

    frames_dir.mkdir(parents=True, exist_ok=True)
    upscaled_dir.mkdir(parents=True, exist_ok=True)
    temporal_dir.mkdir(parents=True, exist_ok=True)
    blur_dir.mkdir(parents=True, exist_ok=True)
    grain_dir.mkdir(parents=True, exist_ok=True)

    print("Starting AI music video finishing pipeline")

    extract_frames(input_video, frames_dir)
    upscale_frames(frames_dir, upscaled_dir, args.scale)
    temporal_smoothing(upscaled_dir, temporal_dir)
    motion_blur(temporal_dir, blur_dir)
    add_film_grain(blur_dir, grain_dir)

    output_master = Path(args.output_master).resolve()
    output_final = Path(args.output_final).resolve()
    assemble_video(grain_dir, output_master, args.fps)
    export_final(output_master, output_final)

    print("Finished. Output:", output_final)


if __name__ == "__main__":
    main()
