#!/usr/bin/env python3
"""
generic_video_pipeline.py

A content-agnostic, reusable, config-driven video prompt + segmentation pipeline.

This module does NOT assume:
- any specific song, lyrics, character, camera rule, aesthetic, or prompt text
- any fixed segment length
- any specific generation provider (Hedra, etc.)

What it DOES provide:
- media loading + duration detection
- timestamped segment computation for arbitrary lengths
- script/lyrics ingestion and approximate mapping to segments (or timed LRC/SRT when provided)
- a modular PromptEngine abstraction that builds:
  - an optional MASTER_PROMPT (locked, global rules)
  - per-segment prompts that can evolve using an escalation curve and pacing profile
- structured exports to disk:
  - master_prompt.txt (optional)
  - script.txt (optional)
  - timestamps.json
  - segments.json (structured per-segment prompts + script mapping)
  - prompt_bundle.json (optional convenience artifact)

This file is intended to be the "core" prompt/segmentation layer. You can plug it into
any downstream renderer by passing a backend adapter, or by consuming the exported JSON.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple

from dotenv import load_dotenv


# =====================================================
# Env + Utilities
# =====================================================

load_dotenv()
load_dotenv(os.path.join(".venv", ".env"), override=True)


def _now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")


def normalize_env_path(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip().strip('"').strip("'")
    # Repair common backslash escape issues when .env uses quotes.
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


def sanitize_filename_component(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    sanitized = re.sub(r'[<>:"/\\\\|?*\\x00-\\x1f]', "_", value)
    sanitized = re.sub(r"\\s+", " ", sanitized).strip()
    return sanitized or None


def sha256_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_text_file(path: str, text: str) -> None:
    directory = os.path.dirname(path)
    if directory:
        ensure_dir(directory)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write((text or "").rstrip() + "\n")


def write_json_file(path: str, payload: Any) -> None:
    directory = os.path.dirname(path)
    if directory:
        ensure_dir(directory)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def read_json_file(path: str) -> Optional[Any]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def format_mmss(seconds: int) -> str:
    seconds = max(0, int(seconds))
    mm, ss = divmod(seconds, 60)
    return f"{mm:02d}:{ss:02d}"


# =====================================================
# Config Types
# =====================================================


@dataclass(frozen=True)
class VideoPipelineConfig:
    """
    Central, reusable configuration for segmentation + prompt generation.

    Any "direction logic" should live in this config (templates, profiles, curves),
    not hardcoded into the pipeline.
    """

    segment_duration_seconds: float = 8.0
    narrative_mode: str = "performance"  # e.g. "performance", "narrative", "montage", "documentary"
    style_profile: Dict[str, Any] = field(default_factory=dict)  # global look/feel knobs
    camera_mode: Dict[str, Any] = field(default_factory=dict)  # movement philosophy, lens, framing
    visual_rules: Dict[str, Any] = field(default_factory=dict)  # constraints (identity lock, text bans, etc.)
    anomaly_rules: Dict[str, Any] = field(default_factory=dict)  # anomaly escalation behaviors
    pacing_profile: Dict[str, Any] = field(default_factory=dict)  # rhythm / edit / beat mapping
    escalation_curve: str = "linear"  # "linear" | "ease_in" | "ease_out" | "ease_in_out" | "custom"
    custom_escalation: Optional[Callable[[float], float]] = None  # progress(0..1) -> intensity(0..1)
    output_format: Dict[str, Any] = field(default_factory=lambda: {"aspect_ratio": "9:16", "resolution": "720p"})

    # Prompt templates (optional). If empty, PromptEngine must override.
    master_prompt_template: str = ""
    segment_prompt_template: str = ""

    # Script mapping knobs (heuristic defaults; can be tuned per project).
    script_max_lines_per_segment: int = 4

    # Export knobs
    export_prompt_bundle: bool = True


@dataclass(frozen=True)
class MediaInfo:
    source_path: Optional[str]
    duration_ms: int
    media_type: str = "audio"  # "audio" | "video" | "unknown"
    sha256: Optional[str] = None


@dataclass(frozen=True)
class SegmentInfo:
    index: int  # 0-based
    start_ms: int
    end_ms: int

    @property
    def duration_ms(self) -> int:
        return max(0, int(self.end_ms - self.start_ms))

    @property
    def start_ts(self) -> str:
        return format_mmss(self.start_ms // 1000)

    @property
    def end_ts(self) -> str:
        return format_mmss(self.end_ms // 1000)

    @property
    def timestamp(self) -> str:
        return f"{self.start_ts}-{self.end_ts}"


@dataclass
class SegmentPrompt:
    segmentNumber: int
    timestamp: str
    intensity: float
    scriptLines: str
    segmentPrompt: str


# =====================================================
# Prompt Engine (Extensible)
# =====================================================


class PromptEngine:
    """
    A reusable prompt builder that accepts config + script input and produces:
    - an optional MASTER_PROMPT (global, locked rules)
    - per-segment prompts that can evolve with intensity / pacing

    Override methods for custom behaviors; do NOT embed story logic in the pipeline.
    """

    def __init__(self, config: VideoPipelineConfig):
        self.config = config

    def build_master_prompt(self, context: Dict[str, Any]) -> str:
        """
        Returns a global prompt string (may be empty). The default implementation uses
        config.master_prompt_template with str.format placeholders.
        """
        template = (self.config.master_prompt_template or "").strip()
        if not template:
            return ""
        return template.format(**context).strip()

    def build_segment_prompt(
        self,
        segment: SegmentInfo,
        script_text: str,
        intensity: float,
        master_prompt: str,
        context: Dict[str, Any],
    ) -> str:
        """
        Returns the assembled prompt for a segment. The default implementation uses
        config.segment_prompt_template with str.format placeholders and prepends MASTER_PROMPT when present.
        """
        template = (self.config.segment_prompt_template or "").strip()
        if not template:
            # Generic, content-agnostic fallback.
            template = (
                "SEGMENT {segment_number:02d} ({timestamp})\n"
                "Narrative mode: {narrative_mode}\n"
                "Intensity: {intensity:.2f}\n"
                "Script (do not render as on-screen text):\n{script}\n"
                "Direction: Maintain continuity with prior segment. No on-screen text or UI artifacts.\n"
            )

        payload = dict(context)
        payload.update(
            {
                "segment_number": segment.index + 1,
                "timestamp": segment.timestamp,
                "start_ts": segment.start_ts,
                "end_ts": segment.end_ts,
                "intensity": float(intensity),
                "narrative_mode": self.config.narrative_mode,
                "script": script_text or "",
            }
        )
        segment_block = template.format(**payload).strip()

        if master_prompt:
            return (master_prompt.strip() + "\n\n" + segment_block).strip()
        return segment_block

    def build_progressive_narrative(self, segments: List[SegmentPrompt], context: Dict[str, Any]) -> List[SegmentPrompt]:
        """
        Hook for advanced engines to post-process per-segment prompts (e.g. enforce continuity language,
        insert recurring motifs, or apply pacing rhythms). Default: return unchanged.
        """
        return segments


# =====================================================
# Script / Lyrics Parsing + Mapping
# =====================================================


def parse_script_or_lyrics(text: Optional[str]) -> str:
    """
    Returns the canonical script text (preserve formatting). This function is intentionally light-weight.
    """
    return (text or "").rstrip()


def normalize_script_lines(script_text: str) -> List[str]:
    if not script_text:
        return []
    lines: List[str] = []
    for raw in script_text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith(("#", "//")):
            continue
        lines.append(line)
    return lines


def map_script_lines_to_segments(
    lines: List[str],
    segment_count: int,
    max_lines_per_segment: int,
) -> List[str]:
    """
    Approximate mapping by evenly distributing script lines across segments.
    If you have timed data (LRC/SRT), plug a different mapping strategy here.
    """
    if segment_count <= 0:
        return []
    if not lines:
        return [""] * segment_count

    per_segment = max(1, math.ceil(len(lines) / segment_count))
    per_segment = min(per_segment, max(1, int(max_lines_per_segment)))

    out: List[str] = []
    cursor = 0
    for _ in range(segment_count):
        chunk = lines[cursor : cursor + per_segment]
        cursor += per_segment
        out.append("\n".join(chunk))
    if len(out) < segment_count:
        out.extend([""] * (segment_count - len(out)))
    return out[:segment_count]


# =====================================================
# Segment Computation
# =====================================================


def compute_segments(duration_ms: int, segment_duration_seconds: float) -> List[SegmentInfo]:
    """
    Compute consecutive segments for arbitrary media duration.
    Last segment may be shorter.
    """
    duration_ms = max(0, int(duration_ms))
    seg_ms = max(250, int(float(segment_duration_seconds) * 1000))
    if duration_ms <= 0:
        return []
    count = int(math.ceil(duration_ms / seg_ms))
    segments: List[SegmentInfo] = []
    for idx in range(count):
        start = idx * seg_ms
        end = min((idx + 1) * seg_ms, duration_ms)
        segments.append(SegmentInfo(index=idx, start_ms=start, end_ms=end))
    return segments


def build_timestamps_json(segments: List[SegmentInfo]) -> List[Dict[str, Any]]:
    return [
        {
            "segment": s.index + 1,
            "start": s.start_ts,
            "end": s.end_ts,
            "timestamp": s.timestamp,
            "start_ms": s.start_ms,
            "end_ms": s.end_ms,
            "duration_ms": s.duration_ms,
        }
        for s in segments
    ]


def _apply_escalation_curve(progress: float, config: VideoPipelineConfig) -> float:
    p = max(0.0, min(1.0, float(progress)))
    mode = (config.escalation_curve or "linear").strip().lower()
    if mode == "custom" and config.custom_escalation:
        try:
            return max(0.0, min(1.0, float(config.custom_escalation(p))))
        except Exception:
            return p
    if mode == "ease_in":
        return p * p
    if mode == "ease_out":
        return 1.0 - (1.0 - p) * (1.0 - p)
    if mode == "ease_in_out":
        return 2 * p * p if p < 0.5 else 1 - pow(-2 * p + 2, 2) / 2
    return p


# =====================================================
# Media Loading (Audio Duration)
# =====================================================


def load_media(path: Optional[str]) -> MediaInfo:
    """
    Load media metadata. Currently supports audio duration via pydub when a file path is provided.
    For video-only pipelines, pass duration via an external metadata source and skip this.
    """
    if not path:
        return MediaInfo(source_path=None, duration_ms=0, media_type="unknown", sha256=None)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Media not found: {path}")

    ext = os.path.splitext(path)[1].lower()
    media_type = "audio" if ext in (".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg") else "unknown"
    duration_ms = 0
    if media_type == "audio":
        from pydub import AudioSegment

        audio = AudioSegment.from_file(path)
        duration_ms = len(audio)
    else:
        # Unknown type: duration must be supplied externally if needed.
        duration_ms = 0

    return MediaInfo(source_path=path, duration_ms=duration_ms, media_type=media_type, sha256=sha256_file(path))


# =====================================================
# Export + Orchestration
# =====================================================


@dataclass(frozen=True)
class PipelineInputs:
    media_path: Optional[str] = None
    script_text: str = ""
    # Optional additional context that PromptEngine templates can reference.
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineOutputs:
    run_dir: str
    master_prompt_path: Optional[str]
    script_path: Optional[str]
    timestamps_path: str
    segments_path: str
    prompt_bundle_path: Optional[str]


def export_outputs(
    run_dir: str,
    master_prompt: str,
    script_text: str,
    timestamps: List[Dict[str, Any]],
    segments_payload: Dict[str, Any],
    export_prompt_bundle: bool,
) -> PipelineOutputs:
    ensure_dir(run_dir)

    master_prompt_path = None
    if master_prompt:
        master_prompt_path = os.path.join(run_dir, "master_prompt.txt")
        write_text_file(master_prompt_path, master_prompt)

    script_path = None
    if script_text:
        script_path = os.path.join(run_dir, "script.txt")
        write_text_file(script_path, script_text)

    timestamps_path = os.path.join(run_dir, "timestamps.json")
    segments_path = os.path.join(run_dir, "segments.json")
    write_json_file(timestamps_path, timestamps)
    write_json_file(segments_path, segments_payload)

    prompt_bundle_path = None
    if export_prompt_bundle:
        prompt_bundle_path = os.path.join(run_dir, "prompt_bundle.json")
        bundle = {
            "master_prompt": master_prompt,
            "script": script_text,
            "timestamps": timestamps,
            "segments": segments_payload.get("segments", []),
            "created_at": datetime.now().isoformat(),
        }
        write_json_file(prompt_bundle_path, bundle)

    return PipelineOutputs(
        run_dir=os.path.abspath(run_dir),
        master_prompt_path=os.path.abspath(master_prompt_path) if master_prompt_path else None,
        script_path=os.path.abspath(script_path) if script_path else None,
        timestamps_path=os.path.abspath(timestamps_path),
        segments_path=os.path.abspath(segments_path),
        prompt_bundle_path=os.path.abspath(prompt_bundle_path) if prompt_bundle_path else None,
    )


def build_prompt_package(
    inputs: PipelineInputs,
    config: VideoPipelineConfig,
    engine: Optional[PromptEngine] = None,
    run_dir: Optional[str] = None,
    segments_override: Optional[List[SegmentInfo]] = None,
    media_duration_ms_override: Optional[int] = None,
    media_sha256_override: Optional[str] = None,
) -> PipelineOutputs:
    """
    Core orchestration:
    - loads media duration (if provided)
    - computes segments from duration + config.segment_duration_seconds
    - parses and maps script to segments
    - builds master prompt (optional)
    - builds per-segment prompts with escalation curve
    - exports structured artifacts to disk
    """
    engine = engine or PromptEngine(config)

    media = MediaInfo(None, 0, "unknown", None)
    if segments_override is not None:
        segments = list(segments_override)
        duration_ms = max(0, int(media_duration_ms_override or (segments[-1].end_ms if segments else 0)))
        media = MediaInfo(
            source_path=inputs.media_path,
            duration_ms=duration_ms,
            media_type="unknown",
            sha256=(media_sha256_override or (inputs.metadata or {}).get("media_sha256")),
        )
    else:
        if media_duration_ms_override is not None:
            duration_ms = max(0, int(media_duration_ms_override))
            media = MediaInfo(
                source_path=inputs.media_path,
                duration_ms=duration_ms,
                media_type="unknown",
                sha256=(media_sha256_override or (inputs.metadata or {}).get("media_sha256")),
            )
        elif inputs.media_path:
            media = load_media(inputs.media_path)
            if media.duration_ms <= 0:
                raise ValueError(
                    "Unable to determine media duration. Provide a supported audio file or supply duration externally."
                )
        else:
            raise ValueError(
                "No media duration available. Provide media_path, media_duration_ms_override, or segments_override."
            )

        segments = compute_segments(media.duration_ms, config.segment_duration_seconds)
    timestamps = build_timestamps_json(segments)

    script_text = parse_script_or_lyrics(inputs.script_text)
    lines = normalize_script_lines(script_text)
    script_by_segment = map_script_lines_to_segments(lines, len(segments), config.script_max_lines_per_segment)

    context: Dict[str, Any] = {
        "media_path": inputs.media_path or "",
        "media_sha256": media.sha256 or "",
        "media_duration_ms": media.duration_ms,
        "segment_duration_seconds": float(config.segment_duration_seconds),
        "segment_count": len(segments),
        "narrative_mode": config.narrative_mode,
        "style_profile": config.style_profile,
        "camera_mode": config.camera_mode,
        "visual_rules": config.visual_rules,
        "anomaly_rules": config.anomaly_rules,
        "pacing_profile": config.pacing_profile,
        **(inputs.metadata or {}),
    }

    master_prompt = engine.build_master_prompt(context)

    segment_prompts: List[SegmentPrompt] = []
    for seg in segments:
        progress = 0.0 if len(segments) <= 1 else seg.index / float(len(segments) - 1)
        intensity = _apply_escalation_curve(progress, config)
        mapped_script = script_by_segment[seg.index] if seg.index < len(script_by_segment) else ""
        assembled = engine.build_segment_prompt(seg, mapped_script, intensity, master_prompt, context)
        segment_prompts.append(
            SegmentPrompt(
                segmentNumber=seg.index + 1,
                timestamp=seg.timestamp,
                intensity=float(intensity),
                scriptLines=mapped_script or "",
                segmentPrompt=assembled,
            )
        )

    segment_prompts = engine.build_progressive_narrative(segment_prompts, context)

    segments_payload = {
        "created_at": datetime.now().isoformat(),
        "pipeline": "generic_video_pipeline",
        "config": dataclasses.asdict(config),
        "media": dataclasses.asdict(media),
        "MASTER_PROMPT": master_prompt,
        "segments": [dataclasses.asdict(s) for s in segment_prompts],
    }

    if not run_dir:
        run_dir = os.path.join("runs", _now_stamp())

    return export_outputs(
        run_dir=run_dir,
        master_prompt=master_prompt,
        script_text=script_text,
        timestamps=timestamps,
        segments_payload=segments_payload,
        export_prompt_bundle=bool(config.export_prompt_bundle),
    )


def main() -> None:
    """
    Minimal CLI entry point for local testing.
    Use env vars to supply inputs/config:
      - MEDIA_PATH
      - SCRIPT_TEXT (optional) or SCRIPT_FILE (optional)
      - SEGMENT_DURATION_SECONDS
      - RUN_DIR (optional)
    """
    media_path = normalize_env_path(os.environ.get("MEDIA_PATH"))
    script_file = normalize_env_path(os.environ.get("SCRIPT_FILE"))
    script_text = os.environ.get("SCRIPT_TEXT", "")
    if script_file and os.path.exists(script_file):
        script_text = Path(script_file).read_text(encoding="utf-8", errors="replace")

    seg_seconds = float(os.environ.get("SEGMENT_DURATION_SECONDS", "8"))
    run_dir = normalize_env_path(os.environ.get("RUN_DIR")) or os.path.join("runs", _now_stamp())

    config = VideoPipelineConfig(segment_duration_seconds=seg_seconds)
    outputs = build_prompt_package(PipelineInputs(media_path=media_path, script_text=script_text), config=config, run_dir=run_dir)
    print("Wrote outputs to:", outputs.run_dir)
    print("segments.json:", outputs.segments_path)
    print("timestamps.json:", outputs.timestamps_path)


if __name__ == "__main__":
    main()
