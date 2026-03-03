from pathlib import Path

from mirror_mouth_pipeline import (
    build_session_video_folder,
    load_prompt_text,
    normalize_env_path,
    save_versioned_video_copy,
)


def test_normalize_env_path_strips_quotes_and_normalizes():
    value = '"D:/ffmpeg/bin/ffmpeg.exe"'
    normalized = normalize_env_path(value)
    assert normalized.endswith(str(Path("D:/ffmpeg/bin/ffmpeg.exe")))


def test_load_prompt_text_reads_file(tmp_path):
    prompt_path = tmp_path / "prompt.txt"
    prompt_path.write_text("hello world\n", encoding="utf-8")

    assert load_prompt_text(str(prompt_path)) == "hello world"


def test_build_session_video_folder_creates_directory(tmp_path):
    session_dir = build_session_video_folder(str(tmp_path), session_label="session-123")

    assert Path(session_dir).exists()
    assert Path(session_dir).name == "session-123"


def test_build_session_video_folder_reuses_resume_session(tmp_path):
    session_dir = build_session_video_folder(str(tmp_path), resume_session="resume-me")

    assert Path(session_dir).exists()
    assert Path(session_dir).name == "resume-me"


def test_build_session_video_folder_does_not_double_prefix_output_root(tmp_path):
    output_root = tmp_path / "video_segments"
    resume_session = output_root / "2026-03-02_16-31-48_504286"

    session_dir = build_session_video_folder(str(output_root), resume_session=str(resume_session))

    assert Path(session_dir) == resume_session


def test_save_versioned_video_copy_increments_versions(tmp_path):
    final_video = tmp_path / "final_music_video.mp4"
    final_video.write_bytes(b"video-bytes")
    input_mp3 = tmp_path / "my_song.mp3"
    input_mp3.write_bytes(b"audio-bytes")
    archive_folder = tmp_path / "final_videos"

    first_copy = save_versioned_video_copy(str(final_video), str(input_mp3), str(archive_folder))
    second_copy = save_versioned_video_copy(str(final_video), str(input_mp3), str(archive_folder))

    assert Path(first_copy).name == "my_song.v1.mp4"
    assert Path(second_copy).name == "my_song.v2.mp4"
