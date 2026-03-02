from pathlib import Path

from mirror_mouth_pipeline import build_session_video_folder, load_prompt_text, normalize_env_path


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
