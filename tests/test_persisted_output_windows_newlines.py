"""Windows text-mode Hermes results can have CRLF-expanded file contents."""

import tempfile

import pytest

from hermes_lcm.ingest_protection import recover_hermes_persisted_output_with_file_stat


def _marker(path, original: str, preview: str | None = None) -> str:
    if preview is None:
        preview = original[:30]
    return (
        "<persisted-output>\n"
        f"This tool result was too large ({len(original):,} characters, 1.0 KB).\n"
        f"Full output saved to: {path}\n"
        "Preview (first 30 chars):\n"
        f"{preview}\n...\n"
        "</persisted-output>"
    )


@pytest.mark.parametrize(
    "original",
    [
        "first\nsecond",
        "first\r\nsecond",
        "first\rsecond\nthird",
        "first\r\r\nsecond\n",
        "中文🙂\nsecond\n",
        "first\n\n",
        "one line",
    ],
)
def test_windows_text_mode_newline_expansion_recovers_original(tmp_path, monkeypatch, original):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    result_dir = tmp_path / "hermes-results"
    result_dir.mkdir()
    path = result_dir / "result.txt"
    path.write_bytes(original.replace("\n", "\r\n").encode("utf-8"))

    recovered = recover_hermes_persisted_output_with_file_stat(_marker(path, original))
    assert recovered is not None
    assert recovered[0] == original
    assert recovered[1]["size"] == len(path.read_bytes())


def test_untouched_lf_file_is_not_normalized(tmp_path, monkeypatch):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    result_dir = tmp_path / "hermes-results"
    result_dir.mkdir()
    path = result_dir / "result.txt"
    original = "first\r\nsecond\nthird"
    path.write_bytes(original.encode("utf-8"))

    recovered = recover_hermes_persisted_output_with_file_stat(_marker(path, original))
    assert recovered is not None
    assert recovered[0] == original


@pytest.mark.parametrize(
    "raw, original, preview",
    [
        ("one\r\ntwoX", "one\ntwo", None),
        ("one\ntwo\r", "one\ntwo", None),
        ("other\r\ntwo", "one\ntwo", None),
        ("one\r\ntwo", "one\ntwo", "wrong"),
    ],
)
def test_windows_newline_recovery_rejects_tampering(tmp_path, monkeypatch, raw, original, preview):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    result_dir = tmp_path / "hermes-results"
    result_dir.mkdir()
    path = result_dir / "result.txt"
    path.write_bytes(raw.encode("utf-8"))

    assert recover_hermes_persisted_output_with_file_stat(
        _marker(path, original, preview)
    ) is None
