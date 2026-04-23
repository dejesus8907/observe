from pathlib import Path


def test_truth_api_client_has_auth_retry_pagination_and_streaming_support():
    api_path = Path(__file__).resolve().parents[3] / "dashboard" / "src" / "api" / "truthApi.ts"
    text = api_path.read_text(encoding="utf-8")
    assert "Authorization" in text
    assert "X-Session-Id" in text
    assert "DEFAULT_RETRIES" in text
    assert "fetchPage" in text
    assert "subscribeStreamingEvents" in text
    assert "new WebSocket" in text
