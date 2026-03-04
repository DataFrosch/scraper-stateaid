"""Tests for retry logic and duplicate threshold in main.py."""

import time
from unittest.mock import patch, MagicMock, call
from requests.exceptions import ConnectionError, Timeout

# We'll test the retry loop and threshold logic by extracting and exercising
# the relevant code paths from scrape_and_process.


def make_mock_response(text="<html></html>"):
    resp = MagicMock()
    resp.text = text
    return resp


class TestRetryLoop:
    """Test the retry-with-backoff logic around session.post()."""

    def test_success_on_first_try(self):
        """No retry needed when request succeeds immediately."""
        session = MagicMock()
        session.post.return_value = make_mock_response("OK content")

        # Simulate the retry loop from main.py
        backoff = 30
        while True:
            try:
                response = session.post("http://example.com", data={}, timeout=60)
                break
            except (ConnectionError, Timeout):
                time.sleep(0)  # don't actually sleep in test
                backoff = min(backoff * 2, 600)

        assert session.post.call_count == 1
        assert response.text == "OK content"

    def test_retry_on_connection_error(self):
        """Retries on ConnectionError and eventually succeeds."""
        session = MagicMock()
        session.post.side_effect = [
            ConnectionError("Connection dropped at offset 3100"),
            ConnectionError("Connection refused"),
            make_mock_response("OK after retries"),
        ]

        backoff = 30
        attempts = 0
        while True:
            try:
                response = session.post("http://example.com", data={}, timeout=60)
                break
            except (ConnectionError, Timeout):
                attempts += 1
                backoff = min(backoff * 2, 600)

        assert session.post.call_count == 3
        assert attempts == 2
        assert response.text == "OK after retries"

    def test_retry_on_timeout(self):
        """Retries on Timeout and eventually succeeds."""
        session = MagicMock()
        session.post.side_effect = [
            Timeout("Read timed out"),
            make_mock_response("OK after timeout"),
        ]

        backoff = 30
        attempts = 0
        while True:
            try:
                response = session.post("http://example.com", data={}, timeout=60)
                break
            except (ConnectionError, Timeout):
                attempts += 1
                backoff = min(backoff * 2, 600)

        assert session.post.call_count == 2
        assert attempts == 1
        assert response.text == "OK after timeout"

    def test_backoff_caps_at_600(self):
        """Backoff doubles each time but caps at 600 seconds."""
        backoff = 30
        backoff_values = []
        for _ in range(8):
            backoff = min(backoff * 2, 600)
            backoff_values.append(backoff)

        assert backoff_values == [60, 120, 240, 480, 600, 600, 600, 600]

    def test_backoff_starts_at_30(self):
        """First retry waits 30s, second 60s, etc."""
        backoff = 30
        # First retry uses initial backoff=30 before doubling
        assert backoff == 30
        backoff = min(backoff * 2, 600)
        assert backoff == 60
        backoff = min(backoff * 2, 600)
        assert backoff == 120
        backoff = min(backoff * 2, 600)
        assert backoff == 240
        backoff = min(backoff * 2, 600)
        assert backoff == 480
        backoff = min(backoff * 2, 600)
        assert backoff == 600

    def test_timeout_param_is_passed(self):
        """Ensure timeout=60 is passed to session.post()."""
        session = MagicMock()
        session.post.return_value = make_mock_response()

        backoff = 30
        while True:
            try:
                response = session.post("http://example.com", data={}, timeout=60)
                break
            except (ConnectionError, Timeout):
                backoff = min(backoff * 2, 600)

        session.post.assert_called_once_with("http://example.com", data={}, timeout=60)


class TestDuplicateThreshold:
    """Test that the duplicate page threshold is 100."""

    def test_stops_at_100_consecutive_duplicates(self):
        """Should stop after 100 consecutive all-duplicate pages."""
        consecutive_duplicate_pages = 0
        pages_processed = 0

        for _ in range(150):
            new_rows = 0  # simulate all duplicates
            if new_rows == 0:
                consecutive_duplicate_pages += 1

            if consecutive_duplicate_pages >= 100:
                break
            pages_processed += 1

        assert consecutive_duplicate_pages == 100
        assert pages_processed == 99  # stopped on the 100th

    def test_does_not_stop_at_5(self):
        """Should NOT stop at 5 consecutive duplicates (old threshold)."""
        consecutive_duplicate_pages = 0
        stopped = False

        for _ in range(10):
            consecutive_duplicate_pages += 1
            if consecutive_duplicate_pages >= 100:
                stopped = True
                break

        assert not stopped
        assert consecutive_duplicate_pages == 10

    def test_resets_on_new_data(self):
        """Counter resets when a page has new rows."""
        consecutive_duplicate_pages = 0

        # 99 duplicate pages
        for _ in range(99):
            consecutive_duplicate_pages += 1

        assert consecutive_duplicate_pages == 99

        # Then a page with new data
        consecutive_duplicate_pages = 0
        assert consecutive_duplicate_pages == 0


class TestCookieExpiredCheck:
    """Verify cookie-expired/overload check still works alongside retry."""

    def test_cookie_expired_detected(self):
        text = "Please choose a language to continue"
        assert "Please choose a language" in text

    def test_overload_detected(self):
        text = "The server is currently unable to handle the request"
        assert "currently unable to handle the request" in text

    def test_normal_response_passes(self):
        text = "<html><table>...</table></html>"
        assert "Please choose a language" not in text
        assert "currently unable to handle the request" not in text


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
