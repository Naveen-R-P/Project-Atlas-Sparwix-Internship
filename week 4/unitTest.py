import pytest
from anomaly_detector import AnomalyDetector


@pytest.fixture
def detector():
    return AnomalyDetector()


def base_event(**overrides):
    """
    Creates a valid base event and allows overrides
    """
    event = {
        "user_id": "123e4567-e89b-12d3-a456-426614174000",
        "action": "view_page",
        "page": "dashboard",
        "duration_ms": 1200,
        "device_type": "web",
        "timestamp": "2025-01-01T10:00:00Z",
        "referrer": None,
        "session_id": "223e4567-e89b-12d3-a456-426614174111",
        "is_high_risk": False,
    }
    event.update(overrides)
    return event


# -----------------------------
# Positive Anomaly Cases
# -----------------------------

def test_high_risk_flag_triggers_anomaly(detector):
    event = base_event(is_high_risk=True)
    assert detector.is_anomalous(event) is True


def test_high_risk_action_triggers_anomaly(detector):
    event = base_event(action="password_reset")
    assert detector.is_anomalous(event) is True


def test_long_duration_triggers_anomaly(detector):
    event = base_event(duration_ms=9000)
    assert detector.is_anomalous(event) is True


# -----------------------------
# Negative (Normal) Cases
# -----------------------------

def test_normal_event_not_anomalous(detector):
    event = base_event()
    assert detector.is_anomalous(event) is False


def test_duration_at_threshold_not_anomalous(detector):
    event = base_event(duration_ms=5000)
    assert detector.is_anomalous(event) is False


# -----------------------------
# Edge Cases
# -----------------------------

def test_missing_duration_defaults_to_safe(detector):
    event = base_event()
    del event["duration_ms"]
    assert detector.is_anomalous(event) is False


def test_missing_action_defaults_to_safe(detector):
    event = base_event()
    del event["action"]
    assert detector.is_anomalous(event) is False


def test_empty_event_safe(detector):
    assert detector.is_anomalous({}) is False