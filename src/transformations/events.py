from datetime import datetime, timezone


def is_valid_listening_event(event):
    required_fields = [
        "event_id",
        "user_id",
        "track_id",
        "source_peer",
        "timestamp",
        "duration_ms",
    ]

    for field in required_fields:
        if field not in event or event[field] in [None, ""]:
            return False

    try:
        duration_ms = int(event["duration_ms"])
        if duration_ms < 5_000:
            return False
        if duration_ms > 36_000_000:
            return False
    except Exception:
        return False

    try:
        timestamp = str(event["timestamp"]).replace("Z", "+00:00")
        event_time = datetime.fromisoformat(timestamp)

        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)

        if event_time > now:
            return False
    except Exception:
        return False

    return True