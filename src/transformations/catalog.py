def normalize_artist_name(name):
    if name is None:
        return None
    return str(name).strip().title()


def validate_track_schema(track):
    errors = []

    required_fields = ["id", "artist_id", "title", "duration_ms", "genre"]

    for field in required_fields:
        if field not in track or track[field] in [None, ""]:
            errors.append(f"missing_field:{field}")

    duration = track.get("duration_ms")
    if duration is not None:
        try:
            duration = int(duration)
            if duration <= 0:
                errors.append("invalid_duration_ms")
            if duration > 36_000_000:
                errors.append("duration_too_long")
        except Exception:
            errors.append("invalid_duration_ms")

    return errors


def deduplicate_artists(artists):
    seen = set()
    result = []

    for artist in artists:
        normalized_name = normalize_artist_name(artist.get("name"))
        label = artist.get("label")
        key = (normalized_name, label)

        if key in seen:
            continue

        seen.add(key)

        clean_artist = {
            **artist,
            "name": normalized_name,
        }

        result.append(clean_artist)

    return result