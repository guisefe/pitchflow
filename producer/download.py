"""Download StatsBomb open-data match events and cache them locally.

The replay producer reads from the local cache so it never re-downloads on
every run and can replay offline once a match has been fetched.

Data source: StatsBomb Open Data (https://github.com/statsbomb/open-data).
"""
import json
import logging
from pathlib import Path

import requests

from producer.config import DATA_DIR, STATSBOMB_EVENTS_URL

logger = logging.getLogger(__name__)


def cache_path(match_id: int) -> Path:
    return Path(DATA_DIR) / f"events_{match_id}.json"


def download_match(match_id: int, timeout: int = 30) -> list[dict]:
    """Return the event list for a match, downloading + caching on first use."""
    path = cache_path(match_id)

    if path.exists():
        logger.info("Loading match %d from cache: %s", match_id, path)
        return json.loads(path.read_text())

    url = STATSBOMB_EVENTS_URL.format(match_id=match_id)
    logger.info("Downloading match %d from %s", match_id, url)

    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    events = response.json()

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(events))
    logger.info("Cached %d events to %s", len(events), path)

    return events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from producer.config import MATCH_ID

    events = download_match(MATCH_ID)
    logger.info("Match %d has %d events", MATCH_ID, len(events))
