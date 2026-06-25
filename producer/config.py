"""Configuration for the replay producer.

All values are overridable via environment variables (see .env.example) so the
same image runs locally, in Docker, and in CI without code changes.
"""
import os

# Kafka / Redpanda
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
MATCH_TOPIC = os.getenv("MATCH_TOPIC", "match.events")

# StatsBomb open-data match to replay. Default: 2022 World Cup Final
# (Argentina 3-3 France), which goes to extra time — plenty of drama.
MATCH_ID = int(os.getenv("MATCH_ID", "3869685"))

# Replay speed multiplier. 60 => one minute of match time per real second,
# so a ~90-minute match streams in ~90 seconds.
REPLAY_SPEED = float(os.getenv("REPLAY_SPEED", "60"))

# Cap the real-time sleep between events so half-time / stoppage gaps don't
# stall the stream for long stretches.
MAX_SLEEP_SECONDS = float(os.getenv("MAX_SLEEP_SECONDS", "3"))

# StatsBomb open-data raw file location.
STATSBOMB_EVENTS_URL = (
    "https://raw.githubusercontent.com/statsbomb/open-data/master/"
    "data/events/{match_id}.json"
)

# Local cache directory for downloaded matches.
DATA_DIR = os.getenv("DATA_DIR", "data")
