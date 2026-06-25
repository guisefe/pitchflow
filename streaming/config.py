"""Configuration for the streaming layer (Spark jobs)."""
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
MATCH_TOPIC = os.getenv("MATCH_TOPIC", "match.events")

LAKEHOUSE_DIR = os.getenv("LAKEHOUSE_DIR", "data/delta")
BRONZE_PATH = f"{LAKEHOUSE_DIR}/bronze/events"
SILVER_PATH = f"{LAKEHOUSE_DIR}/silver/events"
GOLD_DIR = f"{LAKEHOUSE_DIR}/gold"

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "data/checkpoints")
