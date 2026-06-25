"""Pure metric functions for the Silver and Gold layers.

Every function here is a plain Python function — no Spark, no Kafka, no Delta.
This makes them fast to unit-test without any infrastructure.
"""
from __future__ import annotations


def match_second(minute: int, second: int) -> int:
    return minute * 60 + second

def match_minute_label(minute: int, period: int) -> str:
    period_ends = {1: 45, 2: 90, 3: 90, 4: 105}
    base = period_ends.get(period, 105)
    if minute > base:
        return f"{base}+{minute - base}"
    return str(minute)

ATTACKING_TYPES = frozenset({
    "Shot", "Dribble", "Carry", "Pass", "Ball Receipt*",
    "Dribbled Past", "Miscontrol",
})

def is_shot(event_type: str) -> bool:
    return event_type == "Shot"

def is_goal(event_type: str, shot_outcome) -> bool:
    return event_type == "Shot" and shot_outcome == "Goal"

def is_attacking(event_type: str) -> bool:
    return event_type in ATTACKING_TYPES

def safe_xg(xg_value) -> float:
    if xg_value is None:
        return 0.0
    return max(0.0, float(xg_value))

def win_probability(goal_diff: int, xg_diff: float, minutes_remaining: float) -> float:
    import math
    minutes_remaining = max(0, min(90, minutes_remaining))
    time_factor = 1.0 - (minutes_remaining / 90.0)
    goal_weight = 1.5 + 2.0 * time_factor
    xg_weight = 0.6 * (1.0 - time_factor)
    logit = goal_weight * goal_diff + xg_weight * xg_diff
    return round(1.0 / (1.0 + math.exp(-logit)), 4)
