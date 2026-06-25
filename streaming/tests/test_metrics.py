import pytest
from streaming.metrics import (
    match_second, match_minute_label,
    is_shot, is_goal, is_attacking, safe_xg, win_probability,
)

def test_match_second_kickoff(): assert match_second(0, 0) == 0
def test_match_second_mid(): assert match_second(1, 30) == 90
def test_label_normal(): assert match_minute_label(32, 2) == "32"
def test_label_stoppage(): assert match_minute_label(47, 1) == "45+2"
def test_label_extra_time(): assert match_minute_label(96, 3) == "90+6"
def test_is_shot(): assert is_shot("Shot") and not is_shot("Pass")
def test_is_goal(): assert is_goal("Shot", "Goal") and not is_goal("Shot", "Saved")
def test_is_attacking(): assert is_attacking("Shot") and not is_attacking("Foul Committed")
def test_safe_xg(): assert safe_xg(None) == 0.0 and safe_xg(-0.1) == 0.0
def test_win_prob_level(): assert win_probability(0, 0.0, 45) == pytest.approx(0.5, abs=0.001)
def test_win_prob_leading(): assert win_probability(1, 0.5, 45) > 0.5
def test_win_prob_bounded():
    for d in [-3, 0, 3]:
        assert 0.0 <= win_probability(d, 0.0, 45) <= 1.0
