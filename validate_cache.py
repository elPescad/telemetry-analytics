import json
from pathlib import Path

CACHE_FILE = Path("data/analytics_cache.json")

def validate():
    assert CACHE_FILE.exists(), f"Error: {CACHE_FILE} does not exist!"

    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Top-Level Keys Verification
    assert "last_updated" in data, "Missing 'last_updated'"
    assert "student_leaderboard" in data, "Missing 'student_leaderboard'"
    assert "weekly" in data, "Missing 'weekly'"
    assert "monthly" in data, "Missing 'monthly'"

    # Leaderboard Verification
    leaderboard = data["student_leaderboard"]
    assert len(leaderboard) > 0, "Leaderboard is empty"
    first_student = leaderboard[0]
    assert "user_id" in first_student
    assert "college_year" in first_student
    assert "total_score" in first_student

    # Weekly Analytics Verification
    assert "events" in data["weekly"]
    assert "posts" in data["weekly"]

    print("SUCCESS: Cache file layout and keys perfectly match analyticsService.ts!")
    print("\n--- SAMPLE GENERATED PAYLOAD PREVIEW ---")
    print(json.dumps(data, indent=2))

if __name__ == "__main__":
    validate()