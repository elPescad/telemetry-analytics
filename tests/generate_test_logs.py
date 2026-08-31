import json
import time
from pathlib import Path

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
LOG_FILE = DATA_DIR / "hot_tier.log"

now = int(time.time())

# Real Database UUIDs
U1 = "0191f277-7048-4215-8b07-16d36eb2e60e"  # Freshman
U2 = "01a61d1d-dea4-4441-8988-3c0ced0c2a20"  # Sophomore
U3 = "062db9e6-8b7a-40b3-90aa-afee3f25c706"  # Junior
U4 = "0c21c95f-81c9-4450-913f-52b196db632a"  # Senior

E1 = "0177626a-5edf-48b0-9fcb-72c9f409895c"
E2 = "15be3604-799c-4eef-806b-fc32ade141d9"
E3 = "1761ac97-07be-4ade-8c9b-a8e4e3ad5474"

P1 = "00ffad30-ff90-438e-ab05-34caf5bd84f6"
P2 = "0156f852-1b49-4491-9838-3126720a2dd0"
P3 = "0182e6b7-768a-4fbb-99b0-9d7b07f3840d"

# Simulated User Activity Log Stream
events = [
    # User 1 (Freshman) Actions
    {"e": "view_post", "id": P1, "u": U1, "yr": "Freshman"},
    {"e": "like", "id": P1, "u": U1, "yr": "Freshman"},
    {"e": "view_comments", "id": P1, "u": U1, "yr": "Freshman"},
    {"e": "view_event", "id": E1, "u": U1, "yr": "Freshman"},
    {"e": "rsvp_success", "id": E1, "u": U1, "yr": "Freshman"},
    {"e": "qr_scan_success", "id": E1, "u": U1, "yr": "Freshman"},
    {"e": "create_post_start", "id": P3, "u": U1, "yr": "Freshman"},

    # User 2 (Sophomore) Actions
    {"e": "view_post", "id": P1, "u": U2, "yr": "Sophomore"},
    {"e": "like", "id": P1, "u": U2, "yr": "Sophomore"},
    {"e": "view_event", "id": E1, "u": U2, "yr": "Sophomore"},
    {"e": "rsvp_success", "id": E1, "u": U2, "yr": "Sophomore"},
    {"e": "qr_scan_success", "id": E1, "u": U2, "yr": "Sophomore"},
    {"e": "view_event", "id": E2, "u": U2, "yr": "Sophomore"},

    # User 3 (Junior) Actions
    {"e": "view_post", "id": P2, "u": U3, "yr": "Junior"},
    {"e": "like", "id": P2, "u": U3, "yr": "Junior"},
    {"e": "create_post_start", "id": P2, "u": U3, "yr": "Junior"},
    {"e": "view_event", "id": E2, "u": U3, "yr": "Junior"},
    {"e": "rsvp_success", "id": E2, "u": U3, "yr": "Junior"},

    # User 4 (Senior) Actions
    {"e": "view_event", "id": E3, "u": U4, "yr": "Senior"},
    {"e": "rsvp_success", "id": E3, "u": U4, "yr": "Senior"},
    {"e": "qr_scan_success", "id": E3, "u": U4, "yr": "Senior"},
    {"e": "view_post", "id": P3, "u": U4, "yr": "Senior"},
    {"e": "like", "id": P3, "u": U4, "yr": "Senior"},
]

with open(LOG_FILE, "w", encoding="utf-8") as f:
    for event in events:
        log_entry = {
            "timestamp": now,
            "payload": json.dumps(event)
        }
        f.write(json.dumps(log_entry) + "\n")

print(f"Generated {len(events)} real-ID test logs at {LOG_FILE}")