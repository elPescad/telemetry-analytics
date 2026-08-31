import json
import os
import urllib.request
import urllib.parse
from typing import Dict, Any, List

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

def _fetch_supabase_table(table_name: str, ids: List[str], select_cols: str) -> List[Dict[str, Any]]:
    if not ids or not SUPABASE_URL or not SUPABASE_KEY:
        return []

    ids_param = f"in.({','.join(ids)})"
    params = urllib.parse.urlencode({"id": ids_param, "select": select_cols})
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table_name}?{params}"

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as e:
        print(f"Warning: Failed to fetch {table_name} metadata from Supabase: {e}")
        return []

def enrich_cache_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Resolves UUIDs in payload to names, titles, and descriptions using Supabase."""
    
    # 1. Collect all unique IDs
    student_ids = list({s["user_id"] for s in payload.get("student_leaderboard", []) if "user_id" in s})
    
    events_list = payload.get("weekly", {}).get("events", []) + payload.get("monthly", {}).get("events", [])
    event_ids = list({e["event_id"] for e in events_list if "event_id" in e})
    
    posts_list = payload.get("weekly", {}).get("posts", []) + payload.get("monthly", {}).get("posts", [])
    post_ids = list({p["post_id"] for p in posts_list if "post_id" in p})

    # 2. Query Supabase Tables
    users_data = _fetch_supabase_table("users", student_ids, "id,first_name,last_name")
    events_data = _fetch_supabase_table("events", event_ids, "id,name")
    posts_data = _fetch_supabase_table("posts", post_ids, "id,content")

    # Build Mapping Lookups
    user_map = {
        u["id"]: f"{u.get('first_name', '')} {u.get('last_name', '')}".strip() + f" ({u['id'][:8]})"
        for u in users_data
    }
    event_map = {e["id"]: e.get("name", "Unknown Event") for e in events_data}
    post_map = {p["id"]: (p.get("content", "")[:40] + "..." if len(p.get("content", "")) > 40 else p.get("content", "Post")) for p in posts_data}

    # 3. Hydrate Student Leaderboard
    for student in payload.get("student_leaderboard", []):
        uid = student.get("user_id", "")
        student["display_name"] = user_map.get(uid, f"User ({uid[:8]})")

    # 4. Hydrate Weekly & Monthly Events
    for section in ["weekly", "monthly"]:
        for evt in payload.get(section, {}).get("events", []):
            eid = evt.get("event_id", "")
            evt["name"] = event_map.get(eid, f"Event ({eid[:8]})")

    # 5. Hydrate Weekly & Monthly Posts
    for section in ["weekly", "monthly"]:
        for pst in payload.get(section, {}).get("posts", []):
            pid = pst.get("post_id", "")
            pst["content_snippet"] = post_map.get(pid, f"Post ({pid[:8]})")

    return payload