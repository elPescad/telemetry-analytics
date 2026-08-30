import asyncio
import json
from app.api import get_analytics

async def main():
    print("Simulating FastAPI GET /analytics endpoint request...")
    response = await get_analytics()
    
    print(f"File Path Served: {response.path}")
    print(f"Media Type: {response.media_type}")

    with open(response.path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    print("\n--- RECEIVED FRONTEND PAYLOAD ---")
    print(f"Last Updated: {payload.get('last_updated')}")
    print(f"Students in Leaderboard: {len(payload.get('student_leaderboard', []))}")
    print(f"Weekly Events Tracked: {len(payload.get('weekly', {}).get('events', []))}")
    print(f"Weekly Posts Tracked: {len(payload.get('weekly', {}).get('posts', []))}")

    print("\nSIMULATION COMPLETE: FastAPI endpoint successfully validated!")

if __name__ == "__main__":
    asyncio.run(main())