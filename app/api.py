from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

BASE_DIR = Path(__file__).resolve().parent.parent
CACHE_FILE = BASE_DIR / "data" / "analytics_cache.json"

app = FastAPI(title="Campus Telemetry Analytics API")

# Allow requests from Expo / React Native clients
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/analytics")
async def get_analytics():
    if not CACHE_FILE.exists():
        raise HTTPException(status_code=503, detail="Analytics cache file not yet generated.")
    
    return FileResponse(
        CACHE_FILE, 
        media_type="application/json",
        headers={"Cache-Control": "public, max-age=300"}
    )