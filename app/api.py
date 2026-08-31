import os
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from starlette.status import HTTP_403_FORBIDDEN
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from app.main import main as run_pipeline
from app.pdf_generator import generate_monthly_pdf

BASE_DIR = Path(__file__).resolve().parent.parent
CACHE_FILE = BASE_DIR / "data" / "analytics_cache.json"
PDF_FILE = BASE_DIR / "data" / "Monthly_Executive_Summary.pdf"

# API Key Authentication Setup
load_dotenv()

EXPECTED_API_KEY = os.getenv("API_SECRET_KEY", "fallback-dev-key")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(api_key: str = Security(api_key_header)):
    if not api_key or api_key != EXPECTED_API_KEY:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="Could not validate API Key"
        )

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Run pipeline in a background thread on container boot
    try:
        await asyncio.to_thread(run_pipeline)
    except Exception as e:
        print(f"Startup pipeline run failed: {e}")

    # 2. Schedule daily execution at midnight UTC
    scheduler.add_job(run_pipeline, "cron", hour=0, minute=0)
    scheduler.start()
    
    yield
    
    scheduler.shutdown()

# Initialize FastAPI ONCE with lifespan attached
app = FastAPI(title="Campus Telemetry Analytics API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/analytics", dependencies=[Depends(verify_api_key)])
async def get_analytics():
    if not CACHE_FILE.exists():
        raise HTTPException(status_code=503, detail="Analytics cache file not yet generated.")
    return FileResponse(
        CACHE_FILE, 
        media_type="application/json",
        headers={"Cache-Control": "public, max-age=300"}
    )

@app.get("/analytics/pdf", dependencies=[Depends(verify_api_key)])
async def get_analytics_pdf():
    if not PDF_FILE.exists():
        if CACHE_FILE.exists():
            generate_monthly_pdf(str(CACHE_FILE), str(PDF_FILE))
        else:
            raise HTTPException(status_code=503, detail="Cache file missing; cannot generate PDF.")
            
    return FileResponse(
        PDF_FILE,
        media_type="application/pdf",
        filename="Monthly_Executive_Summary.pdf"
    )