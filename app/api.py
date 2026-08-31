from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.main import main as run_pipeline
from app.pdf_generator import generate_monthly_pdf

BASE_DIR = Path(__file__).resolve().parent.parent
CACHE_FILE = BASE_DIR / "data" / "analytics_cache.json"
PDF_FILE = BASE_DIR / "data" / "Monthly_Executive_Summary.pdf"

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Run pipeline immediately on container boot
    try:
        run_pipeline()
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

@app.get("/analytics")
async def get_analytics():
    if not CACHE_FILE.exists():
        raise HTTPException(status_code=503, detail="Analytics cache file not yet generated.")
    return FileResponse(
        CACHE_FILE, 
        media_type="application/json",
        headers={"Cache-Control": "public, max-age=300"}
    )

@app.get("/analytics/pdf")
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