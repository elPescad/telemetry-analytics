from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from app.pdf_generator import generate_monthly_pdf

BASE_DIR = Path(__file__).resolve().parent.parent
CACHE_FILE = BASE_DIR / "data" / "analytics_cache.json"
PDF_FILE = BASE_DIR / "data" / "Monthly_Executive_Summary.pdf"

app = FastAPI(title="Campus Telemetry Analytics API")

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
    # Automatically generate PDF if it doesn't exist yet
    if not PDF_FILE.exists():
        if CACHE_FILE.exists():
            generate_monthly_pdf(str(CACHE_FILE), str(PDF_FILE))
        else:
            raise HTTPException(status_code=533, detail="Cache file missing; cannot generate PDF.")
            
    return FileResponse(
        PDF_FILE,
        media_type="application/pdf",
        filename="Monthly_Executive_Summary.pdf"
    )