from pathlib import Path
from app.pdf_generator import generate_monthly_pdf

DATA_DIR = Path("data")
CACHE_FILE = DATA_DIR / "analytics_cache.json"
OUTPUT_PDF = DATA_DIR / "Monthly_Executive_Summary.pdf"

print("Generating PDF report from cache...")
generate_monthly_pdf(
    json_cache_path=str(CACHE_FILE),
    output_pdf=str(OUTPUT_PDF)
)

if OUTPUT_PDF.exists():
    print(f"SUCCESS: Generated PDF report at {OUTPUT_PDF} ({OUTPUT_PDF.stat().st_size} bytes)")
else:
    print("ERROR: PDF file was not created.")