# Telemetry Analytics Pipeline

**[Source](https://github.com/elPescad/telemetry-analytics/tree/master/app)**

A high-performance, memory-optimized Python telemetry ingestion and analytics engine built with **Polars**, **Google Cloud Storage**, and **FPDF**. The pipeline streams compressed `.gz` log files from GCP, safely parses unnested NDJSON payloads using Rust-native JSON decoders, aggregates student activity metrics, and outputs both a JSON cache for frontend consumption and an executive PDF summary report.

---

### Core Architecture

* **`app/parser.py` (`LogParser`)**: Connects to GCP Storage, pre-filters log blobs by timestamp to reduce network I/O, safely decompresses GZIP streams in-memory, uses native Rust `json_decode` with type fallback, concatenates chunks diagonally (`how="diagonal"`) to defend against schema drift, and maps raw events into structured DataFrames.
* **`app/models.py` (`TelemetryRegistry`)**: Defines strongly typed dataclasses with safe default factories (`pl.DataFrame`) to guarantee non-null DataFrames across all categories (`posts`, `events`, `feed_ui`, `sessions`, `other`).
* **`app/analytics.py`**: Computes weighted user engagement leaderboards, handles student state transitions (e.g., class year updates), deduplicates monthly event attendance, and calculates post engagement rates.
* **`app/pdf_generator.py` (`ExecutiveAnalyticsPDF`)**: Renders `Monthly_Summary_Report.pdf` featuring auto-paginated metric tables, dynamic header re-printing, Latin-1/Unicode string sanitization, and structured leaderboards.
* **`main.py`**: Pipeline entry point that executes cloud ingestion, handles schedule triggers (Fridays for weekly reports, 1st of the month for monthly summaries), and exports both JSON and PDF report artifacts.

---

### Analytics Metrics & Formulas

| Metric | Calculation / Weighting | Target Events |
| :--- | :--- | :--- |
| **User Leaderboard Score** | `(Attended × 15) + (Posts × 5) + (Likes × 1)` | `qr_scan_success`, `create_post_start`, `like` |
| **Event Turnout %** | `(Actual Attended / RSVPs) × 100` | `qr_scan_success`, `rsvp_success` |
| **RSVP-to-Attendance Ratio** | `Total RSVPs / Actual Attended` | `rsvp_success`, `qr_scan_success` |
| **Post Engagement Rate %** | `((Likes + Comment Views) / Views) × 100` | `like`, `view_comments`, `view_post` |

---

### Environment & Setup

Ensure active Google Cloud credentials are configured in your environment (e.g., `GOOGLE_APPLICATION_CREDENTIALS`).

```bash
# Install dependencies
pip install polars google-cloud-storage fpdf2

# Set your GCP Bucket environment variable
export GCP_BUCKET_NAME="your-gcp-bucket-name"

# Run the pipeline
python main.py```
```
---
**Generated Pipeline Artifacts**
Running main.py evaluates date triggers and produces two primary outputs:
1. **analytics_cache.json**: Data cache containing:
     * last_updated: ISO-8859/UTC generation timestamp.
     * weekly: 7-day filtered event funnels, post metrics, and student scores.
     * monthly: 30-day aggregated and deduplicated performance rollups.
2. **Monthly_Summary_Report.pdf**: Printable executive document containing:
     * Top 25 Student Leaderboard: Ranked user activity scores and verified check-ins.
     * Top 12 Event Performance Summary: Views, RSVPs, actual attendance, and turnout percentages.
     * Top 15 Post Engagements: View counts, likes, comment views, and engagement ratios.
