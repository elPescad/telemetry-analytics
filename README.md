# Telemetry Analytics Pipeline

A fast, memory-optimized Python telemetry ingestion and analytics engine built with **Polars** and **Google Cloud Storage**. The pipeline streams compressed `.gz` log files from GCP, safely parses unnested NDJSON payloads, aggregates student activity metrics, and outputs a JSON cache for frontend API consumption.

[Source](https://github.com/elPescad/telemetry-analytics/tree/master/app)
---

**Core Architecture**

* **`app/parser.py` (`LogParser`)**: Connects to GCP Storage, filters log files by a 28-day timestamp cutoff to minimize bandwidth, decompresses stream data in memory, handles corrupted JSON rows, and maps raw event schema to normalized Polars DataFrames.
* **`app/models.py` (`TelemetryRegistry`)**: Defines a strongly typed dataclass storing categorized DataFrames (`posts`, `events`, `feed_ui`, `sessions`, `other`).
* **`app/analytics.py`**: Computes weighted user engagement leaderboards, event funnel conversion rates, post engagement percentages, and time-windowing logic.
* **`main.py`**: Entry point executing cloud ingestion, running time-based checks (Fridays for weekly reports, 1st of the month for monthly reports), logging terminal summaries, and caching results to disk.

---

**Analytics Metrics & Formulas**

| Metric | Calculation / Weighting | Target Events |
| :--- | :--- | :--- |
| **User Leaderboard Score** | `(RSVPs × 10) + (Posts × 5) + (Likes × 1)` | `rsvp_success`, `create_post_start`, `like` |
| **Event Conversion Rate** | `(RSVPs / Views) * 100` | `view_event`, `rsvp_click`, `rsvp_success`, `share_event` |
| **Post Engagement Rate** | `((Likes + Comment Views) / Views) * 100` | `view_post`, `like`, `view_comments` |

---

**Environment & Setup**

Ensure active Google Cloud credentials are set in your environment (e.g., `GOOGLE_APPLICATION_CREDENTIALS`).

```bash
# Dependencies
pip install polars google-cloud-storage

# Set your GCP Bucket environment variable
export GCP_BUCKET_NAME="your-gcp-bucket-name"

# Run the pipeline
python main.py
```
---
**Pipeline Output**
Running main.py produces terminal summary logs based on calendar triggers and exports analytics_cache.json containing:
* `last_updated`: ISO-formatted UTC timestamp.
* `student_leaderboard`: Full ranked student scores over the last 28 days.
* `weekly`: 7-day post engagement and event funnel metrics.
* `monthly`: 28-day post engagement and event funnel metrics.
