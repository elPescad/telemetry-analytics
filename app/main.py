import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import polars as pl

from app.analytics import (
    compute_event_performance,
    compute_monthly_event_performance,
    compute_monthly_post_performance,
    compute_monthly_student_scores,
    compute_post_performance,
    compute_student_weighted_scores,
    filter_by_days,
    is_monthly_report_day,
    is_weekly_report_day,
)
from app.parser import LogParser
from app.pdf_generator import generate_monthly_pdf

# Production Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

SUMMARIES_DIR = Path("summaries")
CACHE_FILE = Path("analytics_cache.json")


def get_aligned_weekly_summaries(max_weeks: int = 4):
    """
    Safely retrieves up to `max_weeks` parquet files guaranteed to share 
    matching week keys across all three telemetry categories.
    """
    student_map = {f.name.replace("students_", ""): f for f in SUMMARIES_DIR.glob("students_*.parquet")}
    event_map = {f.name.replace("events_", ""): f for f in SUMMARIES_DIR.glob("events_*.parquet")}
    post_map = {f.name.replace("posts_", ""): f for f in SUMMARIES_DIR.glob("posts_*.parquet")}

    # Find week keys present across all 3 sets
    common_weeks = sorted(set(student_map.keys()) & set(event_map.keys()) & set(post_map.keys()))[-max_weeks:]

    if not common_weeks:
        return [], [], []

    s_files = [student_map[w] for w in common_weeks]
    e_files = [event_map[w] for w in common_weeks]
    p_files = [post_map[w] for w in common_weeks]

    return s_files, e_files, p_files


def main():
    bucket_name = os.getenv("GCP_BUCKET_NAME")
    if not bucket_name:
        logging.critical("GCP_BUCKET_NAME environment variable is not set.")
        sys.exit(1)

    SUMMARIES_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # 1. Fetch raw telemetry logs from GCP
        logging.info("Fetching cloud telemetry logs...")
        cleaned_df = LogParser.load_cloud_telemetry(bucket_name, max_lookback_days=14)

        if cleaned_df is None or cleaned_df.is_empty():
            logging.warning("No telemetry data retrieved from cloud. Exiting pipeline.")
            return

        # 2. Compute 7-Day Weekly Metrics
        logging.info("Computing 7-day weekly analytics metrics...")
        weekly_raw_df = filter_by_days(cleaned_df, days=7)
        weekly_students = compute_student_weighted_scores(weekly_raw_df)
        weekly_events = compute_event_performance(weekly_raw_df)
        weekly_posts = compute_post_performance(weekly_raw_df)

        # 3. Save Weekly Parquet Summaries (Using ISO Year & Week formatting)
        current_date = datetime.now(timezone.utc)
        iso_year, iso_week, _ = current_date.isocalendar()
        week_key = f"{iso_year}_w{iso_week:02d}"

        student_summary_file = SUMMARIES_DIR / f"students_{week_key}.parquet"
        event_summary_file = SUMMARIES_DIR / f"events_{week_key}.parquet"
        post_summary_file = SUMMARIES_DIR / f"posts_{week_key}.parquet"

        # Guard against saving 0-column dataframes which crash Parquet engines and downstream concats
        if weekly_students.width > 0 and weekly_events.width > 0 and weekly_posts.width > 0:
            weekly_students.write_parquet(student_summary_file)
            weekly_events.write_parquet(event_summary_file)
            weekly_posts.write_parquet(post_summary_file)
            logging.info(f"Successfully saved weekly parquet summaries for week {week_key}")
        else:
            logging.warning(f"Insufficient data to generate parquet summaries for week {week_key}. Skipping write.")

        # 4. Handle Monthly Rollups & Cache Preservation
        monthly_students_dict = []
        monthly_events_dict = []
        monthly_posts_dict = []
        post_of_the_month = {}

        # Load existing cache to preserve monthly data on non-monthly calculation days
        existing_cache = {}
        if CACHE_FILE.exists():
            try:
                # CRITICAL: Encoding added to prevent UnicodeDecodeError on validation load
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    existing_cache = json.load(f)
            except Exception as e:
                logging.warning(f"Could not parse existing cache file: {e}")

        s_files, e_files, p_files = get_aligned_weekly_summaries(max_weeks=4)

        if is_monthly_report_day() or not existing_cache.get("monthly", {}).get("students"):
            if s_files and e_files and p_files:
                logging.info(f"Computing monthly rollups across {len(s_files)} weekly summary files...")
                m_students_df = compute_monthly_student_scores([pl.read_parquet(f) for f in s_files])
                m_events_df = compute_monthly_event_performance([pl.read_parquet(f) for f in e_files])
                m_posts_df = compute_monthly_post_performance([pl.read_parquet(f) for f in p_files])

                monthly_students_dict = m_students_df.to_dicts() if m_students_df.width > 0 else []
                monthly_events_dict = m_events_df.to_dicts() if m_events_df.width > 0 else []
                monthly_posts_dict = m_posts_df.to_dicts() if m_posts_df.width > 0 else []

                # Safely extract post of the month
                if m_posts_df.width > 0 and not m_posts_df.is_empty():
                    top_posts = m_posts_df.head(1).to_dicts()
                    post_of_the_month = top_posts[0] if top_posts else {}
        else:
            # Preserve existing monthly cache if not a monthly calculation day
            logging.info("Preserving existing monthly rollup cache.")
            monthly_data = existing_cache.get("monthly", {})
            monthly_students_dict = monthly_data.get("students", [])
            monthly_events_dict = monthly_data.get("events", [])
            monthly_posts_dict = monthly_data.get("posts", [])
            post_of_the_month = monthly_data.get("post_of_the_month", {})

        # 5. Terminal Logging
        logging.info("--- WEEKLY STUDENT LEADERBOARD ---")
        print(weekly_students)

        if is_weekly_report_day():
            logging.info("--- FRIDAY WEEKLY SUMMARY ---")
            print("Weekly Events Performance:\n", weekly_events)
            print("Weekly Posts Engagement:\n", weekly_posts)

        if is_monthly_report_day() and monthly_posts_dict:
            logging.info("--- MONTHLY SUMMARY ---")
            print("Post of the Month:", post_of_the_month)

        # 6. Export Structured JSON Cache (Atomic Write)
        payload = {
            "last_updated": current_date.isoformat(),
            "student_leaderboard": weekly_students.to_dicts() if weekly_students.width > 0 else [],
            "weekly": {
                "events": weekly_events.to_dicts() if weekly_events.width > 0 else [],
                "posts": weekly_posts.to_dicts() if weekly_posts.width > 0 else [],
            },
            "monthly": {
                "students": monthly_students_dict,
                "events": monthly_events_dict,
                "posts": monthly_posts_dict,
                "post_of_the_month": post_of_the_month,
            },
        }

        tmp_cache_path = CACHE_FILE.with_suffix(".tmp")
        with open(tmp_cache_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        # Atomic file replace prevents partial JSON read race conditions
        tmp_cache_path.replace(CACHE_FILE)
        logging.info(f"Successfully wrote updated frontend cache payload to {CACHE_FILE}")

        # 7. Generate Executive PDF Report if Monthly Trigger active
        if is_monthly_report_day():
            logging.info("Generating Monthly Executive PDF Report...")
            generate_monthly_pdf(
                json_cache_path=str(CACHE_FILE),
                output_pdf="Monthly_Executive_Summary.pdf",
            )

    except Exception as e:
        logging.critical("Fatal error executing telemetry analytics pipeline", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()