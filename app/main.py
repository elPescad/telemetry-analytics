import json
import os
from pathlib import Path
from datetime import datetime, timezone
import polars as pl
from app.parser import LogParser
from app.analytics import (
    filter_by_days,
    is_weekly_report_day,
    is_monthly_report_day,
    compute_student_weighted_scores,
    compute_event_performance,
    compute_post_performance,
    compute_monthly_student_scores,
    compute_monthly_event_performance,
    compute_monthly_post_performance,
)

SUMMARIES_DIR = Path("summaries")

def main():
    bucket_name = os.getenv("GCP_BUCKET_NAME")
    if not bucket_name:
        print("Error: GCP_BUCKET_NAME environment variable is not set.")
        return

    SUMMARIES_DIR.mkdir(exist_ok=True)

    try:
        # 1. Fetch raw logs from GCP
        cleaned_df = LogParser.load_cloud_telemetry(bucket_name, max_lookback_days=14)
        
        if cleaned_df.is_empty():
            print("No telemetry data retrieved from cloud.")
            return

        # 2. Compute 7-Day Weekly Metrics
        weekly_raw_df = filter_by_days(cleaned_df, days=7)
        weekly_students = compute_student_weighted_scores(weekly_raw_df)
        weekly_events = compute_event_performance(weekly_raw_df)
        weekly_posts = compute_post_performance(weekly_raw_df)

        # 3. Save Weekly Parquet Summaries
        current_date = datetime.now(timezone.utc)
        week_key = current_date.strftime("%Y_w%U")

        student_summary_file = SUMMARIES_DIR / f"students_{week_key}.parquet"
        event_summary_file = SUMMARIES_DIR / f"events_{week_key}.parquet"
        post_summary_file = SUMMARIES_DIR / f"posts_{week_key}.parquet"
        
        weekly_students.write_parquet(student_summary_file)
        weekly_events.write_parquet(event_summary_file)
        weekly_posts.write_parquet(post_summary_file)

        # 4. Compute Monthly Rollups from Last 4 Weekly Summaries
        monthly_students = pl.DataFrame()
        monthly_events = pl.DataFrame()
        monthly_posts = pl.DataFrame()
        post_of_the_month = {}

        if is_monthly_report_day():
            student_files = sorted(SUMMARIES_DIR.glob("students_*.parquet"))[-4:]
            event_files = sorted(SUMMARIES_DIR.glob("events_*.parquet"))[-4:]
            post_files = sorted(SUMMARIES_DIR.glob("posts_*.parquet"))[-4:]

            if student_files and event_files and post_files:
                monthly_students = compute_monthly_student_scores([pl.read_parquet(f) for f in student_files])
                monthly_events = compute_monthly_event_performance([pl.read_parquet(f) for f in event_files])
                monthly_posts = compute_monthly_post_performance([pl.read_parquet(f) for f in post_files])

                if not monthly_posts.is_empty():
                    post_of_the_month = monthly_posts.head(1).to_dicts()[0]

        # 5. Terminal Output Logging
        print("--- WEEKLY STUDENT LEADERBOARD ---")
        print(weekly_students)

        if is_weekly_report_day():
            print("\n--- FRIDAY WEEKLY SUMMARY ---")
            print("Weekly Events Performance:\n", weekly_events)
            print("Weekly Posts Engagement:\n", weekly_posts)

        if is_monthly_report_day() and not monthly_posts.is_empty():
            print("\n--- MONTHLY SUMMARY ---")
            print("Monthly Student Leaderboard:\n", monthly_students)
            print("Monthly Events Overview:\n", monthly_events)
            print("Monthly Posts Ranking:\n", monthly_posts)
            print("\nPOST OF THE MONTH:", post_of_the_month)

        # 6. Export Structured JSON Cache for Frontend API
        payload = {
            "last_updated": current_date.isoformat(),
            "student_leaderboard": weekly_students.to_dicts(),
            "weekly": {
                "events": weekly_events.to_dicts(),
                "posts": weekly_posts.to_dicts(),
            },
            "monthly": {
                "students": monthly_students.to_dicts() if not monthly_students.is_empty() else [],
                "events": monthly_events.to_dicts() if not monthly_events.is_empty() else [],
                "posts": monthly_posts.to_dicts() if not monthly_posts.is_empty() else [],
                "post_of_the_month": post_of_the_month,
            }
        }

        output_path = "analytics_cache.json"
        with open(output_path, "w") as f:
            json.dump(payload, f, indent=2)

        print(f"\nSuccessfully wrote frontend cache payload to {output_path}")

    except Exception as e:
        print(f"Pipeline error loading from cloud: {e}")

if __name__ == "__main__":
    main()