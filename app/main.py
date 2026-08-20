import json
import os
from datetime import datetime, timezone
from app.parser import LogParser
from app.analytics import (
    filter_by_days,
    is_weekly_report_day,
    is_monthly_report_day,
    compute_student_weighted_scores,
    compute_event_performance,
    compute_post_performance,
)

def main():
    bucket_name = os.getenv("GCP_BUCKET_NAME")
    if not bucket_name:
        print("Error: GCP_BUCKET_NAME environment variable is not set.")
        return

    try:
        # 1. Load exclusively from GCP Storage via static method
        cleaned_df = LogParser.load_cloud_telemetry(bucket_name, max_lookback_days=28)
        
        if cleaned_df.is_empty():
            print("No telemetry data retrieved from cloud.")
            return

        # 2. Compute Metrics Across Time Windows
        student_leaderboard = compute_student_weighted_scores(cleaned_df)
        
        weekly_df = filter_by_days(cleaned_df, days=7)
        weekly_events = compute_event_performance(weekly_df)
        weekly_posts = compute_post_performance(weekly_df)

        monthly_df = filter_by_days(cleaned_df, days=28)
        monthly_events = compute_event_performance(monthly_df)
        monthly_posts = compute_post_performance(monthly_df)

        # 3. Terminal Output Logging
        print("--- STUDENT LEADERBOARD (CLOUD DATA) ---")
        print(student_leaderboard)

        if is_weekly_report_day():
            print("\n--- FRIDAY WEEKLY SUMMARY (LAST 7 DAYS) ---")
            print("Weekly Events Performance:\n", weekly_events)
            print("Weekly Posts Engagement:\n", weekly_posts)

        if is_monthly_report_day():
            print("\n--- MONTHLY SUMMARY (LAST 28 DAYS) ---")
            print("Monthly Events Overview:\n", monthly_events)

        # 4. Export Structured JSON Cache for Frontend API
        payload = {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "student_leaderboard": student_leaderboard.to_dicts(),
            "weekly": {
                "events": weekly_events.to_dicts(),
                "posts": weekly_posts.to_dicts(),
            },
            "monthly": {
                "events": monthly_events.to_dicts(),
                "posts": monthly_posts.to_dicts(),
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