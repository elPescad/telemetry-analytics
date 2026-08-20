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
    parser = LogParser("hot_tier.log")
    try:
        cleaned_df = parser.parse_events()
        
        #ALWAYS RUN: Real-time Student Leaderboard (All-Time or Active)
        student_leaderboard = compute_student_weighted_scores(cleaned_df)
        print("--- STUDENT LEADERBOARD ---")
        print(student_leaderboard)

        #WEEKLY REPORT: Runs on Fridays over a 7-day lookback window
        if is_weekly_report_day():
            print("\n--- FRIDAY WEEKLY SUMMARY (LAST 7 DAYS) ---")
            weekly_df = filter_by_days(cleaned_df, days=7)
            
            print("Weekly Events Performance:")
            print(compute_event_performance(weekly_df))
            
            print("Weekly Posts Engagement:")
            print(compute_post_performance(weekly_df))

        #MONTHLY REPORT: Runs on the 1st of the month over a 28-day lookback window
        if is_monthly_report_day():
            print("\n--- MONTHLY SUMMARY (LAST 28 DAYS / 4 WEEKS) ---")
            monthly_df = filter_by_days(cleaned_df, days=28)
            
            print("Monthly Events Overview:")
            print(compute_event_performance(monthly_df))
            
            print("Monthly Top Students:")
            print(compute_student_weighted_scores(monthly_df))

    except FileNotFoundError as e:
        print(f"Log file error: {e}")
    except Exception as e:
        print(f"Pipeline error: {e}")

if __name__ == "__main__":
    main()