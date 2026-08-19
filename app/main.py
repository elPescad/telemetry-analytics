# app/main.py
from app.parser import LogParser
from app.analytics import compute_student_weighted_scores


def main():
    parser = LogParser("hot_tier.log")
    
    #Parse and clean
    cleaned_df = parser.parse_events()

    #Organize into registry struct
    registry = parser.organize_events(cleaned_df)
    
    #Compute per student weighted aggregations
    student_scores_df = compute_student_weighted_scores(cleaned_df)

    print("--- WEIGHTED STUDENT SCOREBOARD ---")
    print(student_scores_df)

if __name__ == "__main__":
    main()