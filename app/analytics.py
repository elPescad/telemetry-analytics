import polars as pl
from datetime import datetime, timedelta, timezone

# Point Weights Configuration
WEIGHT_RSVP = 10
WEIGHT_POST = 5
WEIGHT_LIKE = 1

def compute_student_weighted_scores(cleaned_df: pl.DataFrame) -> pl.DataFrame:
    """
    Groups events by student, aggregates counts, computes weighted scores,
    and returns a sorted student leaderboard.
    """
    return (
        cleaned_df
        .filter(pl.col("user_id").is_not_null())
        .group_by(["user_id", "college_year"])
        .agg(
            pl.col("event_type").filter(pl.col("event_type") == "rsvp_success").count().alias("rsvp_count"),
            pl.col("event_type").filter(pl.col("event_type") == "create_post_start").count().alias("post_count"),
            pl.col("event_type").filter(pl.col("event_type") == "like").count().alias("like_count"),
        )
        .with_columns(
            (pl.col("rsvp_count") * WEIGHT_RSVP).alias("rsvp_pts"),
            (pl.col("post_count") * WEIGHT_POST).alias("post_pts"),
            (pl.col("like_count") * WEIGHT_LIKE).alias("like_pts"),
        )
        .with_columns(
            (pl.col("rsvp_pts") + pl.col("post_pts") + pl.col("like_pts")).alias("total_score")
        )
        .sort("total_score", descending=True)
    )