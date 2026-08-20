import polars as pl
from datetime import datetime, timedelta, timezone

# Point Weights Configuration
WEIGHT_RSVP = 10
WEIGHT_POST = 5
WEIGHT_LIKE = 1

# --- TIME WINDOW HELPERS ---

def filter_by_days(cleaned_df: pl.DataFrame, days: int) -> pl.DataFrame:
    """Filters logs to only include entries from the last N days based on server_timestamp."""
    cutoff_epoch = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    return cleaned_df.filter(pl.col("server_timestamp") >= cutoff_epoch)

def is_weekly_report_day() -> bool:
    """Returns True if today is Friday (weekday 4)."""
    return datetime.now(timezone.utc).weekday() == 4

def is_monthly_report_day() -> bool:
    """Returns True if today is the 1st of the month."""
    return datetime.now(timezone.utc).day == 1


# --- AGGREGATION ENGINES ---

def compute_student_weighted_scores(cleaned_df: pl.DataFrame) -> pl.DataFrame:
    """Calculates weighted points per student and returns a ranked leaderboard."""
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

def compute_event_performance(cleaned_df: pl.DataFrame) -> pl.DataFrame:
    """Aggregates event funnel metrics (views, clicks, RSVPs, shares) and conversion rates."""
    return (
        cleaned_df
        .filter(pl.col("target_id").is_not_null())
        .filter(pl.col("event_type").is_in(["view_event", "rsvp_click", "rsvp_success", "share_event"]))
        .group_by("target_id")
        .agg(
            pl.col("event_type").filter(pl.col("event_type") == "view_event").count().alias("views"),
            pl.col("event_type").filter(pl.col("event_type") == "rsvp_click").count().alias("clicks"),
            pl.col("event_type").filter(pl.col("event_type") == "rsvp_success").count().alias("rsvps"),
            pl.col("event_type").filter(pl.col("event_type") == "share_event").count().alias("shares"),
        )
        .with_columns(
            pl.when(pl.col("views") > 0)
            .then((pl.col("rsvps") / pl.col("views") * 100).round(1))
            .otherwise(0.0)
            .alias("conversion_rate_pct")
        )
        .sort("rsvps", descending=True)
    )

def compute_post_performance(cleaned_df: pl.DataFrame) -> pl.DataFrame:
    """Aggregates post engagement metrics (views, likes, comments) and engagement rate."""
    return (
        cleaned_df
        .filter(pl.col("target_id").is_not_null())
        .filter(pl.col("event_type").is_in(["view_post", "like", "view_comments"]))
        .group_by("target_id")
        .agg(
            pl.col("event_type").filter(pl.col("event_type") == "view_post").count().alias("views"),
            pl.col("event_type").filter(pl.col("event_type") == "like").count().alias("likes"),
            pl.col("event_type").filter(pl.col("event_type") == "view_comments").count().alias("comment_views"),
        )
        .with_columns(
            pl.when(pl.col("views") > 0)
            .then(((pl.col("likes") + pl.col("comment_views")) / pl.col("views") * 100).round(1))
            .otherwise(0.0)
            .alias("engagement_rate_pct")
        )
        .sort("views", descending=True)
    )