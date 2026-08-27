import polars as pl
from datetime import datetime, timedelta, timezone

# Point Weights Configuration
WEIGHT_ATTEND = 15   # Actual QR scan check-in
WEIGHT_POST = 5
WEIGHT_LIKE = 1

# --- TIME WINDOW HELPERS ---

def filter_by_days(cleaned_df: pl.DataFrame, days: int) -> pl.DataFrame:
    """Filters logs to only include entries from the last N days based on server_timestamp."""
    if cleaned_df.is_empty():
        return cleaned_df
        
    cutoff_epoch = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    return cleaned_df.filter(pl.col("server_timestamp") >= cutoff_epoch)

def is_weekly_report_day() -> bool:
    """Returns True if today is Friday (weekday 4)."""
    return datetime.now(timezone.utc).weekday() == 4

def is_monthly_report_day() -> bool:
    """Returns True if today is the 1st of the month."""
    return datetime.now(timezone.utc).day == 1


# --- RAW LOG AGGREGATION ENGINES (WEEKLY) ---

def compute_student_weighted_scores(cleaned_df: pl.DataFrame) -> pl.DataFrame:
    if cleaned_df.is_empty() or "user_id" not in cleaned_df.columns:
        return pl.DataFrame()

    return (
        cleaned_df
        .filter(pl.col("user_id").is_not_null())
        .group_by(["user_id", "college_year"])
        .agg(
            # Broad metric: Total count of distinct event check-ins
            pl.col("target_id")
            .filter((pl.col("event_type") == "qr_scan_success") & pl.col("target_id").is_not_null())
            .drop_nulls()  # CRITICAL: Prevents [null] from being counted as 1
            .n_unique()
            .alias("attend_count"),

            # Specific drill-down: Array of unique event IDs attended by this user
            pl.col("target_id")
            .filter((pl.col("event_type") == "qr_scan_success") & pl.col("target_id").is_not_null())
            .drop_nulls()
            .unique()
            .alias("attended_event_ids"),

            pl.col("event_type").filter(pl.col("event_type") == "create_post_start").count().alias("post_count"),
            pl.col("event_type").filter(pl.col("event_type") == "like").count().alias("like_count"),
        )
        .with_columns(
            (pl.col("attend_count") * WEIGHT_ATTEND).alias("attend_pts"),
            (pl.col("post_count") * WEIGHT_POST).alias("post_pts"),
            (pl.col("like_count") * WEIGHT_LIKE).alias("like_pts"),
        )
        .with_columns(
            (pl.col("attend_pts") + pl.col("post_pts") + pl.col("like_pts")).alias("total_score")
        )
        .sort("total_score", descending=True)
    )

def compute_event_performance(cleaned_df: pl.DataFrame) -> pl.DataFrame:
    if cleaned_df.is_empty() or "target_id" not in cleaned_df.columns:
        return pl.DataFrame()

    return (
        cleaned_df
        .filter(pl.col("target_id").is_not_null())
        .filter(pl.col("event_type").is_in(["view_event", "qr_scan_success", "rsvp_success"]))
        .group_by("target_id")
        .agg(
            pl.col("event_type").filter(pl.col("event_type") == "view_event").count().alias("views"),
            pl.col("event_type").filter(pl.col("event_type") == "rsvp_success").count().alias("total_rsvps"),

            # Broad metric: Total actual attendance count
            pl.col("user_id")
            .filter((pl.col("event_type") == "qr_scan_success") & pl.col("user_id").is_not_null())
            .drop_nulls()
            .n_unique()
            .alias("actual_attended"),

            # Specific drill-down: Array of unique user IDs who attended this event
            pl.col("user_id")
            .filter((pl.col("event_type") == "qr_scan_success") & pl.col("user_id").is_not_null())
            .drop_nulls()
            .unique()
            .alias("attendee_user_ids"),
        )
        .with_columns(
            pl.when(pl.col("actual_attended") > 0)
            .then((pl.col("total_rsvps") / pl.col("actual_attended")).round(2))
            .otherwise(0.0)
            .alias("rsvp_to_attendance_ratio"),

            pl.when(pl.col("total_rsvps") > 0)
            .then((pl.col("actual_attended") / pl.col("total_rsvps") * 100).round(1))
            .otherwise(0.0)  # Walk-ins with 0 RSVPs will default to 0.0% to avoid infinity bugs
            .alias("turnout_vs_rsvp_pct")
        )
        .sort("actual_attended", descending=True)
    )

def compute_post_performance(cleaned_df: pl.DataFrame) -> pl.DataFrame:
    if cleaned_df.is_empty() or "target_id" not in cleaned_df.columns:
        return pl.DataFrame()

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


# --- SUMMARY ROLLUP ENGINES (MONTHLY) ---

def _get_valid_dfs(dfs: list[pl.DataFrame]) -> list[pl.DataFrame]:
    """Helper to strip out empty dataframes to prevent pl.concat crashes."""
    return [df for df in dfs if not df.is_empty()]

def compute_monthly_post_performance(weekly_post_dfs: list[pl.DataFrame]) -> pl.DataFrame:
    valid_dfs = _get_valid_dfs(weekly_post_dfs)
    if not valid_dfs:
        return pl.DataFrame()

    return (
        pl.concat(valid_dfs)
        .group_by("target_id")
        .agg(
            pl.col("views").sum(),
            pl.col("likes").sum(),
            pl.col("comment_views").sum(),
        )
        .with_columns(
            pl.when(pl.col("views") > 0)
            .then(((pl.col("likes") + pl.col("comment_views")) / pl.col("views") * 100).round(1))
            .otherwise(0.0)
            .alias("engagement_rate_pct")
        )
        .sort(["engagement_rate_pct", "views"], descending=[True, True])
    )

def compute_monthly_student_scores(weekly_student_dfs: list[pl.DataFrame]) -> pl.DataFrame:
    valid_dfs = _get_valid_dfs(weekly_student_dfs)
    if not valid_dfs:
        return pl.DataFrame()

    return (
        pl.concat(valid_dfs)
        .group_by(["user_id", "college_year"])
        .agg(
            # .explode() flattens the list of lists. drop_nulls() removes empty entries before unique()
            pl.col("attended_event_ids").explode().drop_nulls().unique().alias("attended_event_ids"),
            pl.col("post_count").sum(),
            pl.col("like_count").sum(),
        )
        .with_columns(
            pl.col("attended_event_ids").list.len().alias("attend_count")
        )
        .with_columns(
            (pl.col("attend_count") * WEIGHT_ATTEND).alias("attend_pts"),
            (pl.col("post_count") * WEIGHT_POST).alias("post_pts"),
            (pl.col("like_count") * WEIGHT_LIKE).alias("like_pts"),
        )
        .with_columns(
            (pl.col("attend_pts") + pl.col("post_pts") + pl.col("like_pts")).alias("total_score")
        )
        .sort("total_score", descending=True)
    )

def compute_monthly_event_performance(weekly_event_dfs: list[pl.DataFrame]) -> pl.DataFrame:
    valid_dfs = _get_valid_dfs(weekly_event_dfs)
    if not valid_dfs:
        return pl.DataFrame()

    return (
        pl.concat(valid_dfs)
        .group_by("target_id")
        .agg(
            pl.col("views").sum(),
            pl.col("total_rsvps").sum(),
            # .explode() flattens the list of lists. drop_nulls() removes empty entries before unique()
            pl.col("attendee_user_ids").explode().drop_nulls().unique().alias("attendee_user_ids"),
        )
        .with_columns(
            pl.col("attendee_user_ids").list.len().alias("actual_attended")
        )
        .with_columns(
            pl.when(pl.col("actual_attended") > 0)
            .then((pl.col("total_rsvps") / pl.col("actual_attended")).round(2))
            .otherwise(0.0)
            .alias("rsvp_to_attendance_ratio"),

            pl.when(pl.col("total_rsvps") > 0)
            .then((pl.col("actual_attended") / pl.col("total_rsvps") * 100).round(1))
            .otherwise(0.0)
            .alias("turnout_vs_rsvp_pct")
        )
        .sort("actual_attended", descending=True)
    )