import json
from pathlib import Path
import polars as pl
from app.models import TelemetryRegistry

class LogParser:

    def __init__(self, log_path: str):
        self.log_path = Path(log_path)

    # Loads raw json log
    def load_raw(self) -> pl.DataFrame:
        if not self.log_path.exists():
            raise FileNotFoundError(f"Cannot find log file at {self.log_path}")
        return pl.read_ndjson(self.log_path)

    # Makes sure the json isn't corrupted
    @staticmethod
    def _safe_json_parse(payload_str: str | None) -> dict | None:
        """Safely parses JSON strings, returning None for corrupted rows."""
        if not payload_str:
            return None
        try:
            return json.loads(payload_str)
        except Exception:
            try:
                # Fallback: fix single quotes if present
                return json.loads(payload_str.replace("'", '"'))
            except Exception:
                return None

    # Parses the metadata within the json and promotes to new columns
    def parse_events(self) -> pl.DataFrame:
        df = self.load_raw()

        payload_schema = pl.Struct(
            {
                "e": pl.String, #event
                "id": pl.String, #target id
                "t": pl.Int64, #metric value
                "u": pl.String,  #user id
                "yr": pl.String #college Year (Fresh, Soph, etc)
            }
        )

        result = (
            df.with_columns(
                pl.col("payload")
                .map_elements(
                    self._safe_json_parse, 
                    return_dtype=payload_schema
                )
            )
            .unnest("payload")
            .rename(
                {
                    "timestamp": "server_timestamp", 
                    "e": "event_type", 
                    "id": "target_id", 
                    "t": "metric_value",
                    "u": "user_id",
                    "yr": "college_year"
                }
            )
        )

        # Corrupted logs without ant event_type are filtered out
        cleaned_result = result.filter(
            pl.col("event_type").is_not_null()
        )
        return cleaned_result

    # Organize events into master class TelemetryRegistry
    def organize_events(self, cleaned_df: pl.DataFrame) -> TelemetryRegistry:
        post_events_type = [
            "view_post", "view_comments", "like", "unlike", 
            "delete_post", "create_post_start", "create_post_cancel"
        ]

        event_rsvp_types = [
            "view_event", "rsvp_click", "rsvp_success", 
            "rsvp_cancel", "share_event"
        ]

        feed_ui_types = [
            "pull_to_refresh", "scroll_depth", 
            "check_rank", "profile_press"
        ]

        session_types = [
            "session_start", "session_end"
        ]

        posts_df = cleaned_df.filter(pl.col("event_type").is_in(post_events_type))
        events_df = cleaned_df.filter(pl.col("event_type").is_in(event_rsvp_types))
        feed_df = cleaned_df.filter(pl.col("event_type").is_in(feed_ui_types))
        session_df = cleaned_df.filter(pl.col("event_type").is_in(session_types))

        all_known = post_events_type + event_rsvp_types + feed_ui_types + session_types
        other_df = cleaned_df.filter(~pl.col("event_type").is_in(all_known))

        return TelemetryRegistry(
            posts=posts_df,
            events=events_df,
            feed_ui=feed_df,
            sessions=session_df,
            other=other_df
        )

