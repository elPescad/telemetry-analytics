import json
import time
import gzip
import io
from pathlib import Path
import polars as pl
from google.cloud import storage
from app.models import TelemetryRegistry


class LogParser:

    def __init__(self, log_source: str | Path | io.StringIO):
        self.log_source = log_source

    def load_raw(self) -> pl.DataFrame:
        """Loads raw NDJSON from a local file path or an in-memory string stream."""
        if isinstance(self.log_source, io.StringIO):
            return pl.read_ndjson(self.log_source)
        
        path = Path(self.log_source)
        if not path.exists():
            raise FileNotFoundError(f"Cannot find log file at {path}")
        return pl.read_ndjson(path)

    @staticmethod
    def _safe_json_parse(payload_str: str | None) -> dict | None:
        """Safely parses JSON strings, returning None for corrupted rows."""
        if not payload_str:
            return None
        try:
            return json.loads(payload_str)
        except Exception:
            try:
                return json.loads(payload_str.replace("'", '"'))
            except Exception:
                return None

    def parse_events(self) -> pl.DataFrame:
        df = self.load_raw()

        payload_schema = pl.Struct(
            {
                "e": pl.String,   # Event type
                "id": pl.String,  # Target ID
                "t": pl.Int64,    # Metric value
                "u": pl.String,   # User ID
                "yr": pl.String   # College Year
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

        return result.filter(pl.col("event_type").is_not_null())

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

        session_types = ["session_start", "session_end"]

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

    @staticmethod
    def load_cloud_telemetry(bucket_name: str, max_lookback_days: int = 28) -> pl.DataFrame:
        """
        Fetches GCP log segments created within max_lookback_days,
        skipping older files entirely to optimize memory and bandwidth.
        """
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix="segment_")

        cutoff_timestamp = int(time.time()) - (max_lookback_days * 86400)
        
        dfs = []
        for blob in blobs:
            if not blob.name.endswith(".gz"):
                continue

            try:
                file_timestamp = int(blob.name.split("_")[1].split(".")[0])
                if file_timestamp < cutoff_timestamp:
                    continue
            except (IndexError, ValueError):
                pass

            compressed_bytes = blob.download_as_bytes()
            decompressed_text = gzip.decompress(compressed_bytes).decode("utf-8")
            
            # Now safely works with io.StringIO streams
            parser = LogParser(io.StringIO(decompressed_text))
            dfs.append(parser.parse_events())

        if not dfs:
            return pl.DataFrame()

        return pl.concat(dfs, rechunk=True)