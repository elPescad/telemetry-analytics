from dataclasses import dataclass
import polars as pl

@dataclass
class TelemetryRegistry: 
    posts: pl.DataFrame        # view_post, like, unlike, delete_post, comments, post creation
    events: pl.DataFrame       # view_event, rsvp_click, rsvp_success, rsvp_cancel, share_event
    feed_ui: pl.DataFrame      # scroll_depth, pull_to_refresh, check_rank, profile_press
    sessions: pl.DataFrame     # session_start, session_end
    other: pl.DataFrame        # Catch-all for any future unknown events
