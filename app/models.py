import dataclasses
from dataclasses import dataclass, field
import polars as pl

def _empty_df() -> pl.DataFrame:
    """Helper to generate an empty Polars DataFrame."""
    return pl.DataFrame()

@dataclass(frozen=True, slots=True)
class TelemetryRegistry: 
    posts: pl.DataFrame = field(default_factory=_empty_df)     
    events: pl.DataFrame = field(default_factory=_empty_df)    
    feed_ui: pl.DataFrame = field(default_factory=_empty_df)   
    sessions: pl.DataFrame = field(default_factory=_empty_df)  
    other: pl.DataFrame = field(default_factory=_empty_df)     

    def __post_init__(self):
        """Guardrail to enforce types if the parser accidentally passes None."""
        for f in dataclasses.fields(self):
            if getattr(self, f.name) is None:
                object.__setattr__(self, f.name, pl.DataFrame())