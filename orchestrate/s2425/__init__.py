from enum import auto, Enum
import polars as pl

from tdfio.const import Gender


class Event(Enum):
    skadischase = auto()
    hiihto = auto()

    def to_string(self) -> str:
        return self.name

    @staticmethod
    def from_string(s: str):
        if s == 'skadischase':
            return Event.skadischase

        raise ValueError(f'Unrecognized event string representation {s}')

    def save_df(self, df: pl.DataFrame, g: Gender):
        pretty_df = df.sort(by='total_event_points', descending=True)
        selections = ['gender_place', 'first_name', 'last_name', 'age', 'age_advantage_event_points']
        if 'location' in df.columns:
            selections.append('location')

        pretty_df\
            .select(selections)\
            .write_csv(f'./db/s2425/{self.to_string()}_{g.to_string()}.csv')