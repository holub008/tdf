import polars as pl
from enum import Enum, auto

from tdfio.const import Gender


class Event(Enum):
    skadischase = auto()
    firstchance = auto()

    def to_string(self) -> str:
        return self.name

    @staticmethod
    def from_string(s: str):
        if s == 'skadischase':
            return Event.skadischase
        elif s == 'firstchance':
            return Event.firstchance

        raise ValueError(f'Unrecognized event string representation {s}')


def save_event_df(df: pl.DataFrame, g: Gender, e: Event):
    pretty_df = df.sort(by='total_event_points', descending=True)
    selections = ['gender_place', 'first_name', 'last_name', 'age', 'age_advantage_event_points']
    if 'location' in df.columns:
        selections.append('location')

    pretty_df\
        .select(selections)\
        .write_csv(f'./db/s2324/{e.to_string()}_{g.to_string()}.csv')
