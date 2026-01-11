from enum import auto, Enum
import polars as pl

from tdfio.const import Gender


class Event(Enum):
    bcfk = auto()
    seeley = auto()

    def to_string(self) -> str:
        return self.name

    @staticmethod
    def from_string(s: str):
        match = [v.name for v in Event if v.name == s]
        if not match:
            raise ValueError(f'Unrecognized event string representation {s}')
        return match

    def save_df(self, df: pl.DataFrame, g: Gender):
        pretty_df = df.sort(by='age_advantage_event_points', descending=True)
        selections = ['gender_place', 'first_name', 'last_name', 'age', 'age_advantage_event_points']
        if 'location' in df.columns:
            selections.append('location')

        pretty_df\
            .select(selections)\
            .write_csv(f'./db/s2526/{self.to_string()}_{g.to_string()}.csv')


NON_MAIN_RACE_POINTS = 20.0
NON_MAIN_EVENT_SPOOF = 999
