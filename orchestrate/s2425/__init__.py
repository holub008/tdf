from enum import auto, Enum
import polars as pl

from tdfio.const import Gender


class Event(Enum):
    skadischase = auto()
    hiihto = auto()
    firstchance = auto()
    ll_challenge = auto()
    mount_ashwabay = auto()
    coll = auto()
    vasaloppet = auto()
    pepsi_challenge = auto()

    def to_string(self) -> str:
        return self.name

    @staticmethod
    def from_string(s: str):
        if s == 'skadischase':
            return Event.skadischase
        elif s == 'hiihto':
            return Event.hiihto
        elif s == 'firstchance':
            return Event.firstchance
        elif s == 'll_challenge':
            return Event.ll_challenge
        elif s == 'ashwabay':
            return Event.ashwabay
        elif s == 'vasaloppet':
            return Event.vasaloppet
        elif s == 'pepsi_challenge':
            return Event.pepsi_challenge

        raise ValueError(f'Unrecognized event string representation {s}')

    def save_df(self, df: pl.DataFrame, g: Gender):
        pretty_df = df.sort(by='age_advantage_event_points', descending=True)
        selections = ['gender_place', 'first_name', 'last_name', 'age', 'age_advantage_event_points']
        if 'location' in df.columns:
            selections.append('location')

        pretty_df\
            .select(selections)\
            .write_csv(f'./db/s2425/{self.to_string()}_{g.to_string()}.csv')


NON_MAIN_RACE_POINTS = 20.0
NON_MAIN_EVENT_SPOOF = 999
