from typing import Optional
import polars as pl
from tdfio.const import Gender, Event


class Event2526(Event):
    bcfk = ("Battle Creek Fifty K", 1)
    seeley = ("Seeley Hills Classic", 1)
    riverview = ("Riverview Loppet", 1)
    noquemanon = ("Noquemanon", 1)
    ashwabay = ("Mt. Ashwabay Summit Ski Race", 1)
    coll = ("City of Lakes Loppet", 2)

    def to_string(self) -> str:
        return self.name

    def get_human_readable_name(self) -> str:
        return self.value[0]

    def get_event_days(self) -> Optional[int]:
        """
          This parameter is intended to be used in scoring events where racers may participate in multiple races

          Normally event points will contain duplicate names, in the event 2+ racers share a name.
          However, some events occur over multiple days, and racers may participate in multiple races.
          This conflates the "shared name" and "repeat racer" case. "shared name" requires manual correction; "repeat racer" is resolved by taking highest points (point consolidation).
          This parameter controls how many "repeat racers" we should expect under normal conditions (1 for most races; 2 for COLL or Vasaloppet)
          When 1, no point consolidation will be performed; this makes fixing data in db/ possible, vs more annoying fixes in orchestrate/
        """
        return self.value[1]

    def save_df(self, df: pl.DataFrame, g: Gender):
        pretty_df = df.sort(by='age_advantage_event_points', descending=True)
        selections = ['gender_place', 'first_name', 'last_name', 'age', 'age_advantage_event_points']
        if 'location' in df.columns:
            selections.append('location')

        pretty_df \
            .select(selections) \
            .write_csv(f'./db/s2526/{self.to_string()}_{g.to_string()}.csv')
