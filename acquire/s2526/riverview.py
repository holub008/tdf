import polars as pl
from acquire.mtec import scrape_race

MAIN_RACE_ID = 20055
NONMAIN_RACE_IDS = [20056, 20057, 20058]


def get_results(participation_races: bool) -> pl.DataFrame:
    if participation_races:
        return scrape_race(MAIN_RACE_ID)
    else:
        parts = [scrape_race(rid) for rid in NONMAIN_RACE_IDS]
        return pl.concat(parts)