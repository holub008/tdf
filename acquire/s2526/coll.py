import polars as pl
from acquire.mtec import scrape_race

MAIN_RACE_ID = 20099
NONMAIN_RACE_IDS = [
    20100,  # skate puoli
    20101,  # skate tour
    20097,  # classic marathon
    20098,  # classic puoli
    20105,  # classic tour
]


def get_results(participation_races: bool) -> pl.DataFrame:
    if participation_races:
        return scrape_race(MAIN_RACE_ID)
    else:
        parts = [scrape_race(rid) for rid in NONMAIN_RACE_IDS]
        return pl.concat(parts)
