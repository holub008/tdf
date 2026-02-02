import polars as pl
from acquire.mtec import scrape_race

MAIN_RACE_ID = 20133
NONMAIN_RACE_IDS = [
    20132,  # 30k skate
    20135,  # skiathlon
    20137,  # 15k classic
    20136,  # 15k skate
]


def get_results(participation_races: bool) -> pl.DataFrame:
    if participation_races:
        return scrape_race(MAIN_RACE_ID)
    else:
        parts = [scrape_race(rid) for rid in NONMAIN_RACE_IDS]
        return pl.concat(parts)
