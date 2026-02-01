import polars as pl

from acquire.s2425.hiihto import scrape_hiihto
from orchestrate.s2425 import Event
from tdfio.const import Gender
from score.event import compute_event_points_with_age_advantage


if __name__ == '__main__':
    unparsed = scrape_hiihto()

    for gender in [Gender.male, Gender.female]:
        up = compute_event_points_with_age_advantage(unparsed.filter(pl.col('gender') == gender.to_string()))
        Event.hiihto.save_df(up, gender)