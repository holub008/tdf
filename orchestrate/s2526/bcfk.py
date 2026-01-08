import polars as pl

from acquire.s2526.bcfk import get_results
from orchestrate.s2526 import Event
from tdfio.const import Gender
from score import compute_event_points_with_age_advantage


if __name__ == '__main__':
    unparsed = get_results()

    for gender in [Gender.male, Gender.female]:
        up = compute_event_points_with_age_advantage(unparsed.filter(pl.col('gender') == gender.to_string()))
        Event.bcfk.save_df(up, gender)
