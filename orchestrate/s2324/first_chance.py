from acquire.s2324.first_chance import get_results
import polars as pl

from orchestrate.s2324 import save_event_df, Event
from tdfio.const import Gender
from score import compute_event_points_with_age_advantage


if __name__ == '__main__':
    unparsed = get_results()

    for gender in [Gender.male, Gender.female]:
        up = compute_event_points_with_age_advantage(unparsed.filter(pl.col('gender') == gender.to_string()))
        save_event_df(up, gender, Event.firstchance)
