import acquire.mtec as mtec
import polars as pl

from tdfio.const import Gender
from score import compute_event_points_with_age_advantage


def save_event_df(df, g: Gender):
    pretty_df = df.sort(by='total_event_points', descending=True)
    pretty_df\
        .select('gender_place', 'first_name', 'last_name', 'age', 'location', 'age_advantage_event_points')\
        .write_csv(f'./db/2324/skadi_{g.to_string()}.csv')


if __name__ == '__main__':
    unparsed = mtec.scrape_race(16434)

    for gender in [Gender.male, Gender.female]:
        up = compute_event_points_with_age_advantage(unparsed.filter(pl.col('gender') == gender.to_string()))
        save_event_df(up, gender)
