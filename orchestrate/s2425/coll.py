import polars as pl
import acquire.mtec as mtec
from orchestrate.s2425 import Event
from orchestrate.s2425 import NON_MAIN_EVENT_SPOOF, NON_MAIN_RACE_POINTS
from score.event import compute_event_points_with_age_advantage
from tdfio.const import Gender

MAIN_RACE_ID = 18267
NON_MAIN_RACE_ID = 18268

if __name__ == '__main__':
    main_results = mtec.scrape_race(MAIN_RACE_ID)
    nonmain_results = mtec.scrape_race(NON_MAIN_RACE_ID)

    for gender in [Gender.male, Gender.female, Gender.nb]:
        main_points = compute_event_points_with_age_advantage(main_results.filter(pl.col('gender') == gender.to_string()))
        nmr_gender = nonmain_results.filter(pl.col('gender').eq(gender.name))
        # new in 24/25, non-main event participants also receive points
        nonmain_points = pl.DataFrame({
            'first_name': nmr_gender['first_name'],
            'last_name': nmr_gender['last_name'],
            'gender': gender.name,
            'gender_place': NON_MAIN_EVENT_SPOOF,
            'age': NON_MAIN_EVENT_SPOOF,
            'age_advantage_event_points': NON_MAIN_RACE_POINTS,
        })

        Event.coll.save_df(pl.concat([main_points.select(['first_name', 'last_name', 'gender', 'gender_place', 'age', 'age_advantage_event_points']), nonmain_points]), gender)
