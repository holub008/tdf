import polars as pl
from acquire.s2526.ashwabay import get_results
from orchestrate.s2526 import NON_MAIN_RACE_POINTS, NON_MAIN_EVENT_SPOOF, Event
from score import compute_event_points_with_age_advantage
from tdfio.const import Gender

main_results = get_results(True)
nonmain_results = get_results(False)

for gender in [Gender.male, Gender.female]:
    main_points = compute_event_points_with_age_advantage(main_results.filter(pl.col('gender') == gender.to_string()))
    nmr_gender = nonmain_results.filter(pl.col('gender').eq(gender.name))
    nonmain_points = pl.DataFrame({
        'first_name': nmr_gender['first_name'],
        'last_name': nmr_gender['last_name'],
        'gender': gender.name,
        'gender_place': NON_MAIN_EVENT_SPOOF,
        'age': NON_MAIN_EVENT_SPOOF,
        'age_advantage_event_points': NON_MAIN_RACE_POINTS,
    })

    all_event_points = pl.concat([
        main_points.select(['first_name', 'last_name', 'gender', 'gender_place', 'age', 'age_advantage_event_points']),
        nonmain_points
    ])

    Event.coll.save_df(all_event_points, gender)