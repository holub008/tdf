from acquire.s2425.pepsi import get_results
from orchestrate.s2425 import Event, NON_MAIN_RACE_POINTS, NON_MAIN_EVENT_SPOOF
from score import compute_event_points_with_age_advantage
from tdfio.const import Gender
import polars as pl


def _deduplicate_participation_races(main_results: pl.DataFrame, nonmain_results: pl.DataFrame) -> pl.DataFrame:
    # doing participation sucks because there will be duplicates across days. we'll need to clear out
    # with preference for the main race. we also need to dedupe people doing nonmain both days
    nonmain_results = nonmain_results.unique(['first_name', 'last_name'])
    return nonmain_results.join(main_results, on=['first_name', 'last_name'], how='anti')


if __name__ == '__main__':
    main_results = get_results(False)
    nonmain_results = get_results(True)
    nonmain_results = _deduplicate_participation_races(main_results, nonmain_results)

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

        Event.pepsi_challenge.save_df(pl.concat([main_points.select(['first_name', 'last_name', 'gender', 'gender_place', 'age', 'age_advantage_event_points']), nonmain_points]), gender)
