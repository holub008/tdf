from dataclasses import dataclass

import polars as pl
# this import needs to be updated every season. Needs some abstraction
from tdfio.const import Event
from tdfio.const import Gender

NON_MAIN_RACE_POINTS = 20.0
NON_MAIN_EVENT_SPOOF = 999


def score_and_save_event(
        event: Event,
        main_results: pl.DataFrame,
        nonmain_results: pl.DataFrame,
):
    """
        for an individual event, score it across all participating genders & persist points to db/{season}
        event: What you're scoring for
        main_results: Results of the race within the event that scores 20-100. should have columns first_name, last_name, age, gender, gender_place
        nonmain_results: Results of any other race in the event (scored 20). Should have columns first_name, last_name, age, gender, gender_place
        multi_day_dupe_consolidation_threshold: Normally persisted points will contain duplicate names, in the event 2+ racers share a name.
          However, some events occur over multiple days, and racers may participate in multiple races.
          This conflates the "shared name" and "repeat racer" case. "shared name" requires manual correction; "repeat racer" is resolved by taking highest points.
          Supply this parameter when you want to allow "repeat racers" up to a limit (i.e. the number of races a participant could be involved in; usually 2 for COLL or Vasaloppet)
          When falsy, no point consolidation will be performed. This should be used in single-day events.
    """
    for gender in [Gender.male, Gender.female, Gender.nb]:
        main_points = compute_event_points_with_age_advantage(
            main_results.filter(pl.col('gender') == gender.to_string()))
        main_points = main_points.select(
            ['first_name', 'last_name', 'gender', 'gender_place', 'age', 'age_advantage_event_points'])
        nmr_gender = nonmain_results.filter(pl.col('gender').eq(gender.name))
        nonmain_points = pl.DataFrame({
            'first_name': nmr_gender['first_name'],
            'last_name': nmr_gender['last_name'],
            'gender': gender.name,
            'gender_place': NON_MAIN_EVENT_SPOOF,
            'age': NON_MAIN_EVENT_SPOOF,
            'age_advantage_event_points': NON_MAIN_RACE_POINTS,
        })

        if main_points.is_empty() and nonmain_points.is_empty():
            continue
        elif main_points.is_empty():
            all_event_points = nonmain_points
        elif nonmain_points.is_empty():
            all_event_points = main_points
        else:
            all_event_points = pl.concat([
                main_points,
                nonmain_points
            ])

        if event.get_event_days() > 1:
            # for multi-day events, there will be repeat customers. We take the highest score ("consolidation")
            consolidated = all_event_points.groupby(['first_name', 'last_name'])
            unexpected = consolidated.count().filter(pl.col('count') > event.get_event_days())
            if not unexpected.is_empty():
                print(unexpected)
                raise ValueError('While consolidating multi-day events, found likely legitimate duplicate names above. '
                                 'Congrats! This means you need to wedge in some code between data acquisition and event scoring to resolve dupes.')
            all_event_points = consolidated.agg(
                pl.all().sort_by('age_advantage_event_points').last(),
            )

        event.save_df(all_event_points, gender)


def compute_age_advantage(rr):
    return rr.with_columns(
        pl.when(pl.col('age').is_null() | (pl.col('age') <= 45))
        .then(0.0)
        .otherwise(pl.col('age').sub(45.0))
        .alias('age_advantage')
    )


def compute_event_points_with_age_advantage(gender_raw_results: pl.DataFrame) -> pl.DataFrame:
    max_place = gender_raw_results.select(pl.col('gender_place').max().alias('m')).item(0, 'm')
    rrp = gender_raw_results.with_columns(
        pl.lit(1).sub((pl.col('gender_place').sub(1)).truediv(max_place)).mul(100).alias('placement_points')
    )
    rrp_floored = rrp.with_columns(
        pl.when(pl.col('placement_points') >= 20.0)
        .then(pl.col('placement_points'))
        .otherwise(20.0)
        .alias('floored_placement_points')
    )

    waa = compute_age_advantage(rrp_floored)
    # no event incentives for the first event!
    fpp = waa.with_columns(
        pl.col('floored_placement_points').add(pl.col('age_advantage')).alias('total_event_points')
    )

    return fpp.with_columns(
        pl.when(pl.col('total_event_points') > 100.0).then(100.0)
        .otherwise(pl.col('total_event_points'))
        .alias('age_advantage_event_points')
    )
