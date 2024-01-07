import polars as pl

def compute_age_advantage(rr):
    return rr.with_columns(
        pl.when(pl.col('age') <= 40)
        .then(0.0)
        .otherwise(pl.col('age').sub(40.0).truediv(2.0))
        .alias('age_advantage')
    )


def compute_placement_points(matched_event_results: pl.DataFrame) -> pl.DataFrame:
    """
    compute placement points for a single (event, gender)
    input df should contain columns `racer_id` and `place`. we will precondition it and vomit if the data looks bad
    :return:
    """
    pass



def compute_event_incentives(matched_results: pl.DataFrame) -> pl.DataFrame:
    """
    returned df has columns `racer_id` and `points`
    will provide an inner join on matched results (with 0 points for non-eligibles)
    """
    incentive_thresholds = [(3, 15), (6, 20), (9, 25)]
    ep = matched_results\
        .groupby('racer_id')\
        .count()
    # polars doesn't support non-equi joins: https://github.com/pola-rs/polars/issues/10068
    point_tups = [(r['racer_id'], it[1] if ep['count'] >= it[0] else 0) for r in ep.iter_rows(named=True) for it in incentive_thresholds]

    return pl.DataFrame(point_tups, schema=['racer_id', 'points'])


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


def compute_team_points():
    pass