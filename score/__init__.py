import polars as pl

from tdfio.racers import RacerDB


def _compute_age_advantage(a: float) -> float:
    if not a or a <= 40:
        return 0

    return (a - 40) / 2


def compute_placement_points(matched_event_results: pl.DataFrame, racer_db: RacerDB) -> pl.DataFrame:
    """
    compute placement points for a single (event, gender)
    input df should contain columns `racer_id` and `place`. we will precondition it and vomit if the data looks bad
    :return:
    """




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
    point_tups = [(r['racer_id'], it[1] if ep['count'] >= it[0]) for r in ep.iter_rows(named=True) for it in incentive_thresholds]

    return pl.DataFrame(point_tups, schema=['racer_id', 'points'])


def compute_team_points():
    pass