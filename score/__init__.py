import polars as pl
import heapq
from dataclasses import dataclass

# TODO this is awkward, have to update the definition each season
from orchestrate.s2526 import Event
from tdfio.const import Gender


@dataclass(frozen=True)
class EventTeamPointsResult:
    team_scores: pl.DataFrame
    report: pl.DataFrame


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


def attach_event_incentives(aggregate_results: pl.DataFrame) -> pl.DataFrame:
    """
    returned df has columns `racer_id` and `points`
    will provide an inner join on matched results (with 0 points for non-eligibles)
    """
    event_incentives = []
    for ar in aggregate_results.iter_rows(named=True):
        ei = 0
        if ar['n_events'] >= 3:
            ei += 15
        if ar['n_events'] >= 6:
            ei += 20
        if ar['n_events'] >= 9:
            ei += 25

        event_incentives.append(ei)

    aggregate_results = aggregate_results.with_columns(
        pl.Series(name='event_incentive_points', values=event_incentives)
    )
    return aggregate_results


def compute_total_individual_points(
        event_results: list,
        events: list
) -> pl.DataFrame:
    if not len(event_results) == len(events):
        raise ValueError('events and results must match in length')

    if len(event_results) < 1:
        raise ValueError('must supply 1 or more ')

    aggregate_results = event_results[0]
    aggregate_results = aggregate_results \
        .clone() \
        .rename({
        'age_advantage_event_points': f'{events[0].to_string()}_points',
    }) \
        .select('first_name', 'last_name', f'{events[0].to_string()}_points')

    for _ix, other in enumerate(event_results[1:]):
        ix = _ix + 1
        event = events[ix]
        oc = other.select('first_name', 'last_name', 'age_advantage_event_points') \
            .rename({'age_advantage_event_points': f'{event.to_string()}_points'})
        # we can expect the validation to eventually raise a problem
        joined = aggregate_results.join(oc, on=['first_name', 'last_name'], how='outer')

        # validate that we didn't get any multi-joins
        if not joined.n_unique(subset=['first_name', 'last_name']) == joined.shape[0]:
            print(joined
                  .groupby(['first_name', 'last_name'])
                  .count()
                  .filter(pl.col('count') > 1))
            raise ValueError('Data contained a many-1 join on names. Go fix this in db/ data by making '
                             'names unique (e.g. using the age/location in parens)')

        aggregate_results = joined

    total_n_events = []
    series_event_points = []
    for ar in aggregate_results.iter_rows(named=True):
        n_events = 0
        all_event_points = 0
        ind_event_points = []
        for e in events:
            event_points = ar[f'{e.to_string()}_points']
            if event_points is not None:
                n_events += 1
                all_event_points += event_points
                ind_event_points.append(event_points)

        total_n_events.append(n_events)
        series_event_points.append(sum(heapq.nlargest(6, ind_event_points)))

    aggregate_results = aggregate_results \
        .with_columns(
        pl.Series(name='n_events', values=total_n_events),
        pl.Series(name='series_points', values=series_event_points)
    )

    ar_with_ei = attach_event_incentives(aggregate_results)
    ar_with_ei = ar_with_ei.with_columns(
        pl.col('series_points').add(pl.col('event_incentive_points')).alias('series_points')
    )
    return ar_with_ei


def _compute_event_team_points_within_gender(
        membership: pl.DataFrame,
        points: pl.DataFrame,
        e: Event,
) -> pl.DataFrame:
    event_points_column = f'{e.to_string()}_points'
    joinable_points = points.select([event_points_column, 'first_name', 'last_name']).drop_nulls([event_points_column])
    points_joined_membership = membership.join(joinable_points, on=['first_name', 'last_name'], how='inner')

    if not points_joined_membership.n_unique(['first_name', 'last_name']) == points_joined_membership.shape[0]:
        raise ValueError('Unexpected multijoin, bad bad bad')

    points_joined_membership = points_joined_membership.sort(
        ['team_name', event_points_column],
        descending=[False, True]
    )

    ranked = points_joined_membership.sort(
        ["team_name", event_points_column, "last_name", "first_name"],
        descending=[False, True, False, False],
        nulls_last=True,
    ).with_columns(
        pl.col(event_points_column)
        .rank(method="ordinal", descending=True)
        .over("team_name")
        .alias("team_rank")
    ).with_columns(
        pl.when(pl.col("team_rank") <= 3)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("is_scoring")
    ).with_columns(
        pl.lit(e.get_human_readable_name()).alias("event"),
    )

    report_df = ranked.select([
        "event",
        "team_name",
        "gender",
        "first_name",
        "last_name",
        pl.col(event_points_column).round(2).alias("event_points"),
        "team_rank",
        "is_scoring",
    ])

    top_3_scorers_by_team = pl.concat([
        x.top_k(3, by=event_points_column)
        for x in points_joined_membership.partition_by('team_name')
    ])

    team_scores = top_3_scorers_by_team \
        .groupby('team_name') \
        .agg(pl.col(event_points_column).sum().alias(event_points_column)) \
        .select('team_name', event_points_column)

    return EventTeamPointsResult(team_scores=team_scores, report=report_df)


def compute_team_points(
        membership: pl.DataFrame,
        male_points: pl.DataFrame,
        female_points: pl.DataFrame,
        events: list[Event],
) -> EventTeamPointsResult:
    gender_points_by_team = []
    report_dfs: list[pl.DataFrame] = []

    for g in [Gender.female, Gender.male]:
        gender_membership = membership.filter(pl.col('gender') == g.to_string())
        gender_points = male_points if g == Gender.male else female_points

        first = _compute_event_team_points_within_gender(gender_membership, gender_points, events[0])
        report_dfs.append(first.report)
        all_gender_points = first.team_scores

        for e in events[1:]:
            res = _compute_event_team_points_within_gender(gender_membership, gender_points, e)
            report_dfs.append(res.report)
            all_gender_points = all_gender_points.join(res.team_scores, on="team_name", how="outer")

        gender_points_by_team.append(all_gender_points)

    team_points_report = pl.concat(report_dfs) if report_dfs else pl.DataFrame()

    magic_suffix = '_female'
    gpbt_joined = gender_points_by_team[0] \
        .join(gender_points_by_team[1], on='team_name', how='outer', suffix=magic_suffix) \
        .fill_null(0)
    all_event_columns = []
    for e in events:
        event_column1 = f'{e.to_string()}_points'
        all_event_columns.append(event_column1)
        event_column2 = f'{event_column1}{magic_suffix}'
        gpbt_joined = gpbt_joined.with_columns(
            pl.col(event_column1).add(pl.col(event_column2)).alias(event_column1)
        )

    final_scores = gpbt_joined \
        .select(['team_name'] + all_event_columns) \
        .with_columns(pl.sum_horizontal(all_event_columns).alias('total_points'))

    return EventTeamPointsResult(final_scores, team_points_report)
