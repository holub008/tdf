import polars as pl
from db.s2526 import load_results, load_team_membership
from db.namequality import perform_alias_quality_check
from orchestrate.s2526 import Event
from score import compute_total_individual_points, compute_team_points, EventTeamPointsResult
from tdfio.const import Gender

EVENTS_TO_SCORE = [Event.bcfk, Event.seeley, Event.riverview]

SCORER_BIAS = pl.DataFrame([['Karl', 'Holub', -10]], schema=['first_name', 'last_name', 'bias_adjustment'])


# reserved for point keepers who shot from the hip repeatedly this season in interpreting rules and results
def adjust_individual_series_points_for_bias(series_points_df: pl.DataFrame) -> pl.DataFrame:
    joined = series_points_df.join(SCORER_BIAS, how='left', on=['first_name', 'last_name'])
    return joined.with_columns(pl.col('series_points').add(pl.col('bias_adjustment').fill_null(0)).alias('series_points'))


def compute_all_individual_points(g: Gender):
    results = [load_results(e, g) for e in EVENTS_TO_SCORE]
    valid_events = [EVENTS_TO_SCORE[ix] for ix, r in enumerate(results) if r is not None]
    valid_results = [r for r in results if r is not None]
    if not valid_results:
        return pl.DataFrame(
            data=[],
            schema={
                'first_name': pl.Utf8,
                'last_name': pl.Utf8,
                'series_points': pl.Float64,
                'n_events': pl.Int64
            }
        )
    series_points_df = compute_total_individual_points(valid_results, valid_events)
    return adjust_individual_series_points_for_bias(series_points_df)


def compute_and_write_all_individual_points(g: Gender):
    aip = compute_all_individual_points(g) \
        .sort(['series_points', 'first_name', 'last_name'], descending=True)  # name just adds a stable sort for ties

    for event in EVENTS_TO_SCORE:
        rc = f'{event.to_string()}_points'
        if rc not in aip.columns:
            aip = aip.with_columns(pl.lit(0.0).alias(rc))
        else:
            aip = aip.with_columns(pl.col(rc).round(2).alias(rc))

    aip.with_columns(
        pl.concat_str(['first_name', 'last_name'], separator=' ').alias('Name'),
        pl.Series(name='Overall Place', values=range(1, aip.shape[0] + 1)),
        pl.col('series_points').round(2).alias('series_points')
    ) \
        .rename({
            'n_events': 'Number of Events',
            'series_points': 'Series Points',
        } | {f'{e.to_string()}_points': f'{e.get_human_readable_name()} Points' for e in EVENTS_TO_SCORE}) \
        .select(['Overall Place', 'Name', 'Number of Events', 'Series Points'] + [f'{e.get_human_readable_name()} Points' for e in EVENTS_TO_SCORE]) \
        .fill_null(0) \
        .write_csv(f'orchestrate/s2526/tdf_individual_{g.to_string()}_standings.csv')


def compute_and_write_team_points():
    membership = load_team_membership()
    male_points = compute_all_individual_points(Gender.male)
    female_points = compute_all_individual_points(Gender.female)
    results = compute_team_points(membership, male_points, female_points, EVENTS_TO_SCORE)
    
    # Round all event points columns
    team_scores = results.team_scores.sort('total_points', descending=True)
    for event in EVENTS_TO_SCORE:
        col_name = f'{event.to_string()}_points'
        team_scores = team_scores.with_columns(pl.col(col_name).round(2).alias(col_name))
    
    team_scores\
        .with_columns(
            pl.Series(name='Overall Place', values=range(1, team_scores.shape[0] + 1)),
            pl.col('total_points').round(2).alias('total_points'),
        )\
        .rename({
            'team_name': 'Team Name',
            'total_points': 'Total Points'
        } | {f'{e.to_string()}_points': f'{e.get_human_readable_name()} Points' for e in EVENTS_TO_SCORE})\
        .select(['Overall Place', 'Team Name'] + [f'{e.get_human_readable_name()} Points' for e in EVENTS_TO_SCORE] + ['Total Points'])\
        .write_csv('orchestrate/s2526/tdf_team_standings.csv')

    results.report\
        .sort(['team_name', 'event', 'gender', 'team_rank'], descending=[False, False, False, False])\
        .select('team_name', 'event', 'gender', 'first_name', 'last_name', 'team_rank', 'event_points', 'is_scoring')\
        .write_csv('orchestrate/s2526/tdf_team_scoring_report.csv')


if __name__ == '__main__':
    genders = [
        Gender.female,
        Gender.male,
        Gender.nb
    ]

    # Perform and report on name quality
    for g in genders:
        perform_alias_quality_check(g, events=EVENTS_TO_SCORE, load_results=load_results)
        compute_and_write_all_individual_points(g)
    compute_and_write_team_points()
