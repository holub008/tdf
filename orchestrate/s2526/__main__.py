import polars as pl
from db.s2526 import load_results, load_team_membership
from orchestrate.s2526 import Event
from score import compute_total_individual_points, compute_team_points
from tdfio.const import Gender
from db.namequality import perform_alias_quality_check

EVENTS_TO_SCORE = [Event.bcfk, Event.seeley]

SCORER_BIAS = pl.DataFrame([['Karl', 'Holub', -10]], schema=['first_name', 'last_name', 'bias_adjustment'])


# reserved for point keepers who shot from the hip repeatedly this season in interpreting rules and results
def adjust_individual_series_points_for_bias(series_points_df: pl.DataFrame) -> pl.DataFrame:
    joined = series_points_df.join(SCORER_BIAS, how='left', on=['first_name', 'last_name'])
    return joined.with_columns(pl.col('series_points').add(pl.col('bias_adjustment').fill_null(0)).alias('series_points'))


def compute_all_individual_points(g: Gender):
    results = [load_results(e, g) for e in EVENTS_TO_SCORE]
    valid_events = [EVENTS_TO_SCORE[ix] for ix, r in enumerate(results) if r is not None]
    valid_results = [r for r in results if r is not None]
    series_points_df = compute_total_individual_points(valid_results, valid_events)
    return adjust_individual_series_points_for_bias(series_points_df)


def compute_and_write_all_individual_points(g: Gender):
    aip = compute_all_individual_points(g) \
        .sort(['series_points', 'first_name', 'last_name'], descending=True)  # name just adds a stable sort for ties

    for rc in ['bcfk_points', 'seeley_points']:
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
        'bcfk_points': 'BCFK Points',
        'seeley_points': 'Seeley Hills Points',
        'series_points': 'Series Points',
        'n_events': 'Number of Events',
    }) \
        .select('Name', 'Overall Place', 'Number of Events',
                'BCFK Points',
                'Seeley Hills Points',
                'Series Points') \
        .fill_null(0) \
        .write_csv(f'orchestrate/s2526/tdf_individual_{g.to_string()}_standings.csv')


def compute_and_write_team_points():
    membership = load_team_membership()
    male_points = compute_all_individual_points(Gender.male)
    female_points = compute_all_individual_points(Gender.female)
    tp = compute_team_points(membership, male_points, female_points, EVENTS_TO_SCORE)
    tp\
        .sort('total_points', descending=True)\
        .with_columns(
            pl.Series(name='Overall Place', values=range(1, tp.shape[0] + 1)),
            pl.col('bcfk_points').round(2).alias('bcfk_points'),
            pl.col('seeley_points').round(2).alias('seeley_points'),
            pl.col('total_points').round(2).alias('total_points'),
        )\
        .rename({
            'team_name': 'Team Name',
            'bcfk_points': 'BCFK Points',
            'seeley_points': 'Seeley Points',
            'total_points': 'Total Points'
        })\
        .select('Team Name', 'Overall Place',
                'BCFK Points',
                'Seeley Points',
                'Total Points')\
        .write_csv('orchestrate/s2526/tdf_team_standings.csv')

if __name__ == '__main__':
    genders = [
        Gender.female,
        Gender.male,
        # TODO Reinstitute when there are results
        # Gender.nb
    ]

    # Perform and report on name quality
    for g in genders:
        perform_alias_quality_check(g, events=EVENTS_TO_SCORE, load_results=load_results)

    for g in genders:
        compute_and_write_all_individual_points(g)
    compute_and_write_team_points()
