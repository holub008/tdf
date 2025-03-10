import polars as pl
from db.s2425 import load_results, load_team_membership
from orchestrate.s2425 import Event
from score import compute_total_individual_points, compute_team_points
from tdfio.const import Gender

EVENTS_TO_SCORE = [Event.skadischase, Event.hiihto, Event.firstchance, Event.ll_challenge, Event.mount_ashwabay,
                   Event.coll, Event.vasaloppet, Event.pepsi_challenge, Event.snu]

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
        .sort(['series_points', 'first_name', 'last_name'], descending=True) # name just adds a stable sort for ties

    for rc in ['skadischase_points', 'hiihto_points', 'firstchance_points', 'll_challenge_points', 'mount_ashwabay_points',
               'coll_points', 'vasaloppet_points', 'pepsi_challenge_points', 'snu_points']:
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
        'skadischase_points': "Skadi's Chase Points",
        'hiihto_points': 'Hiihto Relay Points',
        'firstchance_points': 'First Chance Points',
        'll_challenge_points': 'LL Challenge Points',
        'mount_ashwabay_points': 'Mount Ashwabay Points',
        'coll_points': 'City of Lakes Loppet Points',
        'vasaloppet_points': 'Vasaloppet Points',
        'pepsi_challenge_points': 'Pepsi Challenge Points',
        'snu_points': 'Ski North Ultra Points',
        'series_points': 'Series Points',
        'n_events': 'Number of Events',
    }) \
        .select('Name', 'Overall Place', 'Number of Events',
                "Skadi's Chase Points", 'Hiihto Relay Points',
                'First Chance Points', 'LL Challenge Points',
                'Mount Ashwabay Points', 'City of Lakes Loppet Points',
                'Vasaloppet Points', 'Pepsi Challenge Points', 'Ski North Ultra Points',
                'Series Points') \
        .fill_null(0) \
        .write_csv(f'orchestrate/s2425/tdf_individual_{g.to_string()}_standings.csv')


def compute_and_write_team_points():
    membership = load_team_membership()
    male_points = compute_all_individual_points(Gender.male)
    female_points = compute_all_individual_points(Gender.female)
    tp = compute_team_points(membership, male_points, female_points, EVENTS_TO_SCORE)
    tp\
        .sort('total_points', descending=True)\
        .with_columns(
            pl.Series(name='Overall Place', values=range(1, tp.shape[0] + 1)),
            pl.col('skadischase_points').round(2).alias('skadischase_points'),
            pl.col('hiihto_points').round(2).alias('hiihto_points'),
            pl.col('firstchance_points').round(2).alias('firstchance_points'),
            pl.col('ll_challenge_points').round(2).alias('ll_challenge_points'),
            pl.col('mount_ashwabay_points').round(2).alias('mount_ashwabay_points'),
            pl.col('coll_points').round(2).alias('coll_points'),
            pl.col('vasaloppet_points').round(2).alias('vasaloppet_points'),
            pl.col('pepsi_challenge_points').round(2).alias('pepsi_challenge_points'),
            pl.col('snu_points').round(2).alias('snu_points'),
            pl.col('total_points').round(2).alias('total_points'),
        )\
        .rename({
            'team_name': 'Team Name',
            'skadischase_points': "Skadi's Chase Points",
            'hiihto_points': "Hiihto Points",
            'firstchance_points': 'First Chance Points',
            'll_challenge_points': 'LL Challenge Points',
            'mount_ashwabay_points': 'Mount Ashwabay Points',
            'coll_points': 'City of Lakes Loppet Points',
            'vasaloppet_points': 'Vasaloppet Points',
            'pepsi_challenge_points': 'Pepsi Challenge Points',
            'snu_points': 'Ski North Ultra Points',
            'total_points': 'Total Points'
        })\
        .select('Team Name', 'Overall Place',
                "Skadi's Chase Points", "Hiihto Points", "First Chance Points", "LL Challenge Points",
                "Mount Ashwabay Points", "City of Lakes Loppet Points", 'Vasaloppet Points', 'Pepsi Challenge Points',
                'Ski North Ultra Points',
                'Total Points')\
        .write_csv(f'orchestrate/s2425/tdf_team_standings.csv')


if __name__ == '__main__':
    compute_and_write_all_individual_points(Gender.female)
    compute_and_write_all_individual_points(Gender.male)
    compute_and_write_all_individual_points(Gender.nb)
    compute_and_write_team_points()
