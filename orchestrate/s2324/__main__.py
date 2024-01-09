import polars as pl
from db.s2324 import load_results, load_team_membership
from orchestrate.s2324 import Event
from score import compute_total_individual_points, compute_team_points
from tdfio.const import Gender

EVENTS_TO_SCORE = [Event.skadischase, Event.firstchance]


def compute_all_individual_points(g: Gender):
    results = [load_results(e, g) for e in EVENTS_TO_SCORE]
    return compute_total_individual_points(results, EVENTS_TO_SCORE)


def compute_and_write_all_individual_points(g: Gender):
    aip = compute_all_individual_points(g) \
        .sort('total_points', descending=True)

    aip.with_columns(
        pl.concat_str(['first_name', 'last_name'], separator=' ').alias('Name'),
        pl.Series(name='Overall Place', values=range(1, aip.shape[0] + 1)),
        pl.col('skadischase_points').round(2).alias('skadischase_points'),
        pl.col('firstchance_points').round(2).alias('firstchance_points'),
        pl.col('total_points').round(2).alias('total_points'),
    ) \
        .rename({
        'skadischase_points': "Skadi's Chase Points",
        'firstchance_points': 'First Chance Points',
        'total_points': 'Total Points',
        'n_events': 'Number of Events'
    }) \
        .select('Name', 'Overall Place', 'Number of Events', "Skadi's Chase Points", 'First Chance Points',
                'Total Points') \
        .fill_null(0) \
        .write_csv(f'orchestrate/s2324/tdf_individual_{g.to_string()}_standings.csv')


def compute_and_write_team_points():
    membership = load_team_membership()
    male_points = compute_all_individual_points(Gender.male)
    female_points = compute_all_individual_points(Gender.female)
    compute_team_points(membership, male_points, female_points, EVENTS_TO_SCORE) \
        .write_csv(f'orchestrate/s2324/tdf_team_standings.csv')


if __name__ == '__main__':
    compute_and_write_all_individual_points(Gender.female)
    compute_and_write_all_individual_points(Gender.male)

    compute_and_write_team_points()
