import polars as pl
from db.s2324 import load_results
from orchestrate.s2324 import Event
from score import compute_total_individual_points
from tdfio.const import Gender


def compute_all_individual_points(g: Gender,
                                  events=[Event.skadischase, Event.firstchance]):
    results = [load_results(e, g) for e in events]
    tips = compute_total_individual_points(results, events)\
        .sort('total_points', descending=True)

    tips\
        .with_columns(
            pl.concat_str(['first_name', 'last_name'], separator=' ').alias('Name'),
            pl.Series(name='Overall Place', values=range(1, tips.shape[0] + 1)),
            pl.col('skadischase_points').round(2).alias('skadischase_points'),
            pl.col('firstchance_points').round(2).alias('firstchance_points'),
            pl.col('total_points').round(2).alias('total_points'),
        )\
        .rename({
            'skadischase_points': "Skadi's Chase Points",
            'firstchance_points': 'First Chance Points',
            'total_points': 'Total Points',
            'n_events': 'Number of Events'
        })\
        .select('Name', 'Overall Place', 'Number of Events', "Skadi's Chase Points", 'First Chance Points', 'Total Points')\
        .fill_null(0)\
        .write_csv(f'orchestrate/s2324/tdf_individual_{g.to_string()}_standings.csv')


def compute_team_points():
    pass


if __name__ == '__main__':
    compute_all_individual_points(Gender.female)
    compute_all_individual_points(Gender.male)
