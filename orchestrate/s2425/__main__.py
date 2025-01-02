import polars as pl
from db.s2425 import load_results
from orchestrate.s2425 import Event
from score import compute_total_individual_points
from tdfio.const import Gender

EVENTS_TO_SCORE = [Event.skadischase, Event.hiihto]


def compute_all_individual_points(g: Gender):
    results = [load_results(e, g) for e in EVENTS_TO_SCORE]
    valid_events = [EVENTS_TO_SCORE[ix] for ix, r in enumerate(results) if r is not None]
    valid_results = [r for r in results if r is not None]
    return compute_total_individual_points(valid_results, valid_events)


def compute_and_write_all_individual_points(g: Gender):
    aip = compute_all_individual_points(g) \
        .sort('total_points', descending=True)

    for rc in ['skadischase_points', 'hiihto_points']:
        if rc not in aip.columns:
            aip = aip.with_columns(pl.lit(0.0).alias(rc))
        else:
            aip = aip.with_columns(pl.col(rc).round(2).alias(rc))

    aip.with_columns(
        pl.concat_str(['first_name', 'last_name'], separator=' ').alias('Name'),
        pl.Series(name='Overall Place', values=range(1, aip.shape[0] + 1)),
        pl.col('total_points').round(2).alias('total_points'),
    ) \
        .rename({
        'skadischase_points': "Skadi's Chase Points",
        'hiihto_points': 'Hiihto Relay Points',
        'total_points': 'Total Points',
        'n_events': 'Number of Events',
    }) \
        .select('Name', 'Overall Place', 'Number of Events',
                "Skadi's Chase Points", 'Hiihto Relay Points',
                'Total Points') \
        .fill_null(0) \
        .write_csv(f'orchestrate/s2425/tdf_individual_{g.to_string()}_standings.csv')

if __name__ == '__main__':
    compute_and_write_all_individual_points(Gender.female)
    compute_and_write_all_individual_points(Gender.male)
