import acquire.mtec as mtec
import polars as pl

def compute_age_advantage(rr):
    return rr.with_columns(
        pl.when(pl.col('age') <= 40)
        .then(0.0)
        .otherwise(pl.col('age').sub(40.0).truediv(2.0))
        .alias('age_advantage')
    )


def compute_all_points(gender_raw_results: pl.DataFrame) -> pl.DataFrame:
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
        .alias('total_event_points')
    )


def export_human_df(df, type):
    pretty_df = df.sort(by='total_event_points', descending=True).with_columns(
        pl.col('total_event_points').round(1).alias('total_event_points'),
        pl.arange(1, pl.count() + 1).alias('overall_place'),
    )
    pretty_df.rename({
        'overall_place': 'Place',
        'first_name': 'First Name',
        'last_name': 'Last Name',
        'age': 'Age',
        'gender_place': 'Skadi\'s Chase Place',
        'total_event_points': 'Points',
    }).select('Place', 'First Name', 'Last Name', 'Age', 'Skadi\'s Chase Place', 'Points').write_csv(f'~/skadi_points_{type}.csv')


unparsed = mtec.scrape_race(16434)

wup = compute_all_points(unparsed.filter(pl.col('gender') == 'female'))
mup = compute_all_points(unparsed.filter(pl.col('gender') == 'male'))

export_human_df(wup, 'women')
export_human_df(mup, 'men')
