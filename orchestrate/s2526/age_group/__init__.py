import polars as pl
from db.s2526 import load_results, load_standings
from orchestrate.s2526 import Event2526
from score.event import NON_MAIN_EVENT_SPOOF
from tdfio.const import Gender

AGE_GROUPS = pl.DataFrame([
    {'lower': 0, 'upper': 30},
    {'lower': 31, 'upper': 35},
    {'lower': 36, 'upper': 40},
    {'lower': 41, 'upper': 45},
    {'lower': 46, 'upper': 50},
    {'lower': 51, 'upper': 55},
    {'lower': 56, 'upper': 60},
    {'lower': 61, 'upper': 65},
    {'lower': 66, 'upper': 70},
    {'lower': 71, 'upper': 123}, # https://en.wikipedia.org/wiki/Jeanne_Calment , though I believe this must have been fraudulent
]).with_columns(pl.concat_str(["lower", "upper"], separator="-").alias('age_group'))


def _build_ages(g: Gender) -> pl.DataFrame:
    events_with_age = []
    for e in Event2526:
        results_df = load_results(e, g)

        if 'age' in results_df:
            if results_df.schema['age'] == pl.Utf8:
                results_df = results_df.with_columns(
                    pl.col("age").str.parse_int(strict=False).alias("age")
                )

            processed = results_df.with_columns(
                pl.when(pl.col("age") == NON_MAIN_EVENT_SPOOF)
                .then(None)
                .otherwise(pl.col("age"))
                .alias("age"))
            processed = processed.filter(pl.col("age").is_not_null())
            processed = processed.with_columns(pl.col("age").cast(pl.Int64))

            events_with_age.append(processed.select('first_name', 'last_name', 'age'))

    combined_age_results = pl.concat(events_with_age)
    combined_age_results = combined_age_results.with_columns(pl.concat_str(['first_name', 'last_name'], separator=' ').alias('name'))
    return combined_age_results.groupby(['name']).agg(
        pl.col("age").mode().first().alias("age")
    ).select('name', 'age')


def _build_age_attached_standings(g: Gender) -> pl.DataFrame:
    ages = _build_ages(g)
    standings = load_standings(g).rename({'Name': 'name', 'Overall Place': 'place'})
    ag_standings = standings.filter(pl.col('place') > 3)
    age_joined = ag_standings.join(ages, how='inner', on='name')
    group_joined = age_joined.join(AGE_GROUPS, how="cross").filter(
        (pl.col("age") >= pl.col("lower")) & (pl.col("age") <= pl.col("upper"))
    )
    return group_joined.select('name', 'place', 'age_group')


def compute_age_group_winners(g: Gender) -> pl.DataFrame:
    age_attached = _build_age_attached_standings(g)
    return (
        age_attached.sort("place", descending=False)
        .group_by("age_group")
        .agg(pl.all().first())
        .sort('age_group')
        .rename({'place': 'Overall Place', 'name': 'Name', 'age_group': 'Age Group'})
    )
