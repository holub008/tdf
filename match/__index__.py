import polars as pl

from dao import MatchedResults, RawResults, Racers


def _process_names_for_matching(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("first_name").str.to_lowercase().str.replace_all(r"'", "").alias('matching_fn'),
        pl.col("last_name").str.to_lowercase().str.replace_all(r"'", "").alias('matching_ln')
    )


def make_racer_matches(results: RawResults, racers: Racers, event: dict) -> MatchedResults:

    results = _process_names_for_matching(results)
    racers = _process_names_for_matching(racers).rename({ 'id': 'racer_id' })

    joined = results.join(racers, on=["matching_fn", "matching_ln"], how="left")

    joined = joined.with_columns(
        (pl.col('age_x') - pl.col('age_y')).alias('age_diff')
    )

    best_matches = joined.group_by('raw_result_id').map_groups(lambda g: g.sort('age_diff', descending=True).head(1))

    best_matches.with_columns({
        'event_id': [event['id'] for _ in range(best_matches.shape[0])]
    })

    return MatchedResults.from_df(best_matches)
