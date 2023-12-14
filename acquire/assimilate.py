import polars as pl
from nameparser import HumanName
import pytimeparse

from const import Gender
from dao import RawResults


def _extract_name_parts(full_name: str) -> tuple:
    n = HumanName(full_name)
    return n.first, n.last


def _attach_name_parts(df: pl.DataFrame) -> pl.DataFrame:
    name_parts = df.select(pl.col('name')).map_rows(lambda r: _extract_name_parts(r[0])) \
        .rename({'column_0': 'first_name', 'column_1': 'last_name'})

    return pl.concat([df, name_parts], how='horizontal')


def _coerce_numerics(df: pl.DataFrame) -> pl.DataFrame:
    numeric_place = df.select(pl.col('gender_place').cast(pl.Int64))
    # convert to seconds
    numeric_time = df.select(pl.col('time')).map_rows(lambda r: pytimeparse.parse(r[0]))
    numeric_age = df.select(pl.col('age').str.strip().cast(pl.Int64))
    with_numeric_time = pl.concat([
        df.drop((['gender_place', 'time', 'age'])),
        numeric_place,
        numeric_time,
        numeric_age
    ], how='horizontal')

    return with_numeric_time


def _assimilate_gender(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl
                           .when(pl.col('gender').str.to_lowercase().str.strip().str.starts_with('m'))
                           .then(Gender.male.name)
                           .when(pl.col('gender').str.to_lowercase().str.strip().str.starts_with('f'))
                           .then(Gender.female.name)
                           .otherwise(None)
                           .alias("gender"))


def assimilate_raw_results(df: pl.DataFrame) -> RawResults:
    with_name_parts = _attach_name_parts(df)
    with_numerics = _coerce_numerics(with_name_parts)
    with_gender = _assimilate_gender(with_numerics)

    # todo work out dao business
    return with_gender
