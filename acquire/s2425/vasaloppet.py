import re

import math
import polars as pl
from acquire.mrr import scrape_race
from tdfio.const import Gender

EVENT_ID = '325478'
# find + manually populate contests at a URL like:
# https://my.raceresult.com/325478/RRPublish/data/config?page=results&noVisitor=1&v=1
MAIN_RACE = [3, 'Result Lists|Overall Results']
PARTICIPATION_RACES = [
    [1, 'Result Lists|Overall Results'],
    [2, 'Result Lists|Overall Results'],
    [4, 'Result Lists|Overall Results'],
    [6, 'Result Lists|Overall Results'],
]

NAME_REGEX = r'(([A-Z][a-z]+ )+)([A-Z| ]+)'
AGES_REGEX = r'.* ([0-9]+) to ([0-9]+) .*'


def _split_names(df: pl.DataFrame) -> pl.DataFrame:
    def extract_last(n: str) -> str:
        m = re.match(NAME_REGEX, n)
        if not m:
            return None
        return m.group(3).capitalize()

    def extract_first(n: str) -> str:
        m = re.match(NAME_REGEX, n)
        if not m:
            return None
        return m.group(1).strip()

    last_names = df['Name'].map_elements(lambda n: extract_last(n))
    first_names = df['Name'].map_elements(lambda n: extract_first(n))
    return df.with_columns(
        first_names.alias('first_name'),
        last_names.alias('last_name')
    )


def _guess_ages(df: pl.DataFrame) -> pl.DataFrame:
    # we don't get the actual age-- so we pick the midpoint of the range given :/
    def interpolate_age(age_group: str) -> int:
        m = re.match(AGES_REGEX, age_group)
        if not m:
            return None
        lower = float(m.group(1))
        upper = float(m.group(2))
        return math.floor((upper + lower) / 2)

    ages = df['AG (Rank)'].map_elements(lambda ag: interpolate_age(ag))

    return df.with_columns(ages.alias('age'))


def _attach_gender(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col('Gender (Rnk)').str.starts_with('M'))
        .then(Gender.male.to_string())
        .otherwise(Gender.female.to_string())
        .alias('gender')
    )


def _attach_gender_place(df: pl.DataFrame) -> pl.DataFrame:
    wr = df.filter(pl.col('gender') == Gender.male.to_string())
    mr = df.filter(pl.col('gender') == Gender.female.to_string())

    wr = wr.with_columns(pl.Series(name='gender_place', values=range(1, wr.shape[0] + 1)))
    mr = mr.with_columns(pl.Series(name='gender_place', values=range(1, mr.shape[0] + 1)))

    return pl.concat([wr, mr])


def _structure(df: pl.DataFrame) -> pl.DataFrame:
    # first_name,last_name,age,gender,time,place
    df = _split_names(df)
    df = _guess_ages(df)
    df = _attach_gender(df)
    df = df.rename({'Place': 'place', 'Time': 'time'})
    return _attach_gender_place(df.select(['first_name', 'last_name', 'age', 'gender', 'time', 'place']))


def get_results(participation_races: bool) -> pl.DataFrame:
    if participation_races:
        all_raw = [scrape_race(EVENT_ID, pr[0], pr[1]) for pr in PARTICIPATION_RACES]
        return pl.concat([_structure(r) for r in all_raw])
    else:
        raw = scrape_race(EVENT_ID, MAIN_RACE[0], MAIN_RACE[1])
        return _structure(raw)
