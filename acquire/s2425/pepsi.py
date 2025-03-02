import re

import math
import polars as pl
from acquire.mrr import scrape_race
from tdfio.const import Gender

EVENT_ID = '329043'
# find + manually populate contests at a URL like:
# https://my.raceresult.com/325478/RRPublish/data/config?page=results&noVisitor=1&v=1
MAIN_RACE = [4, 'Result Lists|Overall Results 1Lap']
PARTICIPATION_RACES = [
    [1, 'Result Lists|Overall Results'],
    [2, 'Result Lists|Overall Results'],
    [3, 'Result Lists|Overall Results 1Lap'],
    [5, 'Result Lists|Overall Results 1Lap'],
]

NAME_REGEX = r'(([A-Z][a-z|-]+ )+)([A-Z| |-]+)'
AGE_GROUP_REGEX = r'(M|F)(([0-9]+)-([0-9]+)|75\+) \(.*\)'


def _split_names(df: pl.DataFrame) -> pl.DataFrame:
    def extract_last(n: str) -> str:
        return ' '.join(n.split(' ')[1:])

    def extract_first(n: str) -> str:
        return n.split(' ')[0]

    last_names = df['Name'].map_elements(lambda n: extract_last(n))
    first_names = df['Name'].map_elements(lambda n: extract_first(n))
    return df.with_columns(
        first_names.alias('first_name'),
        last_names.alias('last_name')
    )


def _guess_ages(df: pl.DataFrame) -> pl.DataFrame:
    # we don't get the actual age-- so we pick the midpoint of the range given :/
    def interpolate_age(age_group: str) -> int:
        m = re.match(AGE_GROUP_REGEX, age_group)
        if not m:
            return None
        if m.group(2).endswith('+'):
            lower = float(m.group(2).rstrip('+'))
            upper = lower
        else:
            lower = float(m.group(3))
            upper = float(m.group(4))
        return math.floor((upper + lower) / 2)

    ages = df['AG'].map_elements(lambda ag: interpolate_age(ag))

    return df.with_columns(ages.alias('age'))


def _attach_gender(df: pl.DataFrame) -> pl.DataFrame:
    def extract_gender(ag: str) -> str:
        m = re.match(AGE_GROUP_REGEX, ag)
        g_raw = m.group(1) if m else None
        if g_raw == 'M':
            return Gender.male.to_string()
        elif g_raw == 'F':
            return Gender.female.to_string()
        else:
            raise ValueError(f'Unexpected age group: {ag}')

    genders = df['AG'].map_elements(lambda ag: extract_gender(ag))

    return df.with_columns(genders.alias('gender'))


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
