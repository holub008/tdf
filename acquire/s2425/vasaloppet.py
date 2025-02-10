import re
import polars as pl
from acquire.mrr import scrape_race

EVENT_ID = '325478'
MAIN_RACE_ID = '3_44CF8A'
PARTICIPATION_RACE_IDS = ['#1_7AAEAD', '4_05A4C0', '2_50D290', '6_2DD4FF']

NAME_REGEX = r'((A-Z[a-z]+)+)([A-Z| ]+)'
AGES_REGEX = r'([0-9]+) to ([0-9]+)'


def _split_names(df: pl.DataFrame) -> pl.DataFrame:
    def extract_last(n: str) -> str:
        m = re.match(NAME_REGEX, n)
        if not m:
            return None
        return m.group(2)

    def extract_first(n: str) -> str:
        m = re.match(NAME_REGEX, n)
        if not m:
            return None
        return m.group(0)

    last_names = df['Name'].map_elements(lambda n: extract_last(n))
    first_names = df['Name'].map_elements(lambda n: extract_first(n))
    return df.with_columns(
        first_names.alias('first_name'),
        last_names.alias('last_name')
    )


def _guess_ages(df: pl.DataFrame) -> pl.DataFrame:
    # we don't get the actual age-- so we pick the midpoint of the range given :/
    def interpolate_age(age_group: str) -> float:
        m = re.match(AGES_REGEX, age_group)
        if not m:
            return None
        lower = float(m.group(0))
        upper = float(m.group(1))
        return (upper + lower) / 2
    ages = df['AG (Rank)'].map_elements(lambda ag: interpolate_age(ag))

    return df.with_columns(ages.alias('age'))


def _attach_gender(df: pl.DataFrame) -> pl.DataFrame:
    df.with_columns(pl.col('Gender (Rnk)').str.slice(0, 1).alias('gender'))
    return df


def _structure(df: pl.DataFrame) -> pl.DataFrame:
    # first_name,last_name,age,gender,time,place
    df = _split_names(df)
    df = _guess_ages(df)
    df = _attach_gender(df)
    df = df.rename({ 'Place': 'place', 'Time': 'time' })
    return df.select(['first_name', 'last_name', 'age', 'gender', 'time', 'place'])


def get_results(participation_races: bool) -> pl.DataFrame:
    if participation_races:
        all_raw = [scrape_race(EVENT_ID, rid) for rid in PARTICIPATION_RACE_IDS]
        print(all_raw[0])
        return pl.concat([_structure(r) for r in all_raw])
    else:
        raw = scrape_race(EVENT_ID, MAIN_RACE_ID)
        return _structure(raw)
