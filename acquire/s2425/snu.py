import polars as pl
from acquire.runsignup import scrape_race_results
from tdfio.const import Gender

EVENT_ID=171785
MAIN_RACE_ID=532092
PARTICIPATION_RACE_IDS=[532094, 532093]

def _split_names(df: pl.DataFrame) -> pl.DataFrame:
    def extract_last(n: str) -> str:
        return ' '.join(n.split(' ')[1:])

    def extract_first(n: str) -> str:
        return n.split(' ')[0]

    last_names = df['name'].map_elements(lambda n: extract_last(n))
    first_names = df['name'].map_elements(lambda n: extract_first(n))
    return df.with_columns(
        first_names.alias('first_name'),
        last_names.alias('last_name')
    )


def _attach_gender(df: pl.DataFrame) -> pl.DataFrame:
    def extract_gender(g_raw: str) -> str:
        if g_raw == 'M':
            return Gender.male.to_string()
        elif g_raw == 'F':
            return Gender.female.to_string()
        else:
            raise ValueError(f'Unexpected gender: {g_raw}')

    genders = df['gender'].map_elements(lambda g: extract_gender(g))

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
    df = _attach_gender(df)
    df = df.rename({'race_placement': 'place', 'chip_time': 'time'})
    return _attach_gender_place(df.select(['first_name', 'last_name', 'age', 'gender', 'time', 'place']))


def get_results(participation_races: bool):
    if participation_races:
        all_raw = [scrape_race_results(EVENT_ID, prid) for prid in PARTICIPATION_RACE_IDS]
        return pl.concat([_structure(r) for r in all_raw])
    else:
        raw = scrape_race_results(EVENT_ID, MAIN_RACE_ID)
        return _structure(raw)
