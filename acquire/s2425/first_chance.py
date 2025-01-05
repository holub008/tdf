import polars as pl

from acquire.assimilate import assimilate_raw_results


def _attach_gender_place(df: pl.DataFrame) -> pl.DataFrame:
    wr = df.filter(pl.col('Gender') == 'Female')
    mr = df.filter(pl.col('Gender') == 'Male')

    wr = wr.with_columns(pl.Series(name='gender_place', values=range(1, wr.shape[0] + 1)))
    mr = mr.with_columns(pl.Series(name='gender_place', values=range(1, mr.shape[0] + 1)))

    return pl.concat([wr, mr])


def get_results() -> pl.DataFrame:
    raw = pl.read_csv('./acquire/s2425/first_chance.csv').filter(pl.col('Place').is_not_null())

    rr = _attach_gender_place(raw).rename({
        'First name': 'first_name',
        'Last name': 'last_name',
        'Gender': 'gender',
        'Age': 'age',
        'Time': 'time',
    })\
        .select(pl.col('first_name'), pl.col('last_name'), pl.col('gender'), pl.col('age'), pl.col('time'), pl.col('gender_place'))

    return assimilate_raw_results(rr)
