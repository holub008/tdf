import polars as pl

from tdfio.const import Gender
from orchestrate.s2324 import Event


def load_results(e: Event, g: Gender) -> pl.DataFrame:
    return pl.read_csv(f'db/s2324/{e.to_string()}_{g.to_string()}.csv')
