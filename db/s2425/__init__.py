import os
import polars as pl

from orchestrate.s2425 import Event
from tdfio.const import Gender


def load_results(e: Event, g: Gender) -> pl.DataFrame:
    fp = f'db/s2425/{e.to_string()}_{g.to_string()}.csv'
    if os.path.exists(fp):
        return pl.read_csv(f'db/s2425/{e.to_string()}_{g.to_string()}.csv')
    else:
        print(f'Skipping over missing file: {fp}')
        return None