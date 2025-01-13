import requests
import polars as pl
from bs4 import BeautifulSoup

from acquire.assimilate import assimilate_raw_results
from tdfio.dao import RawResults


def _attach_gender_place(df: pl.DataFrame) -> pl.DataFrame:
    # sometimes this data will be missing, which makes the result useless to us
    valid_df = df.filter(~(pl.col('SexPl').str.strip() == ''))
    return valid_df.with_columns(
        (pl.col('SexPl').str.split('/').list.get(0).str.strip()).cast(pl.Int64).alias('gender_place')
    )


def scrape_race(race_id: int) -> RawResults:
    res = requests.get('https://www.mtecresults.com/race/quickResults',
                            # who knows what max perPage the server will allow, but 500 should be good for most races
                            params={'raceid': str(race_id), 'version': '31', 'overall': 'yes', 'perPage': '500'},
                            headers={
                                'X-Requested-With': 'XMLHttpRequest',
                                # mtec uses cloudfront, which seems to be configured with minimal UA checking
                                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
                            })
    soup = BeautifulSoup(res.text, 'html.parser')

    header = [cell.text for cell in soup.select('thead tr th')]
    rows = [[cell.text.strip() for cell in row.select('td')] for row in soup.select('tbody tr')]

    data = {col: [row[i] for row in rows] for i, col in enumerate(header)}
    data['raw_result_id'] = range(1, len(rows) + 1)
    rr = _attach_gender_place(pl.DataFrame(data)).rename({
        'Name': 'name',
        'Sex': 'gender',
        'Age': 'age',
        'City': 'city',
        'State': 'state',
        'Time': 'time',
    })\
        .with_columns(pl.concat_str([pl.col('city').str.strip(), pl.col('state').str.strip()], separator=', ').alias('location'))\
        .select(pl.col('raw_result_id'), pl.col('name'), pl.col('gender'), pl.col('age'), pl.col('location'), pl.col('time'), pl.col('gender_place'))
    return assimilate_raw_results(rr)
