import requests
import polars as pl
from bs4 import BeautifulSoup

from acquire.assimilate import assimilate_raw_results
from tdfio.dao import RawResults


def _attach_gender_place(df: pl.DataFrame) -> pl.DataFrame:
    return df.sort(pl.col('Place').cast(pl.Int64)).with_columns(
        pl.col('Place').cast(pl.Int64).rank(method='ordinal').over('Sex').alias('gender_place')
    )


def scrape_race(race_id: int) -> RawResults:
    all_rows = []
    header = None
    offset = 0
    per_page = 50
    
    while True:
        res = requests.get('https://www.mtecresults.com/race/rankedResults',
                                params={'raceId': str(race_id), 'rankingType': 'OVERALL', 'perPage': str(per_page), 'offset': str(offset)},
                                headers={
                                    'X-Requested-With': 'XMLHttpRequest',
                                    # mtec uses cloudfront, which seems to be configured with minimal UA checking
                                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
                                })
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # Get header on first iteration
        if header is None:
            header = [cell.text for cell in soup.select('thead tr th')]
        
        # Get rows from current page
        rows = [[cell.text.strip() for cell in row.select('td')] for row in soup.select('tbody tr')]
        
        # Break if no rows returned
        if len(rows) == 0:
            break
        
        all_rows.extend(rows)
        offset += per_page

    data = {col: [row[i] for row in all_rows] for i, col in enumerate(header)}
    rr = _attach_gender_place(pl.DataFrame(data)).rename({
        'Name': 'name',
        'Sex': 'gender',
        'Age': 'age',
        'City': 'city',
        'State': 'state',
        'Time': 'time',
        'Place': 'place',
    })\
        .with_columns(pl.concat_str([pl.col('city').str.strip(), pl.col('state').str.strip()], separator=', ').alias('location'))\
        .select(pl.col('name'), pl.col('gender'), pl.col('age'), pl.col('location'), pl.col('time'), pl.col('gender_place'))
    return assimilate_raw_results(rr)
