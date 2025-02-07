import requests
from bs4 import BeautifulSoup
import polars as pl
import re

from acquire.assimilate import assimilate_raw_results

MAIN_RACE_ID = 18382
PARTICIPATION_RACE_IDS = [18381, 18386, 18383, 18384]


def scrape_plain_text_results(race_id) -> str:
    r = requests.get(f'https://www.mtecresults.com/race/leaderboard/{race_id}',
                     headers={
                                'X-Requested-With': 'XMLHttpRequest',
                                # mtec uses cloudfront, which seems to be configured with minimal UA checking
                                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
                            })
    soup = BeautifulSoup(r.text, 'html.parser')
    return soup.find('div', attrs={'class': 'racetextresults'}).find('pre').string


def parse_plain_text_results(text: str) -> pl.DataFrame:
    lines = text.strip().split("\n")

    data = []
    header_found = False
    for line in lines:
        if re.match(r"Place DivPl", line):
            header_found = True
            continue

        if header_found:
            match = re.match(
                r"\s*(\d+)\s+(\d+/\d+)\s+(\d+)\s+([\w\s'-]+)\s+(\d+)\s+([MF])\s+([\w\s'.-]+)\s+(\w{2})\s+(\d+:\d+:\d+|\d+:\d+)\s+(\d+:\d+)",
                line)

            if match:
                data.append([m.strip() for m in match.groups()])

    df = pl.DataFrame(
        data,
        schema=[
            "Place", "DivPl", "Bib", "Name", "Age", "Sex", "City", "State", "Time", "Pace"
        ]
    )
    df = df.with_columns((pl.col('Name').str.split(' ').list.get(0)).alias('first_name'))
    df = df.with_columns((pl.col('Name').str.split(' ').list.slice(1).list.join(' ')).alias('last_name'))
    df = df.rename({'Age': 'age', 'Sex': 'gender', 'Time': 'time', 'Place': 'place'})

    return df.select(['first_name', 'last_name', 'age', 'gender', 'time', 'place'])


def make_results_file(race_ids, filepath):
    res_dfs = [parse_plain_text_results(scrape_plain_text_results(rid)) for rid in race_ids]
    total = pl.concat(res_dfs)
    total.write_csv(filepath)


def _attach_gender_place(df: pl.DataFrame) -> pl.DataFrame:
    wr = df.filter(pl.col('gender') == 'M')
    mr = df.filter(pl.col('gender') == 'F')

    wr = wr.with_columns(pl.Series(name='gender_place', values=range(1, wr.shape[0] + 1)))
    mr = mr.with_columns(pl.Series(name='gender_place', values=range(1, mr.shape[0] + 1)))

    return pl.concat([wr, mr])


def get_results(participation_races=False) -> pl.DataFrame:
    raw = pl.read_csv('./acquire/s2425/ashwabay_participants.csv' if participation_races else './acquire/s2425/ashwabay.csv')

    rr = _attach_gender_place(raw)
    return assimilate_raw_results(rr)


if __name__ == '__main__':
    make_results_file([MAIN_RACE_ID], 'acquire/s2425/ashwabay.csv')
    make_results_file(PARTICIPATION_RACE_IDS, 'acquire/s2425/ashwabay_participants.csv')