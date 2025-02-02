import requests
from bs4 import BeautifulSoup
import polars as pl
import re

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
                r"\s*(\d+)\s+(\d+/\d+)\s+(\d+)\s+([\w\s'-]+)\s+(\d+)\s+([MF])\s+([\w\s'-]+)\s+(\w{2})\s+(\d+:\d+:\d+|\d+:\d+)\s+(\d+:\d+)",
                line)

            if match:
                data.append([m.strip() for m in match.groups()])

    df = pl.DataFrame(
        data,
        schema=[
            "Place", "DivPl", "Bib", "Name", "Age", "Sex", "City", "State", "Time", "Pace"
        ]
    )

    return df


def make_results_file(race_ids, filepath) -> pl.DataFrame:
    res_dfs = [parse_plain_text_results(scrape_plain_text_results(rid)) for rid in race_ids]
    total = pl.concat(res_dfs)
    total.write_csv(filepath)


if __name__ == '__main__':
    make_results_file([MAIN_RACE_ID], 'acquire/s2425/ashwabay.csv')
    make_results_file(PARTICIPATION_RACE_IDS, 'acquire/s2425/ashwabay_participants.csv')