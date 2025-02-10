import polars as pl
from bs4 import BeautifulSoup, PageElement
import requests


def _html_to_polars(soup) -> pl.DataFrame:
    # javascript must mangle the table post-DOM, since
    headers = [th.text.strip('"') for th in soup.find('thead').find_all('th')]
    rows = []
    body = soup.find('tbody', attrs={'class': 'tb_1Data'})
    for tr in body.find_all('tr'):
        row = {}
        for ix, td in tr.find_all('td'):
            row[headers[ix]] = td.text
        rows.append(row)
    return pl.DataFrame(rows)


def scrape_race(event_id: str, race_id: str) -> pl.DataFrame:
    url = f'https://my.raceresult.com/{event_id}/results#{race_id}'
    r = requests.get(url)
    bs = BeautifulSoup(r.text)
    raw_tab = bs.find('table', attrs={'class': 'MainTable'})
    if not raw_tab:
        raise ValueError('Missing `MainTable` that would contain results')

    return _html_to_polars(raw_tab)


