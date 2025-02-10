import polars as pl
import requests


def _get_key(event_id: str) -> tuple:
    url = f'https://my.raceresult.com/{event_id}/RRPublish/data/config?page=results&noVisitor=1&v=1'
    r = requests.get(url)
    j = r.json()

    return j['key']

def _load_results(event_id: str, key: str, contest_number: int, list_name: str) -> pl.DataFrame:
    url = f'https://my3.raceresult.com/{event_id}/RRPublish/data/list'
    r = requests.get(url, params={
        'key': key,
        'listname': list_name,
        'page': 'results',
        'contest': contest_number,
        'r': 'all',
        'l': 0,
    })
    j = r.json()
    columns = [f['Label'] for f in j['list']['Fields']]
    rows = []
    for r in list(j['data'].values())[0]:
        row_dict = {}
        # this is specific to vasaloppet 2025. I can't make heads or tails of their API, so i'm hacking valid indices in
        for ix, c in enumerate(r[1:10]):
            row_dict[columns[ix]] = c
        rows.append(row_dict)

    return pl.DataFrame(rows)


def scrape_race(event_id: str, contest_number: int, list_name: str):
    key = _get_key(event_id)
    return _load_results(event_id, key, contest_number, list_name)

