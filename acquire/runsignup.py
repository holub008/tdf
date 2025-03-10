import requests
import polars as pl


def scrape_race_results(event_id: int, race_id: int) -> pl.DataFrame:

    url = f'https://runsignup.com/Race/Results/{str(event_id)}'
    params = {
        'resultSetId': str(race_id),
        'page': 1,
        'num': 100,  # for bigger races, this will require pagination, skipping since unneeded for now
    }

    response = requests.get(url, params=params, headers={'Accept': 'application/json'})
    response.raise_for_status()
    data = response.json()

    headings = [h['key'] for h in data['headings']]
    return pl.DataFrame(data['resultSet']['results'], schema=headings)
