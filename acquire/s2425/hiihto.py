import pytimeparse
import requests
from bs4 import BeautifulSoup
import polars as pl

from acquire.assimilate import assimilate_raw_results
from tdfio.const import Gender
from tdfio.dao import RawResults

MIXED_NAME_TO_GENDER = {
    'Alex Reich': 'male',
    'Julia Reich': 'female',
    'Craig Stolen': 'male',
    'Siri Stolen': 'female',
    'Erik Pieh': 'male',
    'Elspeth Ronnander': 'female',
    'Craig Cardinal': 'male',
    'Eva Reinicke': 'female',
    'Dan McGrath': 'male',
    'Jamie Lindfors': 'female',
    'Ben Larson': 'male',
    'Sarah Widder': 'female',
    'Saylor Landrum': 'female',
    'Henry Fischer': 'male',
    'Beckie Alexander': 'female',
    'Nico Alexander': 'male',
    'Leo Bramante': 'male',
    'Ellery Fay': 'female',
    'Steve Eberly': 'male',
    'Emily Broderson': 'female',
    'Gregory Pupillo': 'male',
    'Jackie Montpetit': 'female',
}

def stretch_class_results(class_wide_df: pl.DataFrame) -> pl.DataFrame:
    # EP seems to have bastardized their own system for first name = first racer, last name = second
    set1 = class_wide_df.with_columns(pl.col('First Name').str.rstrip('&').str.strip().alias('name'))
    set2 = class_wide_df.with_columns(pl.col('Last Name').str.strip().alias('name'))

    is_mixed = class_wide_df['Class'][0] == 'Mixed'
    if is_mixed:
        return pl.concat([set1, set2]).rename({
            'Total Time': 'time',
        }).with_columns(pl.col('name').apply(lambda n: MIXED_NAME_TO_GENDER[n]).alias('gender')).select('name', 'time', 'gender')
    else:
        gender = 'male' if class_wide_df['Class'][0] == 'Male' else 'female'
        return pl.concat([set1, set2]).rename({
            'Total Time': 'time',
        }).with_columns(pl.lit(gender).alias('gender')).select(pl.col('name'), pl.col('time'), pl.col('gender'))


def _attach_gender_places(gender_results: pl.DataFrame):
    numeric_time_gender_results = gender_results.with_columns(pl.col('time').apply(lambda t: pytimeparse.parse(t)).alias('time_secs'))
    return (numeric_time_gender_results.with_columns(pl.col('time_secs').rank(method="dense").cast(pl.Int32).alias("gender_place"))
            .select(pl.col('name'), pl.col('time'), pl.col('gender'), pl.col('gender_place')))


def stretch_relay_teams(wide_df: pl.DataFrame) -> pl.DataFrame:
    """
    each row corresponds to two racers
    mixed teams get sorted into their respective genders
    """
    stretched = wide_df.groupby('Class').map_groups(lambda g: stretch_class_results(g))
    out = []
    for g in ['male', 'female']:
        matches = stretched.filter(pl.col('gender').eq(g))
        out.append(_attach_gender_places(matches))
    return pl.concat(out)


def scrape_hiihto() -> RawResults:
    res = requests.get('https://www.endurancepromotions.com/ResultDetails.aspx',
                            params={'id': '1669'})
    soup = BeautifulSoup(res.text, 'html.parser')
    table_soup = soup.select_one('table.rgMasterTable')

    header = [cell.text.strip() for cell in table_soup.select('thead tr th.rgHeader')]
    rows = [[cell.text.strip() for cell in row.select('td')] for row in table_soup.select('tbody tr')]
    data = {col: [row[i] for row in rows] for i, col in enumerate(header)}
    wide_df = pl.DataFrame(data)
    rr = stretch_relay_teams(wide_df).with_columns(pl.lit(None).alias('age'))
    print(rr)

    return assimilate_raw_results(rr)


if __name__ == '__main__':
    print(scrape_hiihto())