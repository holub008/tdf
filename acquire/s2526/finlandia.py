import polars as pl

from tdfio.const import Gender

MAIN_RACES = ['BUENA VISTA 18 KM CLASSICAL', 'BEMIDJITHON 18 KM FREESTYLE']
PARTICIPATION_RACES = ['NORTHWOODS TOUR 6 KM']

def get_results(participation_races: bool) -> pl.DataFrame:
    raw = pl.read_csv(f'acquire/s2526/finlandia.csv')
    
    races = PARTICIPATION_RACES if participation_races else MAIN_RACES
    filtered = raw.filter(pl.col('event').is_in(races))
    
    filtered = filtered.with_columns([
        pl.col('name').str.split(' ').list.first().alias('first_name'),
        pl.col('name').str.split(' ').list.last().alias('last_name'),
    ])
    
    filtered = filtered.with_columns(
        pl.when(pl.col('gender') == 'M')
          .then(pl.lit(str(Gender.male)))
          .when(pl.col('gender') == 'F')
          .then(pl.lit(str(Gender.female)))
          .alias('gender')
    )
    
    return filtered.select([
        'first_name',
        'last_name',
        'age',
        'gender',
        pl.col('place').alias('gender_place')
    ])
