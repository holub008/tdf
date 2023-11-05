import polars


def find_racer_match(racer: dict, racer_db_df: polars.DataFrame):
    """
    make matches on name. age and location, when present, are used as sanity checks
    """
    # name similarity
    # age similarity
    # location similarity