import polars


def _precondition_racers_df(df: polars.DataFrame):
    column_set = {c.name for c in df.get_columns()}
    required = {'id', 'first_name', 'last_name', 'age', 'age_lower', 'age_upper', 'location', 'gender'}
    if column_set != required:
        missing = required - column_set
        raise TypeError(f'Racers must have certain columns; the following were missing: {missing}')


class RacerDB:
    @staticmethod
    def load_from_grist():
        pass

    def __init__(self, racers: polars.DataFrame):
        _precondition_racers_df(racers)
        self.racers = racers

    def get(self, racer_id: int) -> dict | None:
        pass

    def lookup(self, raw_racer: dict):
        """
        TODO return type
        TODO this would be an appropriate location to inject overrides (e.g. when we manually scan results or get emails)
        :param raw_racer:
        :return:
        """
        pass