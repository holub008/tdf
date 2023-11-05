import polars as pl

from tdfio.const import Technique, Gender


def _precondition_columns(df, required):
    df_cols = df.get_columns()
    for req in required:
        matches = [dc for dc in df_cols if dc.name == req[0]]
        if len(matches) == 0:
            raise ValueError(f'Missing required column {req[0]}')
        if len(matches) > 1:
            raise ValueError(f'Column {req[0]} found multiple times; must only be given once')

        match = matches[0]
        if match.dtype not in req[1]:
            raise ValueError(f'Column {req[0]} has incorrect type ({match.dtype})')


def _block_write_method():
    raise RuntimeError('Cannot modify DAO. If you intend to modify, make a .clone() first')

class StructuredReadOnlyDF(pl.DataFrame):
    """
    provides some semblance of type safety in a dataframe by:
      - checking df columns for name & type
      - disabling most write methods on the df
        - NOTE: using copy-to-modify methods (eg join()) will "eject" the df
        - if you want to modify the df, .clone() it to eject, then rewrap with `from_df`
      - adding some common sense constraints to the data
    AND, allowing clients to pass around a single type
    """

    def __init__(self, columns, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _precondition_columns(self, columns)

    # these methods are selected from https://pola-rs.github.io/polars/py-polars/html/reference/dataframe/modify_select.html# as of 2023-11-05
    # most methods listed there are not in-place, and therefore aren't blocked

    def drop_in_place(self):
        _block_write_method()

    def insert_at_idx(self):
        _block_write_method()

    def replace_at_idx(self):
        _block_write_method()

    def set_sorted(self):
        _block_write_method()

class Events(StructuredReadOnlyDF):
    @staticmethod
    def from_df(df: pl.DataFrame):
        return Events(df.to_dict())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _precondition_columns(self, [
            ('id', pl.INTEGER_DTYPES),
            ('name', {pl.Utf8}),
            ('image', {}),
            ('website', {pl.Utf8}),
            ('date', pl.DATETIME_DTYPES),
            ('technique', {pl.Utf8}),
            ('distance', pl.FLOAT_DTYPES)
        ])

        # TODO categorical type? replace enum values?
        techniques_given = set(self.unique('technique').get_column('technique').to_list())
        for tg in techniques_given:
            # will raise an error if not valid
            Technique.from_string(tg)


class Racers(StructuredReadOnlyDF):
    @staticmethod
    def from_df(df: pl.DataFrame):
        return Racers(df.to_dict())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _precondition_columns([
            ('id', pl.INTEGER_DTYPES),
            ('first_name', {pl.Utf8}),
            ('last_name', {pl.Utf8}),
            ('gender', {pl.Utf8}),
            ('age', pl.INTEGER_DTYPES), # todo replace with DoB estimate?
            # TODO lower and upper age bounds
            ('location', {pl.Utf8}),
        ])

        # TODO categorical type? replace enum values?
        genders_given = set(self.unique('gender').get_column('gender').to_list())
        for gg in genders_given:
            Gender.from_string(gg)

        # TODO unique ids


class Teams(StructuredReadOnlyDF):
    @staticmethod
    def from_df(df: pl.DataFrame):
        return Teams(df.to_dict())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _precondition_columns(self, [
            ('id', pl.INTEGER_DTYPES),
            ('name', {pl.Utf8}),
        ])


class TeamMemberships(StructuredReadOnlyDF):
    @staticmethod
    def from_df(df: pl.DataFrame):
        return TeamMemberships(df.to_dict())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _precondition_columns(self, [
            ('racer_id', pl.INTEGER_DTYPES),
            ('team_id', pl.INTEGER_DTYPES),
        ])
        # TODO unique


class MatchedResults(StructuredReadOnlyDF):
    @staticmethod
    def from_df(df: pl.DataFrame):
        return MatchedResults(df.to_dict())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _precondition_columns(self, [
            ('event_id', pl.INTEGER_DTYPES),
            ('racer_id', pl.INTEGER_DTYPES),
            ('place', pl.INTEGER_DTYPES),
            ('time', pl.FLOAT_DTYPES),
            ('placement_points', pl.FLOAT_DTYPES),
            ('age_advantage_points', pl.FLOAT_DTYPES),
        ])
        # TODO (event,racer) are unique


class EventIncentives(StructuredReadOnlyDF):
    @staticmethod
    def from_df(df: pl.DataFrame):
        return EventIncentives(df.to_dict())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _precondition_columns(self, [
            ('racer_id', pl.INTEGER_DTYPES),
            ('number_of_events', pl.INTEGER_DTYPES),
            ('points', pl.FLOAT_DTYPES),
        ])
        # TODO racers are unique


class IndividualStandings(StructuredReadOnlyDF):
    @staticmethod
    def from_df(df: pl.DataFrame):
        return IndividualStandings(df.to_dict())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _precondition_columns(self, [
            ('racer_id', pl.INTEGER_DTYPES),
            ('place', pl.INTEGER_DTYPES),
            ('points', pl.FLOAT_DTYPES),
        ])

        # TODO places are consecutive from 1 to n rows
        # TODO racers are unique


class TeamStandings(StructuredReadOnlyDF):
    @staticmethod
    def from_df(df: pl.DataFrame):
        return IndividualStandings(df.to_dict())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _precondition_columns(self, [
            ('team_id', pl.INTEGER_DTYPES),
            ('place', pl.INTEGER_DTYPES),
            ('points', pl.FLOAT_DTYPES),
        ])
        # TODO places are consecutive from 1 to n rows
        # TODO teams are unique