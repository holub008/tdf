import os
import polars as pl

from orchestrate.s2425 import Event
from tdfio.const import Gender


def load_results(e: Event, g: Gender) -> pl.DataFrame:
    fp = f'db/s2425/{e.to_string()}_{g.to_string()}.csv'
    if os.path.exists(fp):
        return pl.read_csv(f'db/s2425/{e.to_string()}_{g.to_string()}.csv')
    else:
        print(f'Skipping over missing file: {fp}')
        return None


TEAM_NAME_COL = 'Team Name:'


def _expand_team_row(r):
    team_name = r[TEAM_NAME_COL]
    name_columns = ['Team captain/representative name:', '2nd team member name', '3rd team member name'] + \
                   [f'{i}th team member name' for i in range(4, 11)]
    gender_columns = ['Team captain gender', '2nd team member gender', '3rd team member gender'] + \
                     [f'{i}th team member gender' for i in range(4, 11)]

    member_tups = []
    for i in range(len(name_columns)):
        nc = name_columns[i]
        gc = gender_columns[i]
        ncv = r.get(nc, None)
        gcv_raw = r.get(gc, None)
        if ncv and gcv_raw and (ncv.lower() not in ('na', 'n/a')):
            if gcv_raw.lower().startswith('m'):
                gcv = Gender.male
            elif gcv_raw.lower().startswith('f'):
                gcv = Gender.female
            else:
                # no commentary on gender identity offered- for scoring purposes even NB skiers are decided as F/M
                raise ValueError(f'Unexpected gender found: {gcv_raw}')
            name_parts = ncv.split(' ')
            if len(name_parts) < 2:
                print(name_parts)
                raise ValueError('Expected each name to contain 1 or more spaces to make simple first/last name for matching')
            fn = name_parts[0]
            ln = ' '.join(name_parts[1:])
            member_tups.append([team_name, gcv.to_string(), fn, ln])

    return member_tups


def load_team_membership() -> pl.DataFrame:
    raw = pl.read_csv(f'db/s2425/teams.csv')
    if not raw.n_unique(subset=TEAM_NAME_COL) == raw.shape[0]:
        raise ValueError('Expected team name to be unique, crashing out to avoid downstream problems')

    long_team_tups = [r2 for r in raw.iter_rows(named=True) for r2 in _expand_team_row(r)]
    membership = pl.DataFrame(long_team_tups, schema=['team_name', 'gender', 'first_name', 'last_name'])

    if not membership.n_unique(subset=['first_name', 'last_name']) == membership.shape[0]:
        raise ValueError('Expected racers (on first/last names) to have unique membership to teams')

    return membership
