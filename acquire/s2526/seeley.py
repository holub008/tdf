import json
import re
from typing import Optional

import polars as pl

from tdfio.const import Gender

NAME_REGEX = r'(([A-Z][a-z|-]+ )+)([A-Z| |-]+)'


def _name_capitalize(n: str) -> str:
    return " ".join(
        part.capitalize() for part in n.strip().replace("-", " - ").split()
    ).replace(" - ", "-")


def _parse_name(n: str) -> Optional[tuple[str, str]]:
    m = re.match(NAME_REGEX, n)
    if not m:
        return None
    return m.group(1), _name_capitalize(m.group(3))


def _parse_gender_place(s: str) -> tuple[Gender, int]:
    if s.endswith('M'):
        gender = Gender.male
    elif s.endswith('F'):
        gender = Gender.female
    else:
        raise ValueError(f'Unexpected raw gender place string: {s}')
    place_match = re.match(r'([0-9]+)', s)
    if not place_match:
        raise ValueError(f'Invalid gender place format: {s}')
    gender_place = int(place_match.group(1))
    return gender, gender_place


def _parse(j: dict) -> pl.DataFrame:
    results = j['data']
    parsed_rows = []
    for r in results:
        # there is one result row that is just `[184]`. Don't ask questions of MRR you don't want answers to
        if not len(r) == 10:
            continue
        _, _, overall_place, raw_name, raw_age, _, raw_gender_place, _, time_raw, _ = r

        if overall_place in ('DNS', 'DNF'):
            continue

        first, last = _parse_name(raw_name)
        age = int(raw_age)
        gender, gender_place = _parse_gender_place(raw_gender_place)
        parsed_rows.append([first, last, age, str(gender), gender_place])

    return pl.DataFrame(parsed_rows, ['first_name', 'last_name', 'age', 'gender', 'gender_place'])


def get_results(participation_races: bool) -> pl.DataFrame:
    with open(f'acquire/s2526/{"seeley_42k" if participation_races else "seeley_22k"}.json', 'r') as f:
        j = json.load(f)
    return _parse(j)
