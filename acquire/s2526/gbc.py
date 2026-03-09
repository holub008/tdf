import json
import re
from typing import Optional

import polars as pl

from tdfio.const import Gender

AGE_CLASS_REGEX = r'([M|F])([0-9]{1,3})-([0-9]{1,3}).*'


def _name_capitalize(n: str) -> str:
    return " ".join(
        part.capitalize() for part in n.strip().replace("-", " - ").replace("'", " ' ").split()
    ).replace(" - ", "-").replace(" ' ", "'")


def _parse_name(n: str) -> tuple[str, str]:
    first_space = n.index(' ')
    return n[0:first_space], _name_capitalize(n[(first_space+1):])


def _infer_age_gender(age_class: str) -> Optional[tuple[Optional[int], Gender]]:
    """
    the race did not publish exact ages, so per standard practice, we drop it in the middle of the age class
    They look like this: "M 50 to 54 (2)"

    If the whole return is None, then the result should be dropped
    the classic tour doesn't give ages, so age is also given as an optional. This IS still a valid result
    """
    # i have no idea wtf is going on with their age groups
    if '70+' in age_class:
        return 70, Gender.male if age_class.startswith('M') else Gender.female
    if '75+' in age_class:
        return 75, Gender.male if age_class.startswith('M') else Gender.female
    if '12-' in age_class:
        return 12, Gender.male if age_class.startswith('M') else Gender.female
    if '19-' in age_class:
        return 19, Gender.male if age_class.startswith('M') else Gender.female
    if '16-' in age_class:
        return 16, Gender.male if age_class.startswith('M') else Gender.female

    basic_match = re.match(AGE_CLASS_REGEX, age_class)
    if not basic_match:
        print(age_class)
    age_low = int(basic_match.group(2))
    age_high = int(basic_match.group(3))

    gender_raw = basic_match.group(1)
    if gender_raw == 'M':
        gender = Gender.male
    elif gender_raw == 'F':
        gender = Gender.female
    else:
        raise ValueError(f'Bad gender extraction: {gender_raw}')

    return round((age_high + age_low) / 2), gender


def _attach_gender_place(results: list[list]) -> list[list]:
    m_results = [r for r in results if r[3] == Gender.male.to_string()]
    f_results = [r for r in results if r[3] == Gender.female.to_string()]

    if len(m_results) + len(f_results) != len(results):
        raise ValueError('Unexpected gender summation')

    placed_m = [r + [ix + 1] for ix, r in enumerate(sorted(m_results, key=lambda r: r[4]))]
    placed_f = [r + [ix + 1] for ix, r in enumerate(sorted(f_results, key=lambda r: r[4]))]

    return placed_m + placed_f


def _parse(j: dict, key: str) -> pl.DataFrame:
    results = j['data'][key]
    parsed_rows = []
    for r in results:
        if len(r) == 9:
            (bib_number, idk, overall_place, raw_name,
             city_state, gender_age, start_time, lap1_time, total_time) = r
        elif len(r) == 8:
            (bib_number, idk, overall_place, raw_name,
             city_state, gender_age, start_time, total_time) = r
        else:
            raise ValueError(f'Unexpected result row length: {len(r)}')

        if overall_place in ('DNS', 'DNF', '*'):
            continue

        first, last = _parse_name(raw_name)
        ag = _infer_age_gender(gender_age)
        if not ag:
            continue
        age, gender = ag

        parsed_rows.append([first, last, age, str(gender), int(overall_place)])

    parsed_rows = _attach_gender_place(parsed_rows)

    return pl.DataFrame(parsed_rows, {
        "first_name": pl.Utf8,
        "last_name": pl.Utf8,
        "age": pl.Int64,
        "gender": pl.Utf8,
        "overall_place": pl.Int64,
        "gender_place": pl.Int64
    })


def get_results(participation_races: bool) -> pl.DataFrame:
    """
    trying to interact with the myraceresult API baffles me
    (if intentional obfuscation by the developers, nice work!)
    so i no longer bother and just grab json from the dev console
    """
    if participation_races:
        filenames = [
            ('10k_classic.json', '#1_10km Classic'),
            ('10k_skate.json', '#1_10km Freestyle'),
            ('20k_skiathlon.json', '#1_20km Skiathlon'),
            ('24k_classic.json', '#1_24km Classic'),
            ('24k_skate.json', '#1_24km Freestyle'),
            ('52k_classic.json', '#1_52km Classic'),
            ('52k_skiathlon.json', '#1_52km Skiathlon'),
        ]
    else:
        filenames = [
            ('52k_skate.json', '#1_52km Freestyle'),
        ]

    dfs = []
    for fname, key in filenames:
        print(fname)
        with open(f'acquire/s2526/gbc/{fname}', 'r') as f:
            j = json.load(f)
        dfs.append(_parse(j, key))

    return pl.concat(dfs)
