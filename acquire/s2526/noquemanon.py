import json
import re
from typing import Optional

import polars as pl

from tdfio.const import Gender

NAME_REGEX = r'^(([A-Z][a-z]+( |-))+)([A-Z| |\-|\']+)$'
AGE_CLASS_REGEX = r'([M|F]) ?([0-9]{1,3}) to ([0-9]{1,3}) \(.+\)'
GENDER_ONLY_AGE_CLASS_REGEX = r'(Male|Female) \(.+\)'
VARIABLE_UNBOUNDED_AGE_CLASS_REGEX = r'([M|F]) +([0-9]{2}).*'


def _name_capitalize(n: str) -> str:
    return " ".join(
        part.capitalize() for part in n.strip().replace("-", " - ").replace("'", " ' ").split()
    ).replace(" - ", "-").replace(" ' ", "'")


def _parse_name(n: str) -> tuple[str, str]:
    # lol
    if n == ' NEED NAME':
        return "Need", "Name"

    m = re.match(NAME_REGEX, n)
    if not m:
        raise ValueError(f"Name regex is displeased with {n}")
    return m.group(1).strip(), _name_capitalize(m.group(4))


def _infer_age_gender(age_class: str) -> Optional[tuple[Optional[int], Gender]]:
    """
    the race did not publish exact ages, so per standard practice, we drop it in the middle of the age class
    They look like this: "M 50 to 54 (2)"

    If the whole return is None, then the result should be dropped
    the classic tour doesn't give ages, so age is also given as an optional. This IS still a valid result
    """
    basic_match = re.match(AGE_CLASS_REGEX, age_class)
    if not basic_match:
        gender_only_match = re.match(GENDER_ONLY_AGE_CLASS_REGEX, age_class)
        if gender_only_match:
            gender_raw = gender_only_match.group(1)
            if gender_raw == 'Male':
                gender = Gender.male
            elif gender_raw == 'Female':
                gender = Gender.female
            else:
                raise ValueError(f'Bad gender extraction: {gender_raw}')

            return None, gender
        else:
            # i hate this
            unbounded_match = re.match(VARIABLE_UNBOUNDED_AGE_CLASS_REGEX, age_class)
            if unbounded_match:
                gender_raw = unbounded_match.group(1)
                if gender_raw == 'M':
                    gender = Gender.male
                elif gender_raw == 'F':
                    gender = Gender.female
                else:
                    raise ValueError(f'Bad gender extraction: {gender_raw}')
                age = unbounded_match.group(2)
                return int(age), gender

            print(f"Mangled seeming age class we're dropping: '{age_class}'")
            return None
    else:
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


def _parse_gender_place(gender_place: str) -> int:
    place_match = re.match(r'(Male|Female) \(([0-9]+)\)', gender_place)
    if not place_match:
        raise ValueError(f"Unexpected gender place format: {gender_place}")

    return int(place_match.group(2))


def _parse(j: dict) -> pl.DataFrame:
    results = j['data']
    parsed_rows = []
    for r in results:
        # there is one result row that is just `[184]`. Don't ask questions of MRR you don't want answers to
        if not len(r) == 12:
            continue
        (bib_number, idk, overall_place, raw_name,
         city_state, idk2, raw_gender_place, age_class,
         split_1, split_2, elapsed_time, tb) = r

        if overall_place in ('DNS', 'DNF'):
            continue

        first, last = _parse_name(raw_name)
        ag = _infer_age_gender(age_class)
        if not ag:
            continue
        age, gender = ag

        gender_place = _parse_gender_place(raw_gender_place)
        parsed_rows.append([first, last, age, str(gender), gender_place])

    return pl.DataFrame(parsed_rows, {
        "first_name": pl.Utf8,
        "last_name": pl.Utf8,
        "age": pl.Int64,
        "gender": pl.Utf8,
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
            'noquemanon_50k_skate.json',
            'noquemanon_50k_classic_tour.json',
            'noquemanon_24k_skate.json',
            'noquemanon_24k_classic.json',
            'noquemanon_12k_skate.json',
            'noquemanon_12k_classic.json',
        ]
    else:
        filenames = ['noquemanon_50k_classic.json']

    dfs = []
    for fname in filenames:
        print(fname)
        with open(f'acquire/s2526/noquemanon/{fname}', 'r') as f:
            j = json.load(f)
        dfs.append(_parse(j))

    return pl.concat(dfs)
