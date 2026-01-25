import json
import re
import polars as pl

from tdfio.const import Gender

NAME_REGEX = r'(([A-Z][a-z]+( |-))+)([A-Z| |-]+)'
AGE_CLASS_REGEX = r'([M|F]) ([0-9]{1-3}) to ([0-9]{1-3}) \([0-9]+\)'


def _name_capitalize(n: str) -> str:
    return " ".join(
        part.capitalize() for part in n.strip().replace("-", " - ").split()
    ).replace(" - ", "-")


def _parse_name(n: str) -> tuple[str, str]:
    m = re.match(NAME_REGEX, n)
    if not m:
        raise ValueError(f"Name regex is displeased with {n}")
    return m.group(1).strip(), _name_capitalize(m.group(4))


def _infer_age_gender(age_class: str) -> tuple[int, Gender]:
    """
    the race did not publish exact ages, so per standard practice, we drop it in the middle of the age class
    They look like this: "M 50 to 54 (2)"
    """
    match = re.match(AGE_CLASS_REGEX, age_class)
    if not match:
        raise ValueError('Unexpected')

    age_low = match.group(3)
    age_high = match.group(4)

    gender_raw = match.group(2)
    if gender_raw == 'M':
        gender = Gender.male
    elif gender_raw == 'F':
        gender = Gender.female
    else:
        raise ValueError(f'Bad gender extraction: {gender_raw}')

    return (age_high + age_low) / 2, gender


def _parse_gender_place(gender_place: str) -> int:
    place_match = re.match(r'\(([0-9]+)\)', gender_place)
    if not place_match:
        raise ValueError(f"Unexpected gender place format: {gender_place}")

    return int(place_match.group(1))


def _parse(j: dict) -> pl.DataFrame:
    results = j['data']
    parsed_rows = []
    for r in results:
        # there is one result row that is just `[184]`. Don't ask questions of MRR you don't want answers to
        if not len(r) == 10:
            continue
        (bib_number, idk, overall_place, raw_name,
         city_state, idk2, raw_gender_place, age_class,
         split_1, split_2, elapsed_time, tb) = r

        if overall_place in ('DNS', 'DNF'):
            continue

        first, last = _parse_name(raw_name)
        age, gender = _infer_age_gender(age_class)

        gender_place = _parse_gender_place(raw_gender_place)
        parsed_rows.append([first, last, age, str(gender), gender_place])

    return pl.DataFrame(parsed_rows, ['first_name', 'last_name', 'age', 'gender', 'gender_place'])


def get_results(participation_races: bool) -> pl.DataFrame:
    """
    trying to interact with the myraceresult API baffles me
    (if intentional obfuscation by the developers, nice work!)
    so i no longer bother and just grab json from the dev console
    """
    if participation_races:
        filenames = ['noquemanon_50k_classic.json']
    else:
        filenames = [
            'noquemanon_50k_skate.json',
            'noquemanon_50k_classic_tour.json',
            'noquemanon_24k_skate.json',
            'noquemanon_24k_classic.json',
            'noquemanon_12k_skate.json',
            'noquemanon_12k_classic.json',
        ]

    dfs = []
    for fname in filenames:
        with open(f'acquire/s2526/noquemanon/{fname}.json', 'r') as f:
            j = json.load(f)
        dfs.append(_parse(j))

    return pl.concat(dfs)
