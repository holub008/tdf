# tdf
Result aggregation and scoring for the Tour de Finn Ski series

## Developing

### Set up

Setup a venv & install dependencies:

```shell
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Project Structure

- `acquire/` contains logic for scraping or otherwise obtaining and structuring results data
- `db/` contains well structured results data
    - the data here is expected to be modified (e.g. to correct name misspellings)
- `match/` contains generic logic for matching/joining racers across multiple results
- `orchestrate/` contains scripts necessary. Scores can be recomputed (from data in `db/`) at any point in time
    using this script
- `score/` contains generic logic for computing TDF points from results
- `tdfio/` has data structures common to race results and scoring

## Running

### Rationale

This project is set up to run & be modified on two periods:

1. Per-season
2. Per-race week

Seasons are denoted in how data is `acquire/`d, `orchestrate/`d, and placed in a `db/`, looking like `s2425`.

Typically, each race week will require the creation of:

- a new `acquire/` script, which retrieves/constructs a dataframe of results with no particular structure
- an event `orchestrate/` script, which computes points for the event and places it into the `db/`
    - the resulting file in `db/` will occasionally need to be edited by hand when there are name conflicts or other problems 
- the addition of the event to `orchestrate/season/__init__.py` & `orchstrate/season/__main__.py`
    - this process involves a lot of copy/paste from season to season and week to week. Ideally it will be abstracted
once we settle into a point system.

The reason bullets 2 & 3 are separate is to allow manual editing of a results file (e.g. to deduplicate names).
This means that the corresponding `acquire/` script should only be run once, to avoid overwriting any manual edits to the `db/` file.

### Commands

To compute points for an event, e.g. city of lakes loppet, run:

```commandline
python -m orchestrate.s2425.coll
```

This will write `{event}_{gender}.csv` files under `db/s2425`.

To compute standings for a season, e.g. the 24/25 season, run:

```commandline
python -m orchestrate.s2425
```

This will write `*standings.csv` files in `orchestrate`. Note that some debug logging for missing files is expected,
since most races do not report an NB division, but we still check for it.