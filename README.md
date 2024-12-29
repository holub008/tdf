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

This project is set up to run & be modified on two periods:

1. Per-season
2. Per-race week

Seasons are denoted in how data is `acquire/`d, `orchestrate/`d, and placed in a `db/`, looking like `s2425`.

Typically, each race week will require the creation of a new `acquire/` script

