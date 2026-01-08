from __future__ import annotations

from pathlib import Path
from typing import Optional

import requests
import polars as pl

from acquire.assimilate import assimilate_raw_results


SHEET_ID = "1-b5mwvrVxfELr2F0AiEZX3BT6ETFU0RMMdqv1dTLfkU"
GID = 1567928321

OUT_CSV = Path("acquire/s2526/bcfk.csv")


def _sheet_export_csv_url(sheet_id: str, gid: int) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def download_sheet_csv(sheet_id: str, gid: int, out_path: Path, timeout_s: int = 30) -> Path:
    out_path = out_path.expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    url = _sheet_export_csv_url(sheet_id, gid)
    r = requests.get(
        url,
        timeout=timeout_s,
        headers={
            "User-Agent": "Mozilla/5.0",
        },
    )

    ctype = (r.headers.get("content-type") or "").lower()
    if r.status_code != 200:
        raise RuntimeError(f"Failed to download sheet CSV (HTTP {r.status_code}) from {url}")

    if "text/csv" not in ctype and not r.text.lstrip().startswith(("Overall", '"Overall', "Overall Place", '"Overall Place')):
        snippet = r.text[:200].replace("\n", "\\n")
        raise RuntimeError(
            "Downloaded content does not look like CSV. "
            f"Content-Type={ctype!r}, first_chars={snippet!r}"
        )

    out_path.write_bytes(r.content)
    return out_path


def _split_name(full_name: str) -> tuple[str, str]:
    # "First Last", "First Middle Last", "Last, First" (handle a bit)
    if not full_name:
        return "", ""

    s = full_name.strip()

    # Handle "Last, First ..."
    if "," in s:
        parts = [p.strip() for p in s.split(",", 1)]
        last = parts[0]
        first = parts[1].split()[0] if len(parts) > 1 and parts[1] else ""
        return first, last

    toks = s.split()
    if len(toks) == 1:
        return toks[0], ""
    return toks[0], " ".join(toks[1:])


def sheet_csv_to_results_df(csv_path: Path) -> pl.DataFrame:
    """
    Read the downloaded CSV and normalize into the expected raw results schema.
    Output columns: first_name, last_name, age, gender, time, place
    """
    df = pl.read_csv(str(csv_path), infer_schema_length=200)

    # Normalize column names (in case someone edits header capitalization)
    # We'll map a few likely variants.
    colmap = {c: c.strip() for c in df.columns}
    df = df.rename(colmap)

    def find_col(*candidates: str) -> Optional[str]:
        lower = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand.lower() in lower:
                return lower[cand.lower()]
        return None

    col_place = find_col("Overall Place")
    col_name = find_col("Name")
    col_time = find_col("Finish Time")
    col_gender = find_col("Gender")
    col_age = find_col("Age")

    missing = [k for k, v in {
        "Overall Place": col_place,
        "Name": col_name,
        "Finish Time": col_time,
        "Gender": col_gender,
        "Age": col_age,
    }.items() if v is None]

    if missing:
        raise RuntimeError(
            f"CSV is missing expected columns: {missing}. "
            f"Found columns: {df.columns}"
        )

    # Basic cleanup: drop rows without a place or name
    df = df.filter(
        pl.col(col_place).is_not_null()
        & (pl.col(col_place).cast(pl.Utf8).str.strip_chars() != "")
        & pl.col(col_name).is_not_null()
        & (pl.col(col_name).cast(pl.Utf8).str.strip_chars() != "")
    )

    # Ensure place is int-ish (but keep robust if sheet has blanks/notes)
    df = df.with_columns([
        pl.col(col_place).cast(pl.Utf8).str.extract(r"(\d+)", 1).cast(pl.Int64).alias("place"),
        pl.col(col_time).cast(pl.Utf8).str.strip_chars().alias("time"),
        pl.col(col_gender).cast(pl.Utf8).str.strip_chars().alias("gender"),
        pl.col(col_age).cast(pl.Utf8).str.extract(r"(\d+)", 1).cast(pl.Int64).alias("age"),
        pl.col(col_name).cast(pl.Utf8).str.strip_chars().alias("_full_name"),
    ])

    # Split name into first/last
    df = df.with_columns([
        pl.col("_full_name").map_elements(lambda s: _split_name(s)[0], return_dtype=pl.Utf8).alias("first_name"),
        pl.col("_full_name").map_elements(lambda s: _split_name(s)[1], return_dtype=pl.Utf8).alias("last_name"),
    ])

    # Normalize gender values to M/F if the sheet uses words
    df = df.with_columns([
        pl.when(pl.col("gender").str.to_uppercase().str.starts_with("M"))
          .then(pl.lit("M"))
          .when(pl.col("gender").str.to_uppercase().str.starts_with("F"))
          .then(pl.lit("F"))
          .otherwise(pl.col("gender").str.to_uppercase())
          .alias("gender")
    ])

    return df.select(["first_name", "last_name", "age", "gender", "time", "place"])


def make_results_file_from_sheet(sheet_id: str, gid: int, out_csv: Path) -> Path:
    """
    Download sheet tab as CSV, normalize, and write canonical raw-results CSV.
    """
    tmp_path = out_csv.with_suffix(".download.csv")
    download_sheet_csv(sheet_id=sheet_id, gid=gid, out_path=tmp_path)

    results_df = sheet_csv_to_results_df(tmp_path)
    results_df.write_csv(str(out_csv))

    # Keep the downloaded raw sheet CSV around only if you want it.
    # Comment out next line to preserve for debugging
    tmp_path.unlink(missing_ok=True)

    return out_csv


def _attach_gender_place(df: pl.DataFrame) -> pl.DataFrame:
    men = df.filter(pl.col("gender") == "M")
    women = df.filter(pl.col("gender") == "F")

    men = men.with_columns(pl.Series(name="gender_place", values=range(1, men.shape[0] + 1)))
    women = women.with_columns(pl.Series(name="gender_place", values=range(1, women.shape[0] + 1)))

    return pl.concat([men, women])


def get_results() -> pl.DataFrame:
    """
    Read the built CSV (created by main/make_results_file_from_sheet),
    attach gender_place, and run assimilate_raw_results().
    """
    raw = pl.read_csv(OUT_CSV)
    rr = _attach_gender_place(raw)
    return assimilate_raw_results(rr)


if __name__ == "__main__":
    make_results_file_from_sheet(sheet_id=SHEET_ID, gid=GID, out_csv=OUT_CSV)
