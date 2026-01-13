from tdfio.const import Gender
import polars as pl

class Ansi:
    RESET = "\033[0m"
    BOLD = "\033[1m"

    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"

def perform_alias_quality_check(g: Gender, events: list, load_results: callable):
    """Perform a quality check on alias names in the results.

    Args:
        g (Gender): The gender category to check.
        events (list): The list of events to check.
        load_results (callable): A function to load results for a given event and gender.
    """
    known_aliases = pl.read_csv("db/known_aliases.csv")

    issues = []

    for event in events:
        df = load_results(event, g)
        if df is None:
            continue

        hits = (
            df.join(
                known_aliases,
                how="inner",
                left_on=["first_name", "last_name"],
                right_on=["alias_fn", "alias_ln"],
            )
            .with_columns(
                pl.lit(event.to_string()).alias("event"),
                pl.lit(g.to_string()).alias("gender"),
            )
            .select(
                ["event", "gender", "first_name", "last_name", "actual_fn", "actual_ln"]
                + ([c for c in ["gender_place", "age"] if c in df.columns])
            )
        )

        if hits.height > 0:
            issues.append(hits)

    if not issues:
        print(f"{g.to_string()}: Alias quality check: no known-alias matches found.")
        return

    report = pl.concat(issues).sort(["event", "gender", "last_name", "first_name"])

    print(f"{Ansi.RED}Alias quality check: FOUND {report.height} possible match(es):{Ansi.RESET}")
    for r in report.iter_rows(named=True):
        place = f", place={r['gender_place']}" if "gender_place" in r else ""
        age = f", age={r['age']}" if "age" in r else ""
        print(
            f"{Ansi.YELLOW}"
            f"  - {r['event']} / {r['gender']}: "
            f"{r['first_name']} {r['last_name']} matches alias → prefer {r['actual_fn']} {r['actual_ln']}"
            f"{place}{age}"
            f"{Ansi.RESET}"
        )
