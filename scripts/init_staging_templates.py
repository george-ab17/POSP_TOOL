"""Create per-category staging Excel templates from current extraction headers."""

from __future__ import annotations

from pathlib import Path
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
EXTRACTION_DIR = ROOT / "data" / "extraction"
STAGING_DIR = ROOT / "data" / "staging"

FILES = [
    "twoWheeler.xlsx",
    "privatecar.xlsx",
    "pcv.xlsx",
    "misc.xlsx",
    "gcv.xlsx",
]


def _first_non_empty_sheet(path: Path) -> pd.DataFrame:
    xl = pd.ExcelFile(path)
    for sheet in xl.sheet_names:
        df = xl.parse(sheet)
        if len(df) > 0:
            return df
    raise ValueError(f"No non-empty sheet found in {path}")


def main() -> None:
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    for name in FILES:
        src = EXTRACTION_DIR / name
        dst = STAGING_DIR / name
        df = _first_non_empty_sheet(src)
        template = pd.DataFrame(columns=list(df.columns))
        with pd.ExcelWriter(dst, engine="openpyxl") as writer:
            template.to_excel(writer, index=False, sheet_name="staging_rows")
        print(f"[STAGING] template created: {dst}")


if __name__ == "__main__":
    main()

