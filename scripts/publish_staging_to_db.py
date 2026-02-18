"""Publish confirmed staging files to extraction and import into DB.

Behavior:
- For each category file in data/staging:
  - If it has rows, replace the corresponding extraction file with it (backup kept).
- Run import_data in append mode with payout-update logic:
  - existing row -> update payout
  - missing row -> insert new row
"""

from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
import pandas as pd

from import_data import import_excels


ROOT = Path(__file__).resolve().parents[1]
EXTRACTION_DIR = ROOT / "data" / "extraction"
STAGING_DIR = ROOT / "data" / "staging"
BACKUP_DIR = EXTRACTION_DIR / "backups"

FILES = [
    "twoWheeler.xlsx",
    "privatecar.xlsx",
    "pcv.xlsx",
    "misc.xlsx",
    "gcv.xlsx",
]


def _first_sheet(path: Path) -> pd.DataFrame:
    xl = pd.ExcelFile(path)
    return xl.parse(xl.sheet_names[0])


def _headers(path: Path) -> list[str]:
    return list(_first_sheet(path).columns)


def main() -> None:
    if not STAGING_DIR.exists():
        raise SystemExit("data/staging not found. Run scripts/init_staging_templates.py first.")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / stamp
    backup_path.mkdir(parents=True, exist_ok=True)

    replaced = []
    skipped = []

    for name in FILES:
        staged = STAGING_DIR / name
        target = EXTRACTION_DIR / name
        if not staged.exists():
            skipped.append((name, "staging file missing"))
            continue

        sdf = _first_sheet(staged)
        if len(sdf) == 0:
            skipped.append((name, "no staging rows"))
            continue

        sh = _headers(staged)
        th = _headers(target)
        if sh != th:
            raise SystemExit(
                f"Header mismatch for {name}\n"
                f"staging: {sh}\n"
                f"target : {th}"
            )

        shutil.copy2(target, backup_path / name)
        shutil.copy2(staged, target)
        replaced.append((name, len(sdf)))

    if not replaced:
        print("[PUBLISH] Nothing to publish.")
        for item in skipped:
            print(f"  - {item[0]}: {item[1]}")
        return

    for name, count in replaced:
        print(f"[PUBLISH] replaced {name} with {count} staged row(s)")

    print("[PUBLISH] Importing to DB (append + update existing payouts + insert missing rows)...")
    import_excels(
        include_gcv=True,
        replace_existing=False,
        update_existing_payouts=True,
        update_only=False,
    )
    print(f"[PUBLISH] Done. backups: {backup_path}")


if __name__ == "__main__":
    main()

