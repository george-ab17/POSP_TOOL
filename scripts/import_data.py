"""Import cleaned Excel payout data into MySQL for UI/API usage.

Default datasets:
- data/extraction/twoWheeler.xlsx
- data/extraction/privatecar.xlsx
- data/extraction/pcv.xlsx
- data/extraction/misc.xlsx

Optional:
- data/extraction/gcv.xlsx (via --include-gcv)
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import mysql.connector
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.config import STATE_CODE_MAP

EXTRACTION_DIR = ROOT / "data" / "extraction"
SCHEMA_PATH = ROOT / "db" / "schema.sql"
RTO_MASTER_PATH = EXTRACTION_DIR / "district_rto"

DEFAULT_FILES = [
    EXTRACTION_DIR / "twoWheeler.xlsx",
    EXTRACTION_DIR / "privatecar.xlsx",
    EXTRACTION_DIR / "pcv.xlsx",
    EXTRACTION_DIR / "misc.xlsx",
]
GCV_FILE = EXTRACTION_DIR / "gcv.xlsx"


def _connect() -> mysql.connector.MySQLConnection:
    load_dotenv()
    db_password = os.getenv("DB_PASS") or os.getenv("DB_PASSWORD") or ""
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = int(os.getenv("DB_PORT", 3306))
    user = os.getenv("DB_USER", "root")
    db_name = os.getenv("DB_NAME", "posp_payout_db")

    try:
        return mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=db_password,
            database=db_name,
        )
    except mysql.connector.Error as exc:
        if getattr(exc, "errno", None) != 1049:
            raise

    # Database doesn't exist yet: create it with the same .env credentials.
    bootstrap = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=db_password,
    )
    cur = bootstrap.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    bootstrap.commit()
    cur.close()
    bootstrap.close()

    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=db_password,
        database=db_name,
    )


def _run_schema(conn: mysql.connector.MySQLConnection) -> None:
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    cur = conn.cursor()
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        cur.execute(stmt)
    conn.commit()
    cur.close()


def _first_non_empty_sheet(path: Path) -> Tuple[str, pd.DataFrame]:
    xl = pd.ExcelFile(path)
    for sheet in xl.sheet_names:
        df = xl.parse(sheet)
        if len(df) > 0:
            return sheet, df
    raise ValueError(f"No non-empty sheet found in {path}")


def _as_clean_str(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.lower() in {"nan", "none", "null"}:
        return None
    return s


def _to_float(value: object) -> Optional[float]:
    s = _as_clean_str(value)
    if s is None:
        return None
    try:
        n = float(s)
    except Exception:
        return None
    # Guard against fraction-style payouts (0.3381 -> 33.81)
    if 0 < n < 1:
        n = n * 100.0
    return round(n, 4)


def _to_int(value: object) -> Optional[int]:
    f = _to_float(value)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def _normalize_state_code(state_cell: Optional[str]) -> Optional[str]:
    if not state_cell:
        return None
    s = state_cell.strip()
    if "," in s or s.lower().startswith("except "):
        return None
    if re.fullmatch(r"[A-Za-z]{2,3}", s):
        return s.upper()
    return STATE_CODE_MAP.get(s)


def _normalize_rto_token(token: str) -> Optional[str]:
    t = token.strip().upper()
    if not t:
        return None
    # Remove optional state prefix: TN-01 / TN 01 / AP-31 etc.
    t = re.sub(r"^[A-Z]{1,3}\s*[- ]\s*", "", t).strip()
    t = t.replace(" ", "")
    if not t:
        return None
    # Numeric-only -> 2 digit canonical
    if re.fullmatch(r"\d+", t):
        return f"{int(t):02d}"
    # Alphanumeric codes (e.g., 15M, 83M)
    if re.fullmatch(r"[0-9A-Z]+", t):
        return t
    return None


def _split_csv_tokens(value: Optional[str]) -> List[str]:
    if not value:
        return []
    out: List[str] = []
    for token in value.split(","):
        normalized = _normalize_rto_token(token)
        if normalized:
            out.append(normalized)
    # preserve order + dedupe
    seen = set()
    ordered: List[str] = []
    for t in out:
        if t not in seen:
            ordered.append(t)
            seen.add(t)
    return ordered


@dataclass
class RtoRule:
    applies_all: bool
    include_codes: List[str]
    exclude_codes: List[str]


def _parse_rto_rule(rto_cell: Optional[str]) -> RtoRule:
    if not rto_cell:
        return RtoRule(applies_all=True, include_codes=[], exclude_codes=[])

    cell = rto_cell.strip()
    low = cell.lower()
    if low.startswith("except "):
        excluded = _split_csv_tokens(cell[7:].strip())
        return RtoRule(applies_all=True, include_codes=[], exclude_codes=excluded)

    return RtoRule(applies_all=False, include_codes=_split_csv_tokens(cell), exclude_codes=[])


def _parse_rto_master(path: Path) -> Dict[str, Dict[str, str]]:
    """Parse JS-like rtoMasterData object from data/extraction/district_rto."""
    text = path.read_text(encoding="utf-8", errors="ignore")
    marker = "const rtoMasterData ="
    start = text.find(marker)
    if start < 0:
        return {}

    # Find the first object block after marker.
    brace_start = text.find("{", start)
    if brace_start < 0:
        return {}

    depth = 0
    brace_end = -1
    for i in range(brace_start, len(text)):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                brace_end = i
                break
    if brace_end < 0:
        return {}

    obj_str = text[brace_start : brace_end + 1]
    try:
        parsed = ast.literal_eval(obj_str)
    except Exception:
        return {}

    out: Dict[str, Dict[str, str]] = {}
    if not isinstance(parsed, dict):
        return out
    for state_code, codes in parsed.items():
        if not isinstance(codes, dict):
            continue
        state = str(state_code).upper().strip()
        out[state] = {}
        for code, name in codes.items():
            normalized_code = _normalize_rto_token(str(code))
            if normalized_code:
                out[state][normalized_code] = str(name).strip()
    return out


def _seed_rto_codes(conn: mysql.connector.MySQLConnection, rto_master: Dict[str, Dict[str, str]]) -> None:
    cur = conn.cursor()
    known_codes = set()
    for state_codes in rto_master.values():
        for code, name in state_codes.items():
            if code in known_codes:
                continue
            cur.execute(
                "INSERT INTO rto (code, name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name = VALUES(name)",
                (code, name or code),
            )
            known_codes.add(code)
    conn.commit()
    cur.close()


def _get_rto_id_map(conn: mysql.connector.MySQLConnection) -> Dict[str, int]:
    cur = conn.cursor()
    cur.execute("SELECT id, code FROM rto")
    rows = cur.fetchall()
    cur.close()
    return {str(code): int(rid) for rid, code in rows}


def _ensure_rto_id(
    conn: mysql.connector.MySQLConnection, rto_cache: Dict[str, int], code: str
) -> Optional[int]:
    if code in rto_cache:
        return rto_cache[code]
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO rto (code, name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name = COALESCE(name, VALUES(name))",
        (code, code),
    )
    conn.commit()
    cur.execute("SELECT id FROM rto WHERE code = %s", (code,))
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    rto_id = int(row[0])
    rto_cache[code] = rto_id
    return rto_id


def _create_import_record(
    conn: mysql.connector.MySQLConnection, filenames: Iterable[str], uploaded_by: str
) -> int:
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO imports (filename, uploaded_by, status, notes) VALUES (%s, %s, 'pending', %s)",
        ("+".join(filenames), uploaded_by, "Excel import via import_data.py"),
    )
    import_id = int(cur.lastrowid)
    conn.commit()
    cur.close()
    return import_id


def _finish_import_record(
    conn: mysql.connector.MySQLConnection, import_id: int, row_count: int, status: str, notes: str
) -> None:
    cur = conn.cursor()
    cur.execute(
        "UPDATE imports SET row_count=%s, status=%s, notes=%s WHERE id=%s",
        (row_count, status, notes, import_id),
    )
    conn.commit()
    cur.close()


def _reset_data(conn: mysql.connector.MySQLConnection) -> None:
    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    cur.execute("TRUNCATE TABLE rate_excluded_rto")
    cur.execute("TRUNCATE TABLE rate_included_rto")
    cur.execute("TRUNCATE TABLE rates")
    cur.execute("TRUNCATE TABLE imports")
    cur.execute("TRUNCATE TABLE rto")
    cur.execute("SET FOREIGN_KEY_CHECKS=1")
    conn.commit()
    cur.close()


def _build_raw_json_row(df_row: pd.Series) -> Dict[str, object]:
    raw: Dict[str, object] = {}
    for col in df_row.index:
        value = df_row[col]
        cleaned = _as_clean_str(value)
        raw[str(col)] = cleaned if cleaned is not None else None
    return raw


def _insert_rates_from_file(
    conn: mysql.connector.MySQLConnection,
    import_id: int,
    path: Path,
    rto_cache: Dict[str, int],
) -> int:
    sheet, df = _first_non_empty_sheet(path)
    print(f"[IMPORT] {path.name} -> sheet={sheet}, rows={len(df)}")

    cur = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        raw_json = _build_raw_json_row(row)
        company = _as_clean_str(row.get("Company"))
        final_payout = _to_float(row.get("Final Payout"))
        if company is None or final_payout is None:
            # Mandatory fields for a valid payout row.
            continue

        state_cell = _as_clean_str(row.get("State"))
        state_code = _normalize_state_code(state_cell)
        condition_text = _as_clean_str(row.get("Conditions"))
        age_min = _to_int(row.get("Vehicle_Age_Min"))
        age_max = _to_int(row.get("Vehicle_Age_Max"))
        gvw_min = _to_float(row.get("GVW_Min"))
        gvw_max = _to_float(row.get("GVW_Max"))

        rto_cell = _as_clean_str(row.get("RTO_Code"))
        rto_rule = _parse_rto_rule(rto_cell)

        cur.execute(
            """
            INSERT INTO rates
              (import_id, state_code, company, condition_text, final_payout,
               age_min, age_max, gvw_min, gvw_max, applies_all_rto, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                import_id,
                state_code,
                company,
                condition_text,
                final_payout,
                age_min,
                age_max,
                gvw_min,
                gvw_max,
                1 if rto_rule.applies_all else 0,
                json.dumps(raw_json, ensure_ascii=True),
            ),
        )
        rate_id = int(cur.lastrowid)

        # Included RTO codes
        for code in rto_rule.include_codes:
            rto_id = _ensure_rto_id(conn, rto_cache, code)
            if rto_id is None:
                continue
            cur.execute(
                "INSERT IGNORE INTO rate_included_rto (rate_id, rto_id) VALUES (%s, %s)",
                (rate_id, rto_id),
            )

        # Excluded RTO codes (for applies_all_rto rows)
        for code in rto_rule.exclude_codes:
            rto_id = _ensure_rto_id(conn, rto_cache, code)
            if rto_id is None:
                continue
            cur.execute(
                "INSERT IGNORE INTO rate_excluded_rto (rate_id, rto_id) VALUES (%s, %s)",
                (rate_id, rto_id),
            )

        inserted += 1

    conn.commit()
    cur.close()
    return inserted


def import_excels(include_gcv: bool = False, replace_existing: bool = True) -> None:
    conn = _connect()
    try:
        _run_schema(conn)
        if replace_existing:
            _reset_data(conn)

        rto_master = _parse_rto_master(RTO_MASTER_PATH)
        _seed_rto_codes(conn, rto_master)
        rto_cache = _get_rto_id_map(conn)

        files = list(DEFAULT_FILES)
        if include_gcv:
            files.append(GCV_FILE)

        import_id = _create_import_record(conn, [p.name for p in files], uploaded_by="codex")
        total_rows = 0
        try:
            for file_path in files:
                total_rows += _insert_rates_from_file(conn, import_id, file_path, rto_cache)
            _finish_import_record(conn, import_id, total_rows, "completed", "Import completed")
            print(f"[IMPORT] Completed. import_id={import_id}, rows={total_rows}")
        except Exception as exc:
            _finish_import_record(conn, import_id, total_rows, "failed", f"Import failed: {exc}")
            raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Import POSP payout Excel files into MySQL")
    parser.add_argument(
        "--include-gcv",
        action="store_true",
        help="Include data/extraction/gcv.xlsx as part of this import",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append new import instead of replacing existing imported rows",
    )
    args = parser.parse_args()

    import_excels(include_gcv=args.include_gcv, replace_existing=not args.append)


if __name__ == "__main__":
    main()
