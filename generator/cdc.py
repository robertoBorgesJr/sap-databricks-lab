"""
CDC simulator — generates delta batches that mimic ODP extracts.

Each record in a delta file carries three technical columns:
  _op            : 'I' (insert), 'U' (update), 'D' (delete)
  _seq           : globally unique, monotonically increasing integer across batches
  _extracted_at  : ISO-8601 timestamp string, deterministic per seed+batch

These column names are a lab simplification; real ODP field names depend on
the extraction tool (e.g., SAP SLT, AIF, or third-party connectors).

Mandatory CDC scenarios injected per batch
------------------------------------------
  multi_update    : same key appears with two 'U' records (different _seq)
  insert_delete   : new key inserted then immediately deleted within same batch
  logical_delete  : flag column (LOEVM / LVORM) flipped to 'X' via a 'U' record
  duplicate_file  : same part file copied to part-00001 (opt-in via CLI flag)

Out-of-order delivery is not injected automatically; it is exercised by running
cmd_delta with explicit --batch numbers in a non-sequential order.
"""

from __future__ import annotations

import json
import pathlib
import random
import shutil
from datetime import datetime, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from generator.tables.bkpf import SCHEMA as BKPF_SCHEMA
from generator.tables.bseg import SCHEMA as BSEG_SCHEMA
from generator.tables.kna1 import SCHEMA as KNA1_SCHEMA
from generator.tables.makt import SCHEMA as MAKT_SCHEMA
from generator.tables.mara import SCHEMA as MARA_SCHEMA
from generator.tables.tcurr import SCHEMA as TCURR_SCHEMA
from generator.tables.vbak import SCHEMA as VBAK_SCHEMA
from generator.tables.vbap import SCHEMA as VBAP_SCHEMA

# ---------------------------------------------------------------------------
# Schema registry
# ---------------------------------------------------------------------------

_SCHEMA: dict[str, list[str]] = {
    "KNA1": KNA1_SCHEMA,
    "MARA": MARA_SCHEMA,
    "MAKT": MAKT_SCHEMA,
    "VBAK": VBAK_SCHEMA,
    "VBAP": VBAP_SCHEMA,
    "BKPF": BKPF_SCHEMA,
    "BSEG": BSEG_SCHEMA,
    "TCURR": TCURR_SCHEMA,
}

# Primary key columns per table (used to identify records for U/D ops)
_KEYS: dict[str, list[str]] = {
    "KNA1":  ["MANDT", "KUNNR"],
    "MARA":  ["MANDT", "MATNR"],
    "MAKT":  ["MANDT", "MATNR", "SPRAS"],
    "VBAK":  ["MANDT", "VBELN"],
    "VBAP":  ["MANDT", "VBELN", "POSNR"],
    "BKPF":  ["MANDT", "BUKRS", "BELNR", "GJAHR"],
    "BSEG":  ["MANDT", "BUKRS", "BELNR", "GJAHR", "BUZEI"],
    "TCURR": ["MANDT", "KURST", "FCURR", "TCURR", "GDATU"],
}

# Logical-delete flag columns per table (empty string = not deleted, 'X' = deleted)
_LOGICAL_DELETE_FLAG: dict[str, str | None] = {
    "KNA1": "LOEVM",
    "MARA": "LVORM",
    "MAKT": None,
    "VBAK": None,
    "VBAP": None,
    "BKPF": None,
    "BSEG": None,
    "TCURR": None,
}

# Mutable non-key columns per table (safe to overwrite in an update)
_MUTABLE_COLS: dict[str, list[str]] = {
    "KNA1":  ["NAME1", "NAME2", "ORT01", "PSTLZ", "TELF1", "LOEVM"],
    "MARA":  ["BRGEW", "NTGEW", "LVORM", "MATKL"],
    "MAKT":  ["MAKTX"],
    "VBAK":  ["NETWR", "BSTNK"],
    "VBAP":  ["MENGE", "NETWR", "ABGRU"],
    "BKPF":  ["BKTXT", "BSTAT"],
    "BSEG":  ["KOSTL", "MWSKZ"],
    "TCURR": ["UKURS"],
}


# ---------------------------------------------------------------------------
# State I/O
# ---------------------------------------------------------------------------

class CDCState:
    def __init__(
        self,
        tables: dict[str, list[dict]],
        last_seq: int,
        last_batch: int,
    ) -> None:
        self.tables = tables          # table_name → list of current records
        self.last_seq = last_seq
        self.last_batch = last_batch


def load_state(out_dir: pathlib.Path) -> CDCState:
    state_dir = out_dir / "_state"
    meta = json.loads((state_dir / "meta.json").read_text())
    tables: dict[str, list[dict]] = {}
    for table_name, cols in _SCHEMA.items():
        path = state_dir / f"{table_name}.parquet"
        if path.exists():
            tbl = pq.read_table(path)
            tables[table_name] = [
                {col: str(row[col]) for col in cols}
                for row in tbl.to_pylist()
            ]
        else:
            tables[table_name] = []
    return CDCState(tables, meta["last_seq"], meta["last_batch"])


def save_state(state: CDCState, out_dir: pathlib.Path) -> None:
    state_dir = out_dir / "_state"
    state_dir.mkdir(parents=True, exist_ok=True)
    for table_name, records in state.tables.items():
        if not records:
            continue
        cols = _SCHEMA[table_name]
        tbl = pa.table({col: [r[col] for r in records] for col in cols})
        pq.write_table(tbl, state_dir / f"{table_name}.parquet")
    meta = {"last_seq": state.last_seq, "last_batch": state.last_batch}
    (state_dir / "meta.json").write_text(json.dumps(meta, indent=2))


# ---------------------------------------------------------------------------
# Batch generation
# ---------------------------------------------------------------------------

def _next_seq(state: CDCState) -> int:
    state.last_seq += 1
    return state.last_seq


def _extracted_at(batch_n: int, record_index: int, seed: int) -> str:
    base = datetime(2024, 1, 1, 8, 0, 0)
    offset = timedelta(days=(seed % 365), seconds=batch_n * 3600 + record_index)
    return (base + offset).strftime("%Y-%m-%dT%H:%M:%S")


def _record_key(record: dict, key_cols: list[str]) -> tuple:
    return tuple(record[k] for k in key_cols)


def _is_numeric_str(s: str) -> bool:
    """True if s can be parsed as float (handles comma decimal and negative values)."""
    try:
        float(s.replace(",", "."))
        return True
    except ValueError:
        return False


def _mutate(record: dict, table_name: str, rng: random.Random) -> dict:
    """Return a shallow copy of record with one mutable field changed."""
    r = dict(record)
    cols = _MUTABLE_COLS.get(table_name, [])
    if not cols:
        return r
    col = rng.choice(cols)
    old_val = r[col]
    if col in ("LOEVM", "LVORM"):
        r[col] = "X" if old_val == "" else ""
    elif old_val == "":
        pass  # skip — empty numeric fields must stay parseable; '' + '_UPD' → invalid DECIMAL
    elif _is_numeric_str(old_val):
        r[col] = str(round(float(old_val.replace(",", ".")) * rng.uniform(0.9, 1.1), 2))
    else:
        r[col] = (old_val.rstrip() + "_UPD")[:len(old_val) + 4]
    return r


def generate_batch(
    table_name: str,
    state: CDCState,
    batch_n: int,
    rng: random.Random,
    insert_rate: float = 0.10,
    update_rate: float = 0.20,
    delete_rate: float = 0.05,
) -> list[dict]:
    """
    Generate a delta batch for one table. Each returned record has the business
    columns plus _op, _seq, _extracted_at.
    Does NOT modify state — apply_batch does that.
    """
    current = state.tables.get(table_name, [])
    key_cols = _KEYS[table_name]
    batch: list[dict] = []
    record_idx = 0

    def _tagged(r: dict, op: str) -> dict:
        nonlocal record_idx
        out = dict(r)
        out["_op"] = op
        out["_seq"] = str(_next_seq(state))
        out["_extracted_at"] = _extracted_at(batch_n, record_idx, rng.randint(0, 9999))
        record_idx += 1
        return out

    # -- deletes --
    n_del = max(0, int(len(current) * delete_rate))
    to_delete = rng.sample(current, min(n_del, len(current)))
    for rec in to_delete:
        batch.append(_tagged(rec, "D"))

    # -- updates --
    n_upd = max(0, int(len(current) * update_rate))
    survivors = [r for r in current if r not in to_delete]
    to_update = rng.sample(survivors, min(n_upd, len(survivors)))
    for rec in to_update:
        batch.append(_tagged(_mutate(rec, table_name, rng), "U"))

    # -- inserts --
    n_ins = max(1, int(len(current) * insert_rate))
    for _ in range(n_ins):
        new_rec = _build_insert(table_name, state, rng)
        if new_rec:
            batch.append(_tagged(new_rec, "I"))

    # -- mandatory scenario injections --
    _inject_multi_update(batch, table_name, key_cols, survivors, state, rng)
    _inject_insert_delete(batch, table_name, key_cols, state, rng)
    _inject_logical_delete(batch, table_name, state, rng)

    return batch


def apply_batch(table_name: str, state: CDCState, batch: list[dict]) -> None:
    """Apply a batch to state.tables in _seq order (idempotent for repeated calls)."""
    key_cols = _KEYS[table_name]
    business_cols = _SCHEMA[table_name]

    # sort by _seq to handle within-batch ordering
    sorted_batch = sorted(batch, key=lambda r: int(r["_seq"]))

    current_map: dict[tuple, dict] = {
        _record_key(r, key_cols): r
        for r in state.tables.get(table_name, [])
    }

    for record in sorted_batch:
        key = _record_key(record, key_cols)
        op = record["_op"]
        business_record = {col: record[col] for col in business_cols}
        if op in ("I", "U"):
            current_map[key] = business_record
        elif op == "D":
            current_map.pop(key, None)

    state.tables[table_name] = list(current_map.values())


# ---------------------------------------------------------------------------
# Insert builder (new records for each table)
# ---------------------------------------------------------------------------

def _build_insert(
    table_name: str, state: CDCState, rng: random.Random
) -> dict | None:
    """Build a plausible new record for the table using existing key pools."""
    existing = state.tables.get(table_name, [])
    n_existing = len(existing)
    new_id = str(n_existing + rng.randint(1000, 9999))

    if table_name == "KNA1":
        from generator.tables.kna1 import generate as gen
        recs = gen(1, rng)
        recs[0]["KUNNR"] = new_id.zfill(10)
        return recs[0]
    if table_name == "MARA":
        from generator.tables.mara import generate as gen
        recs = gen(1, rng)
        recs[0]["MATNR"] = new_id.zfill(18)
        return recs[0]
    if table_name == "MAKT":
        mara = state.tables.get("MARA", [])
        if not mara:
            return None
        matnr = rng.choice(mara)["MATNR"]
        from generator.tables.makt import generate as gen
        recs = gen(1, rng, [matnr])
        return recs[0] if recs else None
    if table_name == "VBAK":
        kna1 = state.tables.get("KNA1", [])
        if not kna1:
            return None
        kunnrs = [r["KUNNR"] for r in kna1]
        from generator.tables.vbak import generate as gen
        recs = gen(1, rng, kunnrs)
        recs[0]["VBELN"] = new_id.zfill(10)
        return recs[0]
    if table_name == "VBAP":
        vbak = state.tables.get("VBAK", [])
        mara = state.tables.get("MARA", [])
        if not vbak or not mara:
            return None
        order = rng.choice(vbak)
        matnrs = [r["MATNR"] for r in mara]
        from generator.tables.vbap import generate as gen
        recs = gen(rng, [order], matnrs, min_items=1, max_items=1)
        return recs[0] if recs else None
    if table_name == "BKPF":
        from generator.tables.bkpf import generate as gen
        recs = gen(1, rng)
        recs[0]["BELNR"] = new_id.zfill(10)
        return recs[0]
    if table_name == "BSEG":
        bkpf = state.tables.get("BKPF", [])
        if not bkpf:
            return None
        doc = rng.choice(bkpf)
        # single balanced line pair — pick a debit only (simpler for inserts)
        from decimal import Decimal
        amount = str(round(rng.uniform(10.0, 1000.0), 2))
        buzei_nums = [
            int(r["BUZEI"]) for r in state.tables.get("BSEG", [])
            if r["BELNR"] == doc["BELNR"] and r["GJAHR"] == doc["GJAHR"]
        ]
        next_buzei = str((max(buzei_nums) + 1) if buzei_nums else 1).zfill(3)
        return {
            "MANDT": doc["MANDT"], "BUKRS": doc["BUKRS"],
            "BELNR": doc["BELNR"], "GJAHR": doc["GJAHR"],
            "BUZEI": next_buzei, "SHKZG": "S",
            "DMBTR": amount, "HKONT": "0001100000",
            "KOSTL": "", "MWSKZ": "",
        }
    if table_name == "TCURR":
        from generator.tables.tcurr import generate as gen, _invert_date
        from datetime import date
        new_date = date(rng.randint(2015, 2024), rng.randint(1, 12), 1)
        pairs = [("USD", "BRL"), ("EUR", "BRL"), ("USD", "EUR")]
        fcurr, tcurr = rng.choice(pairs)
        return {
            "MANDT": "100",
            "KURST": rng.choice(["M", "B", "G"]),
            "FCURR": fcurr,
            "TCURR": tcurr,
            "GDATU": _invert_date(new_date),
            "UKURS": f"{rng.uniform(4.5, 6.0):.5f}",
            "FFACT": "1",
            "TFACT": "1",
        }
    return None


# ---------------------------------------------------------------------------
# Mandatory scenario injectors
# ---------------------------------------------------------------------------

def _inject_multi_update(
    batch: list[dict],
    table_name: str,
    key_cols: list[str],
    survivors: list[dict],
    state: CDCState,
    rng: random.Random,
) -> None:
    """Add a second 'U' for a key already updated in this batch."""
    u_records = [r for r in batch if r["_op"] == "U"]
    if not u_records or not survivors:
        return
    target = rng.choice(u_records)
    mutated = _mutate({k: target[k] for k in _SCHEMA[table_name]}, table_name, rng)
    mutated["_op"] = "U"
    mutated["_seq"] = str(_next_seq(state))
    mutated["_extracted_at"] = target["_extracted_at"]
    batch.append(mutated)


def _inject_insert_delete(
    batch: list[dict],
    table_name: str,
    key_cols: list[str],
    state: CDCState,
    rng: random.Random,
) -> None:
    """
    Insert a new record and immediately delete it in the same batch.
    The key must NOT enter the persistent state (apply_batch handles this).
    """
    new_rec = _build_insert(table_name, state, rng)
    if new_rec is None:
        return
    # give this ephemeral record a key that won't collide with existing state
    for col in key_cols:
        if col not in ("MANDT", "BUKRS", "GJAHR"):
            new_rec[col] = "EPHEMERAL_" + new_rec.get(col, "0")[:5]

    seq_i = str(_next_seq(state))
    seq_d = str(_next_seq(state))
    ts = "2024-06-15T12:00:00"
    insert = {**new_rec, "_op": "I", "_seq": seq_i, "_extracted_at": ts}
    delete = {**new_rec, "_op": "D", "_seq": seq_d, "_extracted_at": ts}
    batch.extend([insert, delete])


def _inject_logical_delete(
    batch: list[dict],
    table_name: str,
    state: CDCState,
    rng: random.Random,
) -> None:
    """
    For tables with a logical-delete flag (LOEVM/LVORM), add a 'U' record
    that sets the flag to 'X' — the record stays in Silver but is marked deleted.
    """
    flag_col = _LOGICAL_DELETE_FLAG.get(table_name)
    if flag_col is None:
        return
    current = state.tables.get(table_name, [])
    # pick a record where the flag is currently '' (not yet logically deleted)
    candidates = [r for r in current if r.get(flag_col, "") == ""]
    if not candidates:
        return
    target = rng.choice(candidates)
    mutated = dict(target)
    mutated[flag_col] = "X"
    mutated["_op"] = "U"
    mutated["_seq"] = str(_next_seq(state))
    mutated["_extracted_at"] = "2024-06-15T13:00:00"
    batch.append(mutated)


# ---------------------------------------------------------------------------
# File I/O for a batch
# ---------------------------------------------------------------------------

def write_batch(
    batch: list[dict],
    table_name: str,
    out_dir: pathlib.Path,
    batch_n: int,
    duplicate_file: bool = False,
) -> pathlib.Path:
    """Write batch records to <TABLE>/batch=<N>/part-00000.parquet."""
    business_cols = _SCHEMA[table_name]
    cdc_cols = ["_op", "_seq", "_extracted_at"]
    all_cols = business_cols + cdc_cols

    dest = out_dir / table_name / f"batch={batch_n}"
    dest.mkdir(parents=True, exist_ok=True)

    tbl = pa.table({col: [r[col] for r in batch] for col in all_cols})
    out_path = dest / "part-00000.parquet"
    pq.write_table(tbl, out_path)

    if duplicate_file:
        dup_path = dest / "part-00001.parquet"
        shutil.copy2(out_path, dup_path)

    return out_path
