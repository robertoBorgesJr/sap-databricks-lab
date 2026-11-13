"""
Tests for the CDC simulator.

The tests use an in-memory CDCState built from small full-load data so they
run without touching disk. File I/O is tested in test_cdc_integration.
"""

import random
from collections import defaultdict

from generator.cdc import (
    CDCState,
    _KEYS,
    _SCHEMA,
    apply_batch,
    generate_batch,
)
from generator.tables.kna1 import generate as generate_kna1
from generator.tables.mara import generate as generate_mara
from generator.tables.makt import generate as generate_makt
from generator.tables.vbak import generate as generate_vbak
from generator.tables.vbap import generate as generate_vbap
from generator.tables.bkpf import generate as generate_bkpf
from generator.tables.bseg import generate as generate_bseg
from generator.tables.tcurr import generate as generate_tcurr

SEED = 42
BATCH_N = 1


def _build_state(seed: int = SEED) -> CDCState:
    rng = random.Random(seed)
    kna1 = generate_kna1(50, rng)
    mara = generate_mara(40, rng)
    kunnrs = [r["KUNNR"] for r in kna1]
    matnrs = [r["MATNR"] for r in mara]
    makt = generate_makt(len(matnrs), rng, matnrs)
    vbak = generate_vbak(80, rng, kunnrs)
    vbap = generate_vbap(rng, vbak, matnrs)
    bkpf = generate_bkpf(30, rng)
    bseg = generate_bseg(rng, bkpf)
    tcurr = generate_tcurr(rng)
    return CDCState(
        tables={
            "KNA1": kna1, "MARA": mara, "MAKT": makt,
            "VBAK": vbak, "VBAP": vbap,
            "BKPF": bkpf, "BSEG": bseg, "TCURR": tcurr,
        },
        last_seq=0,
        last_batch=0,
    )


def _batch_for(table: str, seed: int = SEED, batch_n: int = BATCH_N) -> tuple[list[dict], CDCState]:
    state = _build_state(seed)
    rng = random.Random(seed + batch_n)
    batch = generate_batch(table, state, batch_n, rng)
    return batch, state


# ---------------------------------------------------------------------------
# Schema and types
# ---------------------------------------------------------------------------

def test_batch_has_cdc_columns():
    batch, _ = _batch_for("KNA1")
    for r in batch:
        assert "_op" in r
        assert "_seq" in r
        assert "_extracted_at" in r


def test_business_columns_still_strings():
    for table in _SCHEMA:
        batch, _ = _batch_for(table)
        for r in batch:
            for col in _SCHEMA[table]:
                assert isinstance(r[col], str), f"{table}.{col} not str"


def test_op_values():
    for table in _SCHEMA:
        batch, _ = _batch_for(table)
        for r in batch:
            assert r["_op"] in ("I", "U", "D"), f"Unknown op: {r['_op']}"


# ---------------------------------------------------------------------------
# _seq monotonicity
# ---------------------------------------------------------------------------

def test_seq_monotonic_within_batch():
    """_seq values in a batch must all be distinct and positive integers."""
    batch, _ = _batch_for("KNA1")
    seqs = [int(r["_seq"]) for r in batch]
    assert len(seqs) == len(set(seqs)), "Duplicate _seq in batch"
    assert all(s > 0 for s in seqs)


def test_seq_increases_across_batches():
    """Every _seq in batch 2 must be greater than every _seq in batch 1."""
    state = _build_state()

    rng1 = random.Random(SEED + 1)
    batch1 = generate_batch("KNA1", state, 1, rng1)
    apply_batch("KNA1", state, batch1)
    max_seq_b1 = max(int(r["_seq"]) for r in batch1)

    rng2 = random.Random(SEED + 2)
    batch2 = generate_batch("KNA1", state, 2, rng2)
    min_seq_b2 = min(int(r["_seq"]) for r in batch2)

    assert min_seq_b2 > max_seq_b1, (
        f"Batch-2 min _seq ({min_seq_b2}) not greater than batch-1 max ({max_seq_b1})"
    )


# ---------------------------------------------------------------------------
# All ops present
# ---------------------------------------------------------------------------

def test_batch_contains_all_op_types():
    """A generated batch must contain at least one I, one U, and one D."""
    batch, _ = _batch_for("KNA1")
    ops = {r["_op"] for r in batch}
    assert "I" in ops, "No inserts in batch"
    assert "U" in ops, "No updates in batch"
    assert "D" in ops, "No deletes in batch"


# ---------------------------------------------------------------------------
# Mandatory scenario: multi-update same key
# ---------------------------------------------------------------------------

def test_multi_update_same_key():
    """At least one key must appear with two U records in the same batch."""
    batch, _ = _batch_for("KNA1")
    key_cols = _KEYS["KNA1"]
    u_keys = [tuple(r[k] for k in key_cols) for r in batch if r["_op"] == "U"]
    counts = defaultdict(int)
    for k in u_keys:
        counts[k] += 1
    assert any(v >= 2 for v in counts.values()), "No key with multiple U records found"


# ---------------------------------------------------------------------------
# Mandatory scenario: insert then delete in same batch
# ---------------------------------------------------------------------------

def test_insert_delete_same_batch():
    """A key appearing as both I and D in the same batch must be present."""
    batch, _ = _batch_for("KNA1")
    key_cols = _KEYS["KNA1"]
    insert_keys = {tuple(r[k] for k in key_cols) for r in batch if r["_op"] == "I"}
    delete_keys = {tuple(r[k] for k in key_cols) for r in batch if r["_op"] == "D"}
    ephemeral = insert_keys & delete_keys
    assert len(ephemeral) > 0, "No insert+delete pair found in batch"


# ---------------------------------------------------------------------------
# Mandatory scenario: logical delete via flag
# ---------------------------------------------------------------------------

def test_logical_delete_flag():
    """A U record setting LOEVM='X' must be present in KNA1 batch."""
    batch, _ = _batch_for("KNA1")
    logical_deletes = [
        r for r in batch
        if r["_op"] == "U" and r.get("LOEVM", "").strip() == "X"
    ]
    assert len(logical_deletes) > 0, "No logical delete (LOEVM='X') found"


def test_logical_delete_mara():
    """MARA must have a U record setting LVORM='X'."""
    batch, _ = _batch_for("MARA")
    logical_deletes = [
        r for r in batch
        if r["_op"] == "U" and r.get("LVORM", "").strip() == "X"
    ]
    assert len(logical_deletes) > 0, "No logical delete (LVORM='X') found in MARA batch"


# ---------------------------------------------------------------------------
# apply_batch: idempotency
# ---------------------------------------------------------------------------

def test_apply_batch_idempotent():
    """Applying the same batch twice must produce the same state as applying it once."""
    state1 = _build_state()
    state2 = _build_state()

    rng = random.Random(SEED + 1)
    batch = generate_batch("KNA1", state1, 1, rng)

    apply_batch("KNA1", state1, batch)

    rng2 = random.Random(SEED + 1)
    batch2 = generate_batch("KNA1", state2, 1, rng2)
    apply_batch("KNA1", state2, batch2)
    apply_batch("KNA1", state2, batch2)  # second application

    keys1 = {tuple(r[k] for k in _KEYS["KNA1"]) for r in state1.tables["KNA1"]}
    keys2 = {tuple(r[k] for k in _KEYS["KNA1"]) for r in state2.tables["KNA1"]}
    assert keys1 == keys2, "Idempotency violated: second apply changed the state"


# ---------------------------------------------------------------------------
# apply_batch: D removes from state; I+D ephemeral key absent after apply
# ---------------------------------------------------------------------------

def test_deleted_records_removed_from_state():
    state = _build_state()
    rng = random.Random(SEED + 1)
    batch = generate_batch("KNA1", state, 1, rng)

    deleted_keys = {
        tuple(r[k] for k in _KEYS["KNA1"])
        for r in batch if r["_op"] == "D"
    }
    apply_batch("KNA1", state, batch)

    remaining_keys = {tuple(r[k] for k in _KEYS["KNA1"]) for r in state.tables["KNA1"]}
    # ephemeral I+D records should not be in remaining state either
    for key in deleted_keys:
        # only check keys that were NOT also inserted in same batch (ephemeral)
        insert_keys = {
            tuple(r[k] for k in _KEYS["KNA1"])
            for r in batch if r["_op"] == "I"
        }
        if key not in insert_keys:
            assert key not in remaining_keys, f"Deleted key still in state: {key}"


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

def test_determinism():
    state1 = _build_state(SEED)
    rng1 = random.Random(SEED + 1)
    batch1 = generate_batch("KNA1", state1, 1, rng1)

    state2 = _build_state(SEED)
    rng2 = random.Random(SEED + 1)
    batch2 = generate_batch("KNA1", state2, 1, rng2)

    assert batch1 == batch2


# ---------------------------------------------------------------------------
# All tables generate without error
# ---------------------------------------------------------------------------

def test_all_tables_generate():
    state = _build_state()
    for table in _SCHEMA:
        rng = random.Random(SEED + 1)
        batch = generate_batch(table, state, 1, rng)
        assert len(batch) > 0, f"Empty batch for {table}"
        apply_batch(table, state, batch)


# ---------------------------------------------------------------------------
# _mutate must not corrupt numeric fields with _UPD when value is empty string
# ---------------------------------------------------------------------------

_NUMERIC_COLS = {
    "VBAK": ["NETWR"],
    "MARA": ["BRGEW", "NTGEW"],
    "VBAP": ["NETWR", "MENGE"],
    "BSEG": ["DMBTR"],
    "TCURR": ["UKURS"],
}


def test_mutate_never_produces_upd_in_numeric_fields():
    """
    Regression for: _mutate(empty_string) → '_UPD' broke silver CAST to DECIMAL.
    Numeric columns must always contain either '' or a parseable float string.
    """
    import re
    _numeric_re = re.compile(r"^-?\d+([.,]\d+)?$")

    for table, cols in _NUMERIC_COLS.items():
        state = _build_state()
        # run several batches to exercise different mutation paths
        for batch_n in range(1, 6):
            rng = random.Random(SEED + batch_n)
            batch = generate_batch(table, state, batch_n, rng)
            apply_batch(table, state, batch)
            for r in batch:
                if r["_op"] in ("U", "I"):
                    for col in cols:
                        val = r.get(col, "")
                        assert val == "" or _numeric_re.match(val), (
                            f"{table}.{col} has non-numeric value {val!r} in batch {batch_n}"
                        )
