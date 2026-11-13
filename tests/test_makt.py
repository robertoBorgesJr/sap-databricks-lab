import random

from generator.tables.makt import SCHEMA, generate

SEED = 42
MATNRS = [str(i).zfill(18) for i in range(1, 101)]


def _records():
    return generate(len(MATNRS), random.Random(SEED), MATNRS)


# --- schema correctness ---

def test_schema_completeness():
    for r in _records():
        assert set(r.keys()) == set(SCHEMA)


def test_all_fields_are_strings():
    for r in _records():
        for col in SCHEMA:
            assert isinstance(r[col], str), f"{col} is not str: {r[col]!r}"


def test_no_none_values():
    for r in _records():
        for col in SCHEMA:
            assert r[col] is not None


# --- referential integrity ---

def test_all_matnrs_covered():
    records = _records()
    pt_matnrs = {r["MATNR"] for r in records if r["SPRAS"] == "P"}
    assert pt_matnrs == set(MATNRS), "Not all MATNRs have a PT description"


def test_no_extra_matnrs():
    records = _records()
    for r in records:
        assert r["MATNR"] in MATNRS, f"Unknown MATNR: {r['MATNR']}"


# --- SAP field contracts ---

def test_spras_values():
    valid = {"P", "E"}
    for r in _records():
        assert r["SPRAS"] in valid


def test_maktx_max_length():
    for r in _records():
        assert len(r["MAKTX"]) <= 40, f"MAKTX too long: {r['MAKTX']!r}"


def test_has_english_rows():
    assert any(r["SPRAS"] == "E" for r in _records())


def test_every_matnr_has_pt_row():
    records = _records()
    for matnr in MATNRS:
        assert any(r["MATNR"] == matnr and r["SPRAS"] == "P" for r in records)


# --- inconsistencies are present ---

def test_has_padded_maktx():
    assert any(r["MAKTX"].endswith(" ") for r in _records())


def test_has_empty_maktx():
    assert any(r["MAKTX"] == "" for r in _records())


# --- determinism ---

def test_determinism():
    r1 = generate(len(MATNRS), random.Random(SEED), MATNRS)
    r2 = generate(len(MATNRS), random.Random(SEED), MATNRS)
    assert r1 == r2


def test_different_seeds_differ():
    r1 = generate(len(MATNRS), random.Random(SEED), MATNRS)
    r2 = generate(len(MATNRS), random.Random(SEED + 1), MATNRS)
    assert r1 != r2
