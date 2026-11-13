import random

from generator.tables.mara import SCHEMA, generate

N = 200
SEED = 42


def _records():
    return generate(N, random.Random(SEED))


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
            assert r[col] is not None, f"{col} is None in {r}"


# --- SAP field contracts ---

def test_matnr_max_length():
    for r in _records():
        assert len(r["MATNR"].rstrip()) <= 18, f"MATNR too long: {r['MATNR']!r}"


def test_ersda_length_and_digits_or_zeros():
    for r in _records():
        assert len(r["ERSDA"]) == 8, f"ERSDA wrong length: {r['ERSDA']!r}"
        assert r["ERSDA"].isdigit(), f"ERSDA not digits: {r['ERSDA']!r}"


def test_lvorm_valid_values():
    valid = {"X", "", " "}
    for r in _records():
        assert r["LVORM"] in valid, f"Unexpected LVORM: {r['LVORM']!r}"


def test_mandt_distribution():
    records = _records()
    main = sum(1 for r in records if r["MANDT"] == "100")
    alt = sum(1 for r in records if r["MANDT"] == "200")
    assert main > N * 0.80
    assert alt > 0


def test_mtart_valid():
    valid = {"FERT", "ROH", "HALB", "HAWA"}
    for r in _records():
        assert r["MTART"] in valid


# --- inconsistencies are present ---

def test_has_erdat_zero():
    assert any(r["ERSDA"] == "00000000" for r in _records())


def test_has_lvorm_space_trap():
    assert any(r["LVORM"] == " " for r in _records())


def test_has_unpadded_numeric_matnr():
    # migration artifact: stripped leading zeros
    records = _records()
    numeric_unpadded = [
        r for r in records
        if r["MATNR"].strip().isdigit() and len(r["MATNR"].strip()) < 18
        and not r["MATNR"].startswith(" ")
    ]
    assert len(numeric_unpadded) > 0, "No unpadded numeric MATNR found"


# --- determinism ---

def test_determinism():
    r1 = generate(50, random.Random(SEED))
    r2 = generate(50, random.Random(SEED))
    assert r1 == r2


def test_different_seeds_differ():
    r1 = generate(50, random.Random(SEED))
    r2 = generate(50, random.Random(SEED + 1))
    assert r1 != r2
