import random

import pytest

from generator.tables.kna1 import SCHEMA, generate

N = 200
SEED = 42


@pytest.fixture(scope="module")
def records():
    return generate(N, random.Random(SEED))


# --- schema correctness ---

def test_schema_completeness(records):
    for r in records:
        assert set(r.keys()) == set(SCHEMA)


def test_all_fields_are_strings(records):
    for r in records:
        for col in SCHEMA:
            assert isinstance(r[col], str), f"{col} is not str: {r[col]!r}"


def test_no_none_values(records):
    for r in records:
        for col in SCHEMA:
            assert r[col] is not None, f"{col} is None in {r}"


# --- SAP field contracts ---

def test_kunnr_max_length(records):
    for r in records:
        assert len(r["KUNNR"]) <= 10, f"KUNNR too long: {r['KUNNR']!r}"


def test_loevm_valid_values(records):
    valid = {"X", "", " "}
    for r in records:
        assert r["LOEVM"] in valid, f"Unexpected LOEVM: {r['LOEVM']!r}"


def test_erdat_is_8_chars(records):
    for r in records:
        assert len(r["ERDAT"]) == 8, f"ERDAT wrong length: {r['ERDAT']!r}"


def test_erdat_all_digits(records):
    for r in records:
        assert r["ERDAT"].isdigit(), f"ERDAT not all digits: {r['ERDAT']!r}"


def test_mandt_distribution(records):
    main = sum(1 for r in records if r["MANDT"] == "100")
    alt = sum(1 for r in records if r["MANDT"] == "200")
    assert main > N * 0.80, f"Too few MANDT=100: {main}"
    assert alt > 0, "No records with MANDT=200"


def test_spras_is_p(records):
    for r in records:
        assert r["SPRAS"] == "P"


def test_ktokd_valid_values(records):
    valid = {"0001", "0002"}
    for r in records:
        assert r["KTOKD"] in valid


# --- inconsistencies are present ---

def test_has_legacy_alphanumeric_kunnr(records):
    assert any(not r["KUNNR"].isdigit() for r in records), \
        "No alphanumeric legacy KUNNR found — migration artifact missing"


def test_has_erdat_zero(records):
    assert any(r["ERDAT"] == "00000000" for r in records), \
        "No '00000000' ERDAT found — batch load artifact missing"


def test_has_loevm_space_trap(records):
    assert any(r["LOEVM"] == " " for r in records), \
        "No LOEVM=' ' trap found — SAP type trap missing"


def test_has_formatted_stcd1(records):
    assert any("." in r["STCD1"] for r in records), \
        "No formatted STCD1 found — old extraction artifact missing"


# --- inconsistencies are bounded ---

def test_legacy_kunnr_bounded(records):
    count = sum(1 for r in records if not r["KUNNR"].isdigit())
    assert count < N * 0.30, f"Too many alphanumeric KUNNRs: {count}"


# --- determinism ---

def test_determinism():
    r1 = generate(50, random.Random(SEED))
    r2 = generate(50, random.Random(SEED))
    assert r1 == r2, "Same seed produced different output"


def test_different_seeds_differ():
    r1 = generate(50, random.Random(SEED))
    r2 = generate(50, random.Random(SEED + 1))
    assert r1 != r2, "Different seeds produced identical output"
