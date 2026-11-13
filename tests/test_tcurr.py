import random
from datetime import date

from generator.tables.tcurr import SCHEMA, generate, _invert_date

SEED = 42


def _records():
    return generate(random.Random(SEED))


# --- schema correctness ---

def test_schema_completeness():
    for r in _records():
        assert set(r.keys()) == set(SCHEMA)


def test_all_fields_are_strings():
    for r in _records():
        for col in SCHEMA:
            assert isinstance(r[col], str)


def test_no_none_values():
    for r in _records():
        for col in SCHEMA:
            assert r[col] is not None


# --- GDATU inversion ---

def test_invert_date_known_value():
    d = date(2024, 1, 15)
    assert _invert_date(d) == str(99999999 - 20240115)


def test_gdatu_length_8_or_special():
    for r in _records():
        assert len(r["GDATU"]) == 8, f"GDATU wrong length: {r['GDATU']!r}"
        assert r["GDATU"].isdigit(), f"GDATU not digits: {r['GDATU']!r}"


def test_gdatu_is_inverted():
    """Clean GDATU rows (not the '99999999' trap) should decode to a valid date."""
    for r in _records():
        if r["GDATU"] == "99999999":
            continue
        inverted = int(r["GDATU"])
        original_int = 99999999 - inverted
        # original_int should be a valid YYYYMMDD
        s = str(original_int)
        assert len(s) == 8, f"Decoded GDATU not 8 digits: {s}"
        try:
            date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        except ValueError:
            raise AssertionError(f"GDATU decodes to invalid date: {s}")


# --- currency pairs and rate types ---

def test_has_all_rate_types():
    kursts = {r["KURST"] for r in _records()}
    assert {"M", "B", "G"}.issubset(kursts)


def test_has_brl_target():
    assert any(r["TCURR"] == "BRL" for r in _records())


def test_has_usd_eur_source():
    sources = {r["FCURR"] for r in _records()}
    assert "USD" in sources
    assert "EUR" in sources


def test_ukurs_positive_on_clean_rows():
    for r in _records():
        clean = r["UKURS"].strip().replace(",", ".")
        try:
            assert float(clean) > 0
        except ValueError:
            pass


# --- inconsistencies are present ---

def test_has_gdatu_open_ended():
    assert any(r["GDATU"] == "99999999" for r in _records())


def test_has_ukurs_comma():
    assert any("," in r["UKURS"] for r in _records())


# --- determinism ---

def test_determinism():
    r1 = generate(random.Random(SEED))
    r2 = generate(random.Random(SEED))
    assert r1 == r2


def test_different_seeds_differ():
    r1 = generate(random.Random(SEED))
    r2 = generate(random.Random(SEED + 1))
    assert r1 != r2
