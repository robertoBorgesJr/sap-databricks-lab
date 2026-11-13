import random
from decimal import Decimal

from generator.tables.bkpf import SCHEMA as BKPF_SCHEMA, generate as generate_bkpf
from generator.tables.bseg import SCHEMA as BSEG_SCHEMA, generate as generate_bseg

SEED = 42
N_DOCS = 60


def _build():
    rng = random.Random(SEED)
    bkpf = generate_bkpf(N_DOCS, rng)
    bseg = generate_bseg(rng, bkpf)
    return bkpf, bseg


# =============================================================================
# BKPF
# =============================================================================

def test_bkpf_schema():
    bkpf, _ = _build()
    for r in bkpf:
        assert set(r.keys()) == set(BKPF_SCHEMA)


def test_bkpf_all_strings():
    bkpf, _ = _build()
    for r in bkpf:
        for col in BKPF_SCHEMA:
            assert isinstance(r[col], str)


def test_bkpf_no_none():
    bkpf, _ = _build()
    for r in bkpf:
        for col in BKPF_SCHEMA:
            assert r[col] is not None


def test_bkpf_belnr_length():
    bkpf, _ = _build()
    for r in bkpf:
        assert len(r["BELNR"].rstrip()) <= 10


def test_bkpf_gjahr_is_year():
    bkpf, _ = _build()
    for r in bkpf:
        assert r["GJAHR"].isdigit() and len(r["GJAHR"]) == 4


def test_bkpf_budat_format():
    bkpf, _ = _build()
    for r in bkpf:
        assert len(r["BUDAT"]) == 8
        assert r["BUDAT"].isdigit()


def test_bkpf_bstat_values():
    bkpf, _ = _build()
    valid = {"", "V", " "}
    for r in bkpf:
        assert r["BSTAT"] in valid


def test_bkpf_has_zero_bldat():
    bkpf, _ = _build()
    assert any(r["BLDAT"] == "00000000" for r in bkpf)


def test_bkpf_has_bstat_space_trap():
    bkpf, _ = _build()
    assert any(r["BSTAT"] == " " for r in bkpf)


# =============================================================================
# BSEG
# =============================================================================

def test_bseg_schema():
    _, bseg = _build()
    for r in bseg:
        assert set(r.keys()) == set(BSEG_SCHEMA)


def test_bseg_all_strings():
    _, bseg = _build()
    for r in bseg:
        for col in BSEG_SCHEMA:
            assert isinstance(r[col], str)


def test_bseg_no_none():
    _, bseg = _build()
    for r in bseg:
        for col in BSEG_SCHEMA:
            assert r[col] is not None


def test_bseg_shkzg_base_values():
    _, bseg = _build()
    for r in bseg:
        # strip space artifact before checking base value
        assert r["SHKZG"].strip() in {"S", "H"}


def test_bseg_dmbtr_non_negative_on_clean_rows():
    _, bseg = _build()
    for r in bseg:
        val = r["DMBTR"].replace(",", ".")
        try:
            assert float(val) >= 0.0
        except ValueError:
            pass  # artifact rows may have non-numeric format


def test_bseg_balance_per_document():
    """For each document, sum of S lines = sum of H lines (ignoring artifact rows)."""
    bkpf, bseg = _build()
    bkpf_keys = {(r["BUKRS"], r["BELNR"].strip(), r["GJAHR"]) for r in bkpf}

    from collections import defaultdict
    debits: dict = defaultdict(Decimal)
    credits: dict = defaultdict(Decimal)

    for r in bseg:
        key = (r["BUKRS"], r["BELNR"].strip(), r["GJAHR"])
        try:
            amount = Decimal(r["DMBTR"].replace(",", "."))
        except Exception:
            continue
        shkzg = r["SHKZG"].strip()
        if shkzg == "S":
            debits[key] += amount
        elif shkzg == "H":
            credits[key] += amount

    for key in bkpf_keys:
        diff = abs(debits[key] - credits[key])
        assert diff == Decimal("0"), f"Document {key} not balanced: {debits[key]} vs {credits[key]}"


def test_bseg_has_shkzg_space_trap():
    _, bseg = _build()
    assert any(r["SHKZG"].endswith(" ") for r in bseg)


def test_bseg_has_hkont_unpadded():
    _, bseg = _build()
    assert any(len(r["HKONT"]) < 10 and r["HKONT"].isdigit() for r in bseg)


# =============================================================================
# Determinism
# =============================================================================

def test_determinism():
    rng1 = random.Random(SEED)
    bkpf1 = generate_bkpf(N_DOCS, rng1)
    bseg1 = generate_bseg(rng1, bkpf1)

    rng2 = random.Random(SEED)
    bkpf2 = generate_bkpf(N_DOCS, rng2)
    bseg2 = generate_bseg(rng2, bkpf2)

    assert bkpf1 == bkpf2
    assert bseg1 == bseg2
