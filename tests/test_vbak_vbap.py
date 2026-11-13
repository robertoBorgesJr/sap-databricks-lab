import random

from generator.tables.kna1 import generate as generate_kna1
from generator.tables.mara import generate as generate_mara
from generator.tables.vbak import SCHEMA as VBAK_SCHEMA, generate as generate_vbak
from generator.tables.vbap import SCHEMA as VBAP_SCHEMA, generate as generate_vbap

SEED = 42
N_CUSTOMERS = 50
N_MATERIALS = 40
N_ORDERS = 80


def _build():
    rng = random.Random(SEED)
    kna1 = generate_kna1(N_CUSTOMERS, rng)
    mara = generate_mara(N_MATERIALS, rng)
    kunnrs = [r["KUNNR"] for r in kna1]
    matnrs = [r["MATNR"] for r in mara]
    vbak = generate_vbak(N_ORDERS, rng, kunnrs)
    vbap = generate_vbap(rng, vbak, matnrs)
    return vbak, vbap, kunnrs, matnrs


# =============================================================================
# VBAK
# =============================================================================

def test_vbak_schema():
    vbak, _, _, _ = _build()
    for r in vbak:
        assert set(r.keys()) == set(VBAK_SCHEMA)


def test_vbak_all_strings():
    vbak, _, _, _ = _build()
    for r in vbak:
        for col in VBAK_SCHEMA:
            assert isinstance(r[col], str)


def test_vbak_no_none():
    vbak, _, _, _ = _build()
    for r in vbak:
        for col in VBAK_SCHEMA:
            assert r[col] is not None


def test_vbak_vbeln_max_length():
    vbak, _, _, _ = _build()
    for r in vbak:
        assert len(r["VBELN"].rstrip()) <= 10


def test_vbak_erdat_format():
    vbak, _, _, _ = _build()
    for r in vbak:
        assert len(r["ERDAT"]) == 8
        assert r["ERDAT"].isdigit()


def test_vbak_waerk_values():
    vbak, _, _, _ = _build()
    valid = {"BRL", "USD", "EUR", "BRL ", "USD ", "EUR "}
    for r in vbak:
        assert r["WAERK"].strip() in {"BRL", "USD", "EUR"}


def test_vbak_currency_mix():
    vbak, _, _, _ = _build()
    brl = sum(1 for r in vbak if r["WAERK"].strip() == "BRL")
    usd = sum(1 for r in vbak if r["WAERK"].strip() == "USD")
    eur = sum(1 for r in vbak if r["WAERK"].strip() == "EUR")
    assert brl > N_ORDERS * 0.50, "Too few BRL orders"
    assert usd > 0
    assert eur > 0


def test_vbak_kunnr_referential_integrity():
    vbak, _, kunnrs, _ = _build()
    kunnr_set = set(kunnrs)
    for r in vbak:
        assert r["KUNNR"] in kunnr_set


def test_vbak_has_zero_erdat():
    vbak, _, _, _ = _build()
    assert any(r["ERDAT"] == "00000000" for r in vbak)


# =============================================================================
# VBAP
# =============================================================================

def test_vbap_schema():
    _, vbap, _, _ = _build()
    for r in vbap:
        assert set(r.keys()) == set(VBAP_SCHEMA)


def test_vbap_all_strings():
    _, vbap, _, _ = _build()
    for r in vbap:
        for col in VBAP_SCHEMA:
            assert isinstance(r[col], str)


def test_vbap_no_none():
    _, vbap, _, _ = _build()
    for r in vbap:
        for col in VBAP_SCHEMA:
            assert r[col] is not None


def test_vbap_posnr_format():
    _, vbap, _, _ = _build()
    for r in vbap:
        assert len(r["POSNR"]) == 6


def test_vbap_matnr_referential_integrity():
    _, vbap, _, matnrs = _build()
    matnr_set = set(matnrs)
    # artifacts may strip zeros or pad — only check clean records
    for r in vbap:
        clean = r["MATNR"].strip()
        # allow stripped-zero variants produced by migration artifact
        assert clean in matnr_set or clean.lstrip("0").zfill(18) in matnr_set or \
               clean.zfill(18) in matnr_set, f"MATNR {r['MATNR']!r} not in matnrs"


def test_vbap_vbeln_referential_integrity():
    vbak, vbap, _, _ = _build()
    vbeln_set = {r["VBELN"].strip() for r in vbak}
    for r in vbap:
        assert r["VBELN"].strip() in vbeln_set


def test_vbap_every_order_has_items():
    vbak, vbap, _, _ = _build()
    vbeln_in_vbap = {r["VBELN"].strip() for r in vbap}
    for order in vbak:
        assert order["VBELN"].strip() in vbeln_in_vbap


# =============================================================================
# Determinism
# =============================================================================

def test_determinism():
    rng1 = random.Random(SEED)
    kna1 = generate_kna1(N_CUSTOMERS, rng1)
    mara = generate_mara(N_MATERIALS, rng1)
    vbak1 = generate_vbak(N_ORDERS, rng1, [r["KUNNR"] for r in kna1])
    vbap1 = generate_vbap(rng1, vbak1, [r["MATNR"] for r in mara])

    rng2 = random.Random(SEED)
    kna1 = generate_kna1(N_CUSTOMERS, rng2)
    mara = generate_mara(N_MATERIALS, rng2)
    vbak2 = generate_vbak(N_ORDERS, rng2, [r["KUNNR"] for r in kna1])
    vbap2 = generate_vbap(rng2, vbak2, [r["MATNR"] for r in mara])

    assert vbak1 == vbak2
    assert vbap1 == vbap2
