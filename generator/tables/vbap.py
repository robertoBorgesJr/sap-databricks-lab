"""
VBAP — Sales Order Items
Key: MANDT + VBELN + POSNR

POSNR: 6-char zero-padded, counting by 10 (000010, 000020, ...).
MATNR: inherited from MARA (18-char format).
Currency (WAERK) always matches the parent VBAK header.
NETWR here is the item-level net value (not the order total).
"""

import random
from datetime import date, timedelta


SCHEMA = [
    "MANDT", "VBELN", "POSNR", "MATNR", "MENGE", "MEINS",
    "NETWR", "WAERK", "WERKS", "LGORT", "ABGRU",
]

_WERKS = ["1000", "1001", "2000", "3000"]
_LGORT = ["0001", "0002", "WH01", "WH02"]
_ABGRU = ["", "", "", "", "01", "02", "ZZ"]  # mostly empty (not rejected)
_MEINS = ["ST", "KG", "L", "M", "PC"]


def generate(
    rng: random.Random,
    orders: list[dict],   # list of VBAK records (need VBELN, WAERK, MANDT)
    matnrs: list[str],
    min_items: int = 1,
    max_items: int = 5,
) -> list[dict]:
    """
    Generate VBAP rows for every VBAK in orders.
    Each order gets between min_items and max_items items.
    matnrs must come from MARA to ensure referential integrity.
    """
    records = []
    for order in orders:
        n_items = rng.randint(min_items, max_items)
        for pos_idx in range(1, n_items + 1):
            posnr = str(pos_idx * 10).zfill(6)
            menge = f"{rng.randint(1, 100)}.000"
            netwr = f"{rng.uniform(10.0, 10000.0):.2f}"

            records.append({
                "MANDT": order["MANDT"],
                "VBELN": order["VBELN"],
                "POSNR": posnr,
                "MATNR": rng.choice(matnrs),
                "MENGE": menge,
                "MEINS": rng.choice(_MEINS),
                "NETWR": netwr,
                "WAERK": order["WAERK"],
                "WERKS": rng.choice(_WERKS),
                "LGORT": rng.choice(_LGORT),
                "ABGRU": rng.choice(_ABGRU),
            })

    _apply_migration_artifacts(records, rng, rate=0.07)
    _apply_manual_entry_errors(records, rng, rate=0.04)
    _apply_sap_type_traps(records, rng, rate=0.07)

    return records


# ---------------------------------------------------------------------------
# Inconsistency injectors
# ---------------------------------------------------------------------------

def _apply_migration_artifacts(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    - NETWR = '0.00' (item migrated without pricing)
    - MENGE = '0.000' (quantity never filled)
    - MATNR with stripped leading zeros (old short format from legacy)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0:
            r["NETWR"] = "0.00"
        elif choice == 1:
            r["MENGE"] = "0.000"
        else:
            if r["MATNR"].isdigit():
                r["MATNR"] = r["MATNR"].lstrip("0") or "0"


def _apply_manual_entry_errors(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    - NETWR with comma decimal separator
    - MENGE negative (data-entry typo — invalid in SAP but happens in extractions)
    - LGORT empty (storage location not assigned)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0:
            r["NETWR"] = r["NETWR"].replace(".", ",")
        elif choice == 1:
            r["MENGE"] = "-" + r["MENGE"]
        else:
            r["LGORT"] = ""


def _apply_sap_type_traps(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    - MATNR padded with trailing spaces to 18 chars (CHAR field)
    - WAERK padded with trailing space ('BRL ')
    - POSNR = '000000' (item position reset error — should not happen but does)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0:
            r["MATNR"] = r["MATNR"].ljust(18)
        elif choice == 1:
            r["WAERK"] = r["WAERK"].rstrip() + " "
        else:
            r["POSNR"] = "000000"
