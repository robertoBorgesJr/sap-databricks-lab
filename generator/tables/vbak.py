"""
VBAK — Sales Order Header
Key: MANDT + VBELN

VBELN: 10-char zero-padded numeric string (ALPHA conversion).
Currency mix: BRL (70 %), USD (20 %), EUR (10 %) — exercises TCURR conversion in Gold.
NETWR is the order total at header level (sum of items added externally).
"""

import random
from datetime import date, timedelta


SCHEMA = [
    "MANDT", "VBELN", "ERDAT", "ERNAM", "AUDAT", "VKORG", "VTWEG",
    "SPART", "KUNNR", "NETWR", "WAERK", "VKBUR", "BSTNK",
]

_DATE_MIN = date(2015, 1, 1)
_DATE_MAX = date(2024, 12, 31)
_TOTAL_DAYS = (_DATE_MAX - _DATE_MIN).days

_VKORG = ["1000", "1001", "2000"]
_VTWEG = ["10", "20", "30"]
_SPART = ["01", "02", "10"]
_VKBUR = ["0001", "0002", "0003"]
_CURRENCY_WEIGHTS = [("BRL", 0.70), ("USD", 0.20), ("EUR", 0.10)]


def _pick_currency(rng: random.Random) -> str:
    r = rng.random()
    cumulative = 0.0
    for currency, weight in _CURRENCY_WEIGHTS:
        cumulative += weight
        if r < cumulative:
            return currency
    return "BRL"


def _ernam(rng: random.Random) -> str:
    initials = rng.choice("ABCDEFGHIJKLMNOPRSTV")
    surname = rng.choice([
        "SILVA", "SOUZA", "COSTA", "LIMA", "ALVES",
        "FERREIRA", "PEREIRA", "SANTOS", "RODRIGUES", "OLIVEIRA",
    ])
    return (initials + surname)[:12]


def generate(n: int, rng: random.Random, kunnrs: list[str]) -> list[dict]:
    """
    Generate n VBAK records. kunnrs must come from KNA1 to ensure referential integrity.
    Returns the list; callers collect VBELN + WAERK for VBAP.
    """
    records = []
    for i in range(1, n + 1):
        mandt = "200" if rng.random() < 0.07 else "100"
        vbeln = str(i).zfill(10)
        erdat = (_DATE_MIN + timedelta(days=rng.randint(0, _TOTAL_DAYS))).strftime("%Y%m%d")
        audat = erdat  # order date = creation date in most cases

        waerk = _pick_currency(rng)
        netwr = f"{rng.uniform(100.0, 50000.0):.2f}"

        records.append({
            "MANDT": mandt,
            "VBELN": vbeln,
            "ERDAT": erdat,
            "ERNAM": _ernam(rng),
            "AUDAT": audat,
            "VKORG": rng.choice(_VKORG),
            "VTWEG": rng.choice(_VTWEG),
            "SPART": rng.choice(_SPART),
            "KUNNR": rng.choice(kunnrs),
            "NETWR": netwr,
            "WAERK": waerk,
            "VKBUR": rng.choice(_VKBUR),
            "BSTNK": str(rng.randint(10000, 99999)) if rng.random() > 0.4 else "",
        })

    _apply_migration_artifacts(records, rng, rate=0.08)
    _apply_manual_entry_errors(records, rng, rate=0.05)
    _apply_sap_type_traps(records, rng, rate=0.07)

    return records


# ---------------------------------------------------------------------------
# Inconsistency injectors
# ---------------------------------------------------------------------------

def _apply_migration_artifacts(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    - ERDAT = '00000000' (batch load never filled)
    - NETWR = '0.00' (order migrated without recalculation)
    - VBELN with old non-zero-padded format (alphanumeric from legacy)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0:
            r["ERDAT"] = "00000000"
            r["ERNAM"] = rng.choice(["MIGRATE", "BATCH01", "LOADJOB"])
        elif choice == 1:
            r["NETWR"] = "0.00"
        else:
            prefix = rng.choice(["ORD", "OLD", "MIG"])
            r["VBELN"] = (prefix + str(rng.randint(1000, 9999)))[:10]


def _apply_manual_entry_errors(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    - BSTNK duplicated across two orders (customer re-sent same PO number)
    - NETWR with comma decimal separator
    - AUDAT earlier than ERDAT (data entry mistake)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0 and r["BSTNK"]:
            # duplicate the PO number from a neighbour
            neighbour = records[rng.randint(0, len(records) - 1)]
            r["BSTNK"] = neighbour["BSTNK"]
        elif choice == 1:
            r["NETWR"] = r["NETWR"].replace(".", ",")
        else:
            # AUDAT one month before ERDAT
            try:
                d = date(int(r["ERDAT"][:4]), int(r["ERDAT"][4:6]), int(r["ERDAT"][6:8]))
                earlier = d - timedelta(days=30)
                r["AUDAT"] = earlier.strftime("%Y%m%d")
            except ValueError:
                pass


def _apply_sap_type_traps(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    - VBELN padded to 10 chars with trailing spaces (CHAR field)
    - WAERK padded with trailing space ('BRL ')
    - NETWR = '' (never calculated — incomplete order)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0:
            r["VBELN"] = r["VBELN"].ljust(10)
        elif choice == 1:
            r["WAERK"] = r["WAERK"] + " "
        else:
            r["NETWR"] = ""
