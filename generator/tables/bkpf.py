"""
BKPF — Accounting Document Header
Key: MANDT + BUKRS + BELNR + GJAHR

BELNR: 10-char zero-padded.
GJAHR: 4-char fiscal year string.
BLDAT = document date, BUDAT = posting date (may differ).
"""

import random
from datetime import date, timedelta


SCHEMA = [
    "MANDT", "BUKRS", "BELNR", "GJAHR", "BLDAT", "BUDAT",
    "MONAT", "BKTXT", "USNAM", "BSTAT", "WAERS",
]

_DATE_MIN = date(2015, 1, 1)
_DATE_MAX = date(2024, 12, 31)
_TOTAL_DAYS = (_DATE_MAX - _DATE_MIN).days

_BUKRS = ["1000"]
_BSTAT = ["", "", "", "V"]   # mostly empty (posted), occasionally "V" (parked)
_WAERS = ["BRL", "BRL", "BRL", "USD", "EUR"]


def _ernam(rng: random.Random) -> str:
    initials = rng.choice("ABCDEFGHIJKLMNOPRSTV")
    surname = rng.choice([
        "SILVA", "SOUZA", "COSTA", "LIMA", "ALVES",
        "FERREIRA", "PEREIRA", "SANTOS", "RODRIGUES", "OLIVEIRA",
    ])
    return (initials + surname)[:12]


def generate(n: int, rng: random.Random) -> list[dict]:
    """
    Generate n BKPF records. Returns the list; callers pass it to generate_bseg
    so each document gets balanced accounting lines.
    """
    records = []
    for i in range(1, n + 1):
        mandt = "200" if rng.random() < 0.07 else "100"
        bldat_offset = rng.randint(0, _TOTAL_DAYS)
        bldat = (_DATE_MIN + timedelta(days=bldat_offset)).strftime("%Y%m%d")
        # posting date is same day or up to 5 days later
        budat_offset = bldat_offset + rng.randint(0, 5)
        budat = (_DATE_MIN + timedelta(days=min(budat_offset, _TOTAL_DAYS))).strftime("%Y%m%d")
        gjahr = budat[:4]
        monat = budat[4:6]

        records.append({
            "MANDT": mandt,
            "BUKRS": rng.choice(_BUKRS),
            "BELNR": str(i).zfill(10),
            "GJAHR": gjahr,
            "BLDAT": bldat,
            "BUDAT": budat,
            "MONAT": monat,
            "BKTXT": f"DOC {i}" if rng.random() > 0.4 else "",
            "USNAM": _ernam(rng),
            "BSTAT": rng.choice(_BSTAT),
            "WAERS": rng.choice(_WAERS),
        })

    _apply_migration_artifacts(records, rng, rate=0.07)
    _apply_sap_type_traps(records, rng, rate=0.07)

    return records


# ---------------------------------------------------------------------------
# Inconsistency injectors
# ---------------------------------------------------------------------------

def _apply_migration_artifacts(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    - BLDAT = '00000000' (document date not filled in migrated entries)
    - BKTXT with leading/trailing spaces (pasted from external system)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 1)
        if choice == 0:
            r["BLDAT"] = "00000000"
            r["USNAM"] = rng.choice(["MIGRATE", "BATCH01"])
        else:
            if r["BKTXT"]:
                r["BKTXT"] = "  " + r["BKTXT"] + "  "


def _apply_sap_type_traps(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    - BSTAT = ' ' (space) instead of '' — parked status ambiguity
    - WAERS padded with trailing space ('BRL ')
    - BELNR padded with trailing space to 10 chars (CHAR field)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0:
            if r["BSTAT"] == "":
                r["BSTAT"] = " "
        elif choice == 1:
            r["WAERS"] = r["WAERS"] + " "
        else:
            r["BELNR"] = r["BELNR"].ljust(10)
