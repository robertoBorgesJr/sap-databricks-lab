"""
MARA — Material Master (General Data)
Key: MANDT + MATNR

All fields returned as str, mirroring a raw SAP extraction.
MATNR: 18-char zero-padded for numeric materials; alphanumeric materials keep
their own format (no padding). LVORM (deletion flag) follows the X/space/space
trap identical to KNA1.LOEVM.
"""

import random
from datetime import date, timedelta


SCHEMA = [
    "MANDT", "MATNR", "ERSDA", "ERNAM", "MTART", "MBRSH", "MATKL",
    "MEINS", "BRGEW", "GEWEI", "NTGEW", "LVORM",
]

_DATE_MIN = date(2010, 1, 1)
_DATE_MAX = date(2024, 12, 31)
_TOTAL_DAYS = (_DATE_MAX - _DATE_MIN).days

_MTART = ["FERT", "ROH", "HALB", "HAWA"]
_MBRSH = ["M", "A", "B", "H"]   # indústria
_MATKL = ["001", "002", "003", "004", "010"]
_MEINS = ["ST", "KG", "L", "M", "PC"]
_GEWEI = ["KG", "G", "LB"]


def _ernam(rng: random.Random) -> str:
    initials = rng.choice("ABCDEFGHIJKLMNOPRSTV")
    surname = rng.choice([
        "SILVA", "SOUZA", "COSTA", "LIMA", "ALVES",
        "FERREIRA", "PEREIRA", "SANTOS", "RODRIGUES", "OLIVEIRA",
    ])
    return (initials + surname)[:12]


def generate(n: int, rng: random.Random) -> list[dict]:
    """
    Generate n MARA records as all-string dicts.
    Returns the list; callers collect MATNR values for MAKT / VBAP.
    """
    records = []
    for i in range(1, n + 1):
        mandt = "200" if rng.random() < 0.07 else "100"
        # 90 % numeric (zero-padded 18 chars), 10 % alphanumeric (no padding)
        if rng.random() < 0.90:
            matnr = str(i).zfill(18)
        else:
            prefix = rng.choice(["MAT-", "EXT-", "IMP"])
            matnr = (prefix + str(rng.randint(100, 9999)))[:18]

        ersda = (_DATE_MIN + timedelta(days=rng.randint(0, _TOTAL_DAYS))).strftime("%Y%m%d")
        brgew = f"{rng.uniform(0.1, 500.0):.3f}"
        ntgew = f"{float(brgew) * rng.uniform(0.85, 1.0):.3f}"

        records.append({
            "MANDT": mandt,
            "MATNR": matnr,
            "ERSDA": ersda,
            "ERNAM": _ernam(rng),
            "MTART": rng.choice(_MTART),
            "MBRSH": rng.choice(_MBRSH),
            "MATKL": rng.choice(_MATKL),
            "MEINS": rng.choice(_MEINS),
            "BRGEW": brgew,
            "GEWEI": rng.choice(_GEWEI),
            "NTGEW": ntgew,
            "LVORM": "X" if rng.random() < 0.04 else "",
        })

    _apply_migration_artifacts(records, rng, rate=0.10)
    _apply_manual_entry_errors(records, rng, rate=0.04)
    _apply_sap_type_traps(records, rng, rate=0.08)

    return records


# ---------------------------------------------------------------------------
# Inconsistency injectors
# ---------------------------------------------------------------------------

def _apply_migration_artifacts(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    Legacy load artifacts:
    - ERSDA = '00000000' + generic ERNAM (batch job never filled creation date)
    - BRGEW = '' (weight never informed in legacy system)
    - MATNR with no zero-padding even though it is numeric (old short format)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0:
            r["ERSDA"] = "00000000"
            r["ERNAM"] = rng.choice(["MIGRATE", "BATCH01", "LOADJOB"])
        elif choice == 1:
            r["BRGEW"] = ""
            r["NTGEW"] = ""
        else:
            # strip leading zeros from a numeric MATNR — old short format
            if r["MATNR"].isdigit():
                r["MATNR"] = r["MATNR"].lstrip("0") or "0"


def _apply_manual_entry_errors(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    Human data-entry mistakes:
    - MEINS with extra space ('ST ' instead of 'ST')
    - BRGEW with comma decimal separator instead of period (copied from Excel)
    - MATKL left empty (class not assigned)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0:
            r["MEINS"] = r["MEINS"] + " "
        elif choice == 1 and r["BRGEW"]:
            r["BRGEW"] = r["BRGEW"].replace(".", ",")
            r["NTGEW"] = r["NTGEW"].replace(".", ",")
        else:
            r["MATKL"] = ""


def _apply_sap_type_traps(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    SAP type/encoding quirks:
    - LVORM = ' ' (space) instead of '' — the same flag trap as LOEVM in KNA1
    - BRGEW = '0.000' (explicitly zero, not empty — different semantics)
    - MATNR padded with trailing spaces to 18 chars (CHAR field behaviour)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0:
            if r["LVORM"] == "":
                r["LVORM"] = " "
        elif choice == 1:
            r["BRGEW"] = "0.000"
            r["NTGEW"] = "0.000"
        else:
            r["MATNR"] = r["MATNR"].ljust(18)
