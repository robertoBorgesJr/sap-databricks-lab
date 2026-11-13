"""
KNA1 — Customer Master (General Data)
Key: MANDT + KUNNR

All fields are returned as str, exactly as a raw SAP extraction would deliver them.
Intentional inconsistencies are injected by three post-processing functions to
simulate real-world data quality issues found in SAP extractions.
"""

import random
import re
from datetime import date, timedelta

from faker import Faker


SCHEMA = [
    "MANDT", "KUNNR", "LAND1", "NAME1", "NAME2", "ORT01", "PSTLZ",
    "STRAS", "STCD1", "STCD2", "LOEVM", "ERDAT", "ERNAM", "TELF1",
    "SPRAS", "KTOKD",
]

_DATE_MIN = date(2010, 1, 1)
_DATE_MAX = date(2024, 12, 31)
_TOTAL_DAYS = (_DATE_MAX - _DATE_MIN).days


def _digits_only(value: str) -> str:
    return re.sub(r"\D", "", value)


def _ernam(fake: Faker) -> str:
    # SAP user format: first initial + last name, uppercase, max 12 chars
    return (fake.first_name()[:1] + fake.last_name()[:11]).upper()[:12]


def generate(n: int, rng: random.Random) -> list[dict]:
    """
    Generate n KNA1 records as a list of dicts with all-string values.
    Deterministic: same rng seed always produces the same output.
    """
    fake = Faker("pt_BR")
    Faker.seed(rng.randint(0, 2**31 - 1))

    records = []
    for i in range(1, n + 1):
        mandt = "200" if rng.random() < 0.07 else "100"
        kunnr = str(i).zfill(10)

        erdat = (_DATE_MIN + timedelta(days=rng.randint(0, _TOTAL_DAYS))).strftime("%Y%m%d")

        # CNPJ and CPF stripped of formatting — SAP stores digits only
        stcd1 = _digits_only(fake.cnpj())
        stcd2 = _digits_only(fake.cpf()) if rng.random() < 0.30 else ""

        # CEP: 8 digits, strip the hyphen Faker adds
        pstlz = _digits_only(fake.postcode())[:8]

        records.append({
            "MANDT": mandt,
            "KUNNR": kunnr,
            "LAND1": "BR",
            "NAME1": fake.company()[:35].rstrip(),
            "NAME2": fake.company()[:35].rstrip() if rng.random() > 0.6 else "",
            "ORT01": fake.city()[:35],
            "PSTLZ": pstlz,
            "STRAS": fake.street_name()[:35],
            "STCD1": stcd1,
            "STCD2": stcd2,
            "LOEVM": "X" if rng.random() < 0.05 else "",
            "ERDAT": erdat,
            "ERNAM": _ernam(fake),
            "TELF1": fake.phone_number()[:16],
            "SPRAS": "P",
            "KTOKD": rng.choice(["0001", "0002"]),
        })

    _apply_migration_artifacts(records, rng, rate=0.12)
    _apply_manual_entry_errors(records, rng, rate=0.05)
    _apply_sap_type_traps(records, rng, rate=0.08)

    return records


# ---------------------------------------------------------------------------
# Inconsistency injectors
# ---------------------------------------------------------------------------

def _apply_migration_artifacts(
    records: list[dict], rng: random.Random, rate: float = 0.12
) -> None:
    """
    Simulates records loaded from a legacy system before SAP standardization:
    - Alphanumeric KUNNR without zero-padding (old system had its own numbering)
    - ERDAT = '00000000' and ERNAM = batch user (date never filled during load)
    - STCD1 with formatting dots/slashes (old extraction format)
    - ERNAM = generic batch username
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 3)
        if choice == 0:
            prefix = rng.choice(["ALT", "MIG", "OLD"])
            r["KUNNR"] = (prefix + str(rng.randint(100, 9999)))[:10]
        elif choice == 1:
            # Batch load never filled the creation date
            r["ERDAT"] = "00000000"
            r["ERNAM"] = rng.choice(["MIGRATE", "BATCH01", "LOADJOB"])
        elif choice == 2:
            # Old extraction kept CNPJ formatting
            d = r["STCD1"]
            if len(d) == 14:
                r["STCD1"] = f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"
        else:
            r["ERNAM"] = rng.choice(["MIGRATE", "BATCH01", "LOADJOB"])


def _apply_manual_entry_errors(
    records: list[dict], rng: random.Random, rate: float = 0.05
) -> None:
    """
    Simulates human data-entry mistakes:
    - NAME2 accidentally duplicates NAME1
    - PSTLZ with wrong digit count (7 or 9 instead of 8)
    - TELF1 empty even when LAND1 = 'BR' (mandatory field bypassed)
    - LAND1 empty (not filled during migration)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 3)
        if choice == 0:
            r["NAME2"] = r["NAME1"]
        elif choice == 1:
            digits = _digits_only(r["PSTLZ"])
            # 7 digits (one missing) or 9 digits (one extra)
            r["PSTLZ"] = digits[:7] if rng.random() < 0.5 else digits + str(rng.randint(0, 9))
        elif choice == 2:
            r["TELF1"] = ""
        else:
            r["LAND1"] = ""


def _apply_sap_type_traps(
    records: list[dict], rng: random.Random, rate: float = 0.08
) -> None:
    """
    Simulates subtle SAP type/encoding quirks that trip up naive transformations:
    - LOEVM = ' ' (single space) instead of '' — trim() alone is not enough
    - NAME1 all-uppercase (older records before mixed-case was standard)
    - STCD1 = '' even with LAND1 = 'BR' (tax number never filled)
    - NAME1 space-padded to full field length (SAP CHAR behaviour)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 3)
        if choice == 0:
            # Space instead of empty string — the SAP LOEVM trap
            if r["LOEVM"] == "":
                r["LOEVM"] = " "
        elif choice == 1:
            r["NAME1"] = r["NAME1"].upper()
            if r["NAME2"]:
                r["NAME2"] = r["NAME2"].upper()
        elif choice == 2:
            r["STCD1"] = ""
        else:
            # CHAR field padded with trailing spaces to its declared length (35)
            r["NAME1"] = r["NAME1"].ljust(35)
