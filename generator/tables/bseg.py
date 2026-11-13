"""
BSEG — Accounting Document Lines
Key: MANDT + BUKRS + BELNR + GJAHR + BUZEI

Critical SAP constraint: for every BKPF document, sum of DMBTR where SHKZG='S'
(debit) must equal sum of DMBTR where SHKZG='H' (credit). This is enforced here
by generating N-1 free lines and computing the last line as the balancing entry.

DMBTR is always positive; the sign comes exclusively from SHKZG.
"""

import random
from decimal import Decimal


SCHEMA = [
    "MANDT", "BUKRS", "BELNR", "GJAHR", "BUZEI",
    "SHKZG", "DMBTR", "HKONT", "KOSTL", "MWSKZ",
]

_HKONT_DEBIT = ["0001100000", "0001200000", "0004000000", "0006300000"]
_HKONT_CREDIT = ["0004400000", "0004100000", "0007600000", "0002100000"]
_KOSTL = ["", "0001", "0002", "CORP", "SELL"]
_MWSKZ = ["", "S1", "S2", "V1", "V2"]


def generate(rng: random.Random, bkpf_records: list[dict]) -> list[dict]:
    """
    Generate BSEG rows for each BKPF document.
    Each document gets 2–5 debit lines and a single balancing credit line,
    guaranteeing zero net per document.
    """
    records = []
    for doc in bkpf_records:
        n_debits = rng.randint(1, 4)
        debit_amounts = [
            Decimal(str(round(rng.uniform(10.0, 5000.0), 2)))
            for _ in range(n_debits)
        ]
        total_debit = sum(debit_amounts)
        buzei = 1

        for amount in debit_amounts:
            records.append({
                "MANDT": doc["MANDT"],
                "BUKRS": doc["BUKRS"],
                "BELNR": doc["BELNR"],
                "GJAHR": doc["GJAHR"],
                "BUZEI": str(buzei).zfill(3),
                "SHKZG": "S",
                "DMBTR": str(amount),
                "HKONT": rng.choice(_HKONT_DEBIT),
                "KOSTL": rng.choice(_KOSTL),
                "MWSKZ": rng.choice(_MWSKZ),
            })
            buzei += 1

        # balancing credit line — amount = total debits
        records.append({
            "MANDT": doc["MANDT"],
            "BUKRS": doc["BUKRS"],
            "BELNR": doc["BELNR"],
            "GJAHR": doc["GJAHR"],
            "BUZEI": str(buzei).zfill(3),
            "SHKZG": "H",
            "DMBTR": str(total_debit),
            "HKONT": rng.choice(_HKONT_CREDIT),
            "KOSTL": rng.choice(_KOSTL),
            "MWSKZ": "",
        })

    _apply_migration_artifacts(records, rng, rate=0.06)
    _apply_sap_type_traps(records, rng, rate=0.07)

    return records


# ---------------------------------------------------------------------------
# Inconsistency injectors
# ---------------------------------------------------------------------------

def _apply_migration_artifacts(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    - KOSTL with old cost-centre format (spaces instead of zero-padding)
    - DMBTR with comma decimal separator (from legacy extract)
    - HKONT not zero-padded (old 6-digit account format)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0 and r["KOSTL"]:
            r["KOSTL"] = r["KOSTL"].ljust(10)
        elif choice == 1:
            r["DMBTR"] = r["DMBTR"].replace(".", ",")
        else:
            # strip leading zeros from account (6-digit legacy format)
            r["HKONT"] = r["HKONT"].lstrip("0") or "0"


def _apply_sap_type_traps(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    - SHKZG padded with trailing space ('S ')
    - MWSKZ = ' ' (space) instead of '' (tax code not filled)
    - KOSTL padded to 10 chars (CHAR field trailing spaces)
    Zeroing DMBTR is intentionally excluded: it would break the debit=credit
    constraint that BKPF requires, which SAP would reject at posting time.
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0:
            r["SHKZG"] = r["SHKZG"] + " "
        elif choice == 1:
            if r["MWSKZ"] == "":
                r["MWSKZ"] = " "
        else:
            if r["KOSTL"]:
                r["KOSTL"] = r["KOSTL"].ljust(10)
