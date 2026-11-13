"""
TCURR — Exchange Rates
Key: MANDT + KURST + FCURR + TCURR + GDATU

The critical SAP quirk here is GDATU: dates are stored inverted as
  99999999 - YYYYMMDD  (integer subtraction, then formatted as 8-digit string)
so that the most recent rate sorts last in ascending order within a currency pair.

Example: 2024-01-15 → 99999999 - 20240115 = 79759884 → "79759884"

UKURS: exchange rate, stored as a string decimal. Positive means 1 FCURR = UKURS TCURR.
KURST: rate type. 'M' (standard), 'B' (bank buying), 'G' (bank selling).
"""

import random
from datetime import date, timedelta


SCHEMA = ["MANDT", "KURST", "FCURR", "TCURR", "GDATU", "UKURS", "FFACT", "TFACT"]

_KURST = ["M", "B", "G"]
# pairs: source → target (always convert to BRL in this lab)
_PAIRS = [
    ("USD", "BRL"),
    ("EUR", "BRL"),
    ("USD", "EUR"),
]
# approximate rates with realistic ranges
_RATE_RANGES = {
    ("USD", "BRL"): (4.80, 5.80),
    ("EUR", "BRL"): (5.20, 6.20),
    ("USD", "EUR"): (0.88, 0.96),
}

_DATE_MIN = date(2015, 1, 1)
_DATE_MAX = date(2024, 12, 31)


def _invert_date(d: date) -> str:
    """SAP GDATU: 99999999 minus the date as integer YYYYMMDD."""
    date_int = int(d.strftime("%Y%m%d"))
    return str(99999999 - date_int)


def generate(rng: random.Random) -> list[dict]:
    """
    Generate TCURR rows: for each (KURST, FCURR, TCURR) pair, produce monthly
    rates from 2015 to 2024, giving ~120 rows per pair × 3 rate types × 3 pairs
    = ~1 080 rows total.
    """
    records = []

    for kurst in _KURST:
        for fcurr, tcurr_col in _PAIRS:
            rate_min, rate_max = _RATE_RANGES[(fcurr, tcurr_col)]
            current_date = _DATE_MIN
            current_rate = rng.uniform(rate_min, rate_max)

            while current_date <= _DATE_MAX:
                # random walk: rate drifts ±3 % per month
                drift = rng.uniform(-0.03, 0.03)
                current_rate = max(rate_min, min(rate_max, current_rate * (1 + drift)))

                records.append({
                    "MANDT": "100",
                    "KURST": kurst,
                    "FCURR": fcurr,
                    "TCURR": tcurr_col,
                    "GDATU": _invert_date(current_date),
                    "UKURS": f"{current_rate:.5f}",
                    "FFACT": "1",
                    "TFACT": "1",
                })

                # advance one month
                month = current_date.month + 1
                year = current_date.year + (month - 1) // 12
                month = ((month - 1) % 12) + 1
                try:
                    current_date = date(year, month, 1)
                except ValueError:
                    break

    _apply_sap_type_traps(records, rng, rate=0.05)
    return records


# ---------------------------------------------------------------------------
# Inconsistency injectors
# ---------------------------------------------------------------------------

def _apply_sap_type_traps(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    - UKURS with comma decimal separator (old extraction format)
    - UKURS padded with trailing spaces ('5.12300  ')
    - GDATU = '99999999' (open-ended rate — no end date, used as the default)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0:
            r["UKURS"] = r["UKURS"].replace(".", ",")
        elif choice == 1:
            r["UKURS"] = r["UKURS"].ljust(10)
        else:
            r["GDATU"] = "99999999"
