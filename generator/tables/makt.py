"""
MAKT — Material Descriptions
Key: MANDT + MATNR + SPRAS

One row per material per language. The lab generates Portuguese ('P') for all
materials and English ('E') for a subset, exercising the SPRAS filter that
Silver must apply. All fields are str.
"""

import random


SCHEMA = ["MANDT", "MATNR", "SPRAS", "MAKTX"]

_PT_ADJECTIVES = [
    "Industrial", "Comercial", "Premium", "Padrão", "Especial",
    "Compacto", "Reforçado", "Leve", "Resistente", "Técnico",
]
_PT_NOUNS = [
    "Componente", "Peça", "Material", "Produto", "Item",
    "Conjunto", "Módulo", "Elemento", "Parte", "Insumo",
]
_EN_ADJECTIVES = [
    "Industrial", "Commercial", "Premium", "Standard", "Special",
    "Compact", "Reinforced", "Light", "Resistant", "Technical",
]
_EN_NOUNS = [
    "Component", "Part", "Material", "Product", "Item",
    "Assembly", "Module", "Element", "Piece", "Supply",
]


def _description(adj_list: list[str], noun_list: list[str], rng: random.Random) -> str:
    return f"{rng.choice(adj_list)} {rng.choice(noun_list)}"


def generate(n: int, rng: random.Random, matnrs: list[str]) -> list[dict]:
    """
    Generate MAKT rows for the given matnrs list.
    n is ignored — one PT row per MATNR is generated, plus ~30 % also get an EN row.
    Callers pass the MATNR list from MARA to guarantee referential integrity.
    """
    records = []
    for matnr in matnrs:
        mandt = "200" if rng.random() < 0.07 else "100"

        pt_desc = _description(_PT_ADJECTIVES, _PT_NOUNS, rng)
        records.append({
            "MANDT": mandt,
            "MATNR": matnr,
            "SPRAS": "P",
            "MAKTX": pt_desc[:40],
        })

        if rng.random() < 0.30:
            en_desc = _description(_EN_ADJECTIVES, _EN_NOUNS, rng)
            records.append({
                "MANDT": mandt,
                "MATNR": matnr,
                "SPRAS": "E",
                "MAKTX": en_desc[:40],
            })

    _apply_sap_type_traps(records, rng, rate=0.08)
    return records


# ---------------------------------------------------------------------------
# Inconsistency injectors
# ---------------------------------------------------------------------------

def _apply_sap_type_traps(
    records: list[dict], rng: random.Random, rate: float
) -> None:
    """
    - MAKTX padded with trailing spaces to 40 chars (CHAR field behaviour)
    - MAKTX all-uppercase (older records before mixed-case was enabled)
    - MAKTX empty string (description never filled in)
    """
    n = max(1, int(len(records) * rate))
    for idx in rng.sample(range(len(records)), min(n, len(records))):
        r = records[idx]
        choice = rng.randint(0, 2)
        if choice == 0:
            r["MAKTX"] = r["MAKTX"].ljust(40)
        elif choice == 1:
            r["MAKTX"] = r["MAKTX"].upper()
        else:
            r["MAKTX"] = ""
