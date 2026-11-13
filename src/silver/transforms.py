"""
Pure SAP field conversion functions — zero Spark dependency.

Estas funções espelham as transformações aplicadas nos notebooks silver usando
funções nativas do Spark. Existem aqui para testes unitários locais (pytest),
sem precisar de cluster.

Armadilhas SAP cobertas
-----------------------
DATS        '00000000' e string vazia → None
TIMS        string HHMMSS → time
Decimal     separador de milhar com vírgula (formato europeu) → period
Flag        'X' = True; '' e ' ' (armadilha do espaço) = False
ALPHA       remove zeros à esquerda apenas de strings puramente numéricas
GDATU       data invertida: 99999999 − YYYYMMDD → date real; '99999999' = open-ended → None
BSEG/SHKZG  'S' (Soll = débito) → positivo; 'H' (Haben = crédito) → negativo
"""

from __future__ import annotations

from datetime import date, time
from decimal import Decimal, InvalidOperation


def parse_dats(s: str | None) -> date | None:
    """SAP DATS (YYYYMMDD) → Python date.  '00000000' e string em branco → None."""
    if s is None:
        return None
    s = s.strip()
    if not s or s == "00000000":
        return None
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except (ValueError, IndexError):
        return None


def parse_tims(s: str | None) -> time | None:
    """SAP TIMS (HHMMSS) → Python time.  String vazia ou curta → None."""
    if s is None:
        return None
    s = s.strip()
    if len(s) < 6:
        return None
    try:
        return time(int(s[:2]), int(s[2:4]), int(s[4:6]))
    except ValueError:
        return None


def parse_decimal(s: str | None) -> Decimal | None:
    """
    Parse de string decimal SAP → Decimal.
    Trata separador de vírgula (formato europeu) e espaços.
    Retorna None para string vazia ou não parseável.
    """
    if s is None:
        return None
    s = s.strip().replace(",", ".")
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def flag_to_bool(s: str | None) -> bool:
    """
    Flag SAP: 'X' → True; qualquer outra coisa (incluindo ' ') → False.
    Armadilha do espaço: campos LOEVM/LVORM às vezes chegam como ' ' em vez de ''.
    Ambos devem resultar em False (não excluído).
    """
    if s is None:
        return False
    return s.strip() == "X"


def empty_to_none(s: str | None) -> str | None:
    """
    String em branco ou somente espaços → None.
    Campos CHAR do SAP frequentemente carregam trailing spaces em vez de nulos.
    """
    if s is None:
        return None
    stripped = s.strip()
    return stripped if stripped else None


def alpha_strip(s: str | None) -> str | None:
    """
    Conversão ALPHA do SAP: remove zeros à esquerda apenas de strings
    puramente numéricas.  Valores alfanuméricos (artefatos de migração como
    'ALT1234') recebem apenas trim — zeros não são removidos.

    '0000000000'         → '0'      (todos zeros → mantém um zero)
    '0000001234'         → '1234'
    '000000000000000001' → '1'      (MATNR 18 chars)
    'ALT1234   '         → 'ALT1234' (alfanumérico: só trim)
    """
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    if s.isdigit():
        stripped = s.lstrip("0")
        return stripped if stripped else "0"
    return s


def invert_gdatu(s: str | None) -> date | None:
    """
    TCURR.GDATU é armazenado como 99999999 − YYYYMMDD (aritmética inteira).
    '99999999' significa taxa sem data de fim (open-ended) → None.

    Exemplo: '79759884' → 99999999 − 79759884 = 20240115 → date(2024, 1, 15)
    """
    if s is None:
        return None
    s = s.strip()
    if not s or s == "99999999" or s == "00000000":
        return None
    try:
        real_int = 99999999 - int(s)
        year  = real_int // 10000
        month = (real_int % 10000) // 100
        day   = real_int % 100
        return date(year, month, day)
    except (ValueError, OverflowError):
        return None


def signed_amount(value: str | None, shkzg: str | None) -> Decimal | None:
    """
    BSEG: DMBTR é sempre positivo; o sinal vem exclusivamente de SHKZG.
      'S' (Soll  = débito)  → valor positivo
      'H' (Haben = crédito) → valor negativo

    Armadilha: SHKZG pode vir com trailing space ('S ') — trim antes de comparar.
    """
    amount = parse_decimal(value)
    if amount is None:
        return None
    sign_key = (shkzg or "").strip()
    return -amount if sign_key == "H" else amount
