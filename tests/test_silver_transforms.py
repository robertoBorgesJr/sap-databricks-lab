"""
Testes unitários para src/silver/transforms.py.

Rodam localmente com pytest, sem dependência de Spark.
Cada classe de teste cobre uma função e os casos específicos das
armadilhas SAP que o notebook silver deve tratar.
"""

from datetime import date, time
from decimal import Decimal

import pytest

from src.silver.transforms import (
    alpha_strip,
    empty_to_none,
    flag_to_bool,
    invert_gdatu,
    parse_dats,
    parse_decimal,
    parse_tims,
    signed_amount,
)


# ---------------------------------------------------------------------------
# parse_dats — DATS (YYYYMMDD) → date
# ---------------------------------------------------------------------------

class TestParseDats:
    def test_valid_date(self):
        assert parse_dats("20231015") == date(2023, 10, 15)

    def test_zero_date_returns_none(self):
        # '00000000' é o valor SAP para "data não preenchida"
        assert parse_dats("00000000") is None

    def test_empty_string_returns_none(self):
        assert parse_dats("") is None

    def test_whitespace_returns_none(self):
        assert parse_dats("   ") is None

    def test_none_returns_none(self):
        assert parse_dats(None) is None

    def test_date_with_leading_whitespace(self):
        # CHAR fields podem chegar com espaços
        assert parse_dats("  20200101") == date(2020, 1, 1)

    def test_jan_first(self):
        assert parse_dats("20150101") == date(2015, 1, 1)

    def test_dec_thirty_first(self):
        assert parse_dats("20241231") == date(2024, 12, 31)

    def test_invalid_date_returns_none(self):
        # mês 99 é inválido — não deve explodir, deve retornar None
        assert parse_dats("99999999") is None


# ---------------------------------------------------------------------------
# parse_tims — TIMS (HHMMSS) → time
# ---------------------------------------------------------------------------

class TestParseTims:
    def test_valid_time(self):
        assert parse_tims("143022") == time(14, 30, 22)

    def test_midnight(self):
        assert parse_tims("000000") == time(0, 0, 0)

    def test_empty_returns_none(self):
        assert parse_tims("") is None

    def test_too_short_returns_none(self):
        assert parse_tims("1430") is None

    def test_none_returns_none(self):
        assert parse_tims(None) is None

    def test_trimmed(self):
        assert parse_tims("  143022") == time(14, 30, 22)


# ---------------------------------------------------------------------------
# parse_decimal — string decimal SAP → Decimal
# ---------------------------------------------------------------------------

class TestParseDecimal:
    def test_period_separator(self):
        assert parse_decimal("1500.00") == Decimal("1500.00")

    def test_comma_separator(self):
        # Extração legada usa vírgula como separador decimal
        assert parse_decimal("1500,00") == Decimal("1500.00")

    def test_empty_returns_none(self):
        assert parse_decimal("") is None

    def test_none_returns_none(self):
        assert parse_decimal(None) is None

    def test_whitespace_returns_none(self):
        assert parse_decimal("   ") is None

    def test_trimmed(self):
        # UKURS com trailing spaces ('5.12300  ')
        assert parse_decimal("  5.12300  ") == Decimal("5.12300")

    def test_zero_is_valid(self):
        # '0.00' é um valor válido (ordem migrada sem valor), não None
        assert parse_decimal("0.00") == Decimal("0.00")

    def test_non_numeric_returns_none(self):
        assert parse_decimal("abc") is None

    def test_weight_with_three_decimals(self):
        assert parse_decimal("123.456") == Decimal("123.456")

    def test_comma_weight(self):
        # BRGEW/NTGEW com vírgula de Excel
        assert parse_decimal("123,456") == Decimal("123.456")


# ---------------------------------------------------------------------------
# flag_to_bool — flag SAP X/espaço → bool
# ---------------------------------------------------------------------------

class TestFlagToBool:
    def test_x_is_true(self):
        assert flag_to_bool("X") is True

    def test_empty_is_false(self):
        assert flag_to_bool("") is False

    def test_space_is_false(self):
        # Armadilha clássica SAP: campo LOEVM/LVORM com espaço ' '
        # deve ser tratado como não-excluído, não como True
        assert flag_to_bool(" ") is False

    def test_none_is_false(self):
        assert flag_to_bool(None) is False

    def test_x_with_trailing_space(self):
        # 'X ' após trim → 'X' → True
        assert flag_to_bool("X ") is True

    def test_lowercase_x_is_false(self):
        # SAP usa 'X' maiúsculo; 'x' não é uma flag válida
        assert flag_to_bool("x") is False


# ---------------------------------------------------------------------------
# empty_to_none — string em branco → None
# ---------------------------------------------------------------------------

class TestEmptyToNone:
    def test_non_empty_preserved(self):
        assert empty_to_none("SILVA") == "SILVA"

    def test_empty_returns_none(self):
        assert empty_to_none("") is None

    def test_spaces_return_none(self):
        # NAME1 com 35 espaços (campo CHAR SAP preenchido com blanks)
        assert empty_to_none("   ") is None

    def test_trimmed_and_returned(self):
        assert empty_to_none("  SILVA  ") == "SILVA"

    def test_none_returns_none(self):
        assert empty_to_none(None) is None

    def test_single_space_returns_none(self):
        # Armadilha mais comum: campo com um único espaço
        assert empty_to_none(" ") is None


# ---------------------------------------------------------------------------
# alpha_strip — conversão ALPHA SAP
# ---------------------------------------------------------------------------

class TestAlphaStrip:
    def test_numeric_strips_leading_zeros(self):
        # KUNNR numérico
        assert alpha_strip("0000001234") == "1234"

    def test_all_zeros_keeps_one_zero(self):
        # '0000000000' não deve virar string vazia
        assert alpha_strip("0000000000") == "0"

    def test_alphanumeric_only_trims(self):
        # Artefato de migração 'ALT1234' — zeros não são removidos
        assert alpha_strip("ALT1234") == "ALT1234"

    def test_alphanumeric_trims_trailing_spaces(self):
        assert alpha_strip("ALT1234   ") == "ALT1234"

    def test_empty_returns_none(self):
        assert alpha_strip("") is None

    def test_none_returns_none(self):
        assert alpha_strip(None) is None

    def test_already_stripped(self):
        assert alpha_strip("1234") == "1234"

    def test_matnr_18_chars(self):
        # MATNR numérico 18 chars
        assert alpha_strip("000000000000000001") == "1"

    def test_matnr_alphanumeric(self):
        # MATNR alfanumérico como 'MAT-1234'
        assert alpha_strip("MAT-1234") == "MAT-1234"

    def test_numeric_with_trailing_spaces(self):
        # Campo CHAR com trailing spaces + zeros à esquerda
        assert alpha_strip("0000001234  ") == "1234"


# ---------------------------------------------------------------------------
# invert_gdatu — GDATU invertido SAP → date
# ---------------------------------------------------------------------------

class TestInvertGdatu:
    def test_known_date(self):
        # 99999999 - 20240115 = 79759884
        assert invert_gdatu("79759884") == date(2024, 1, 15)

    def test_open_ended_returns_none(self):
        # '99999999' = taxa sem data de fim (open-ended rate)
        assert invert_gdatu("99999999") is None

    def test_zero_returns_none(self):
        assert invert_gdatu("00000000") is None

    def test_none_returns_none(self):
        assert invert_gdatu(None) is None

    def test_empty_returns_none(self):
        assert invert_gdatu("") is None

    def test_jan_2015(self):
        # 99999999 - 20150101 = 79849898
        assert invert_gdatu("79849898") == date(2015, 1, 1)

    def test_dec_2024(self):
        # 99999999 - 20241231 = 79758768
        assert invert_gdatu("79758768") == date(2024, 12, 31)


# ---------------------------------------------------------------------------
# signed_amount — BSEG DMBTR + SHKZG → valor com sinal
# ---------------------------------------------------------------------------

class TestSignedAmount:
    def test_debit_is_positive(self):
        # SHKZG='S' (Soll = débito) → positivo
        assert signed_amount("1500.00", "S") == Decimal("1500.00")

    def test_credit_is_negative(self):
        # SHKZG='H' (Haben = crédito) → negativo
        assert signed_amount("1500.00", "H") == Decimal("-1500.00")

    def test_shkzg_with_trailing_space(self):
        # Armadilha BSEG: 'S ' e 'H ' devem funcionar após trim
        assert signed_amount("100.00", "S ") == Decimal("100.00")
        assert signed_amount("100.00", "H ") == Decimal("-100.00")

    def test_comma_separator_in_dmbtr(self):
        # Extração legada com vírgula em DMBTR
        assert signed_amount("1500,00", "S") == Decimal("1500.00")
        assert signed_amount("1500,00", "H") == Decimal("-1500.00")

    def test_empty_value_returns_none(self):
        assert signed_amount("", "S") is None

    def test_none_value_returns_none(self):
        assert signed_amount(None, "S") is None

    def test_none_shkzg_defaults_to_positive(self):
        # shkzg None → trata como não-H → positivo (defensivo)
        assert signed_amount("100.00", None) == Decimal("100.00")

    def test_balancing_entry(self):
        # Total débitos = total créditos (invariante contábil SAP)
        debit  = signed_amount("3000.00", "S")
        credit = signed_amount("3000.00", "H")
        assert debit + credit == Decimal("0")
