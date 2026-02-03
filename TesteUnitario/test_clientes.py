import pytest
import re
from datetime import datetime, date
from collections import Counter


@pytest.fixture
def sample_data():
    # Dados duplicados para o cliente 1, com atualizações diferentes, e um cliente 2 com telefone inválido
    return [
        {
            "cod_cliente": "1",
            "nm_cliente": "Laura Xavier",
            "telefone_cliente": "(11)99999-9999",
            "dt_nascimento_cliente": "1990-01-01",
            "dt_atualizacao": "2024-01-01",
            "vl_renda": "5000",
        },
        {
            "cod_cliente": "1",
            "nm_cliente": "Laura Xavier",
            "telefone_cliente": "(11)99999-9999",
            "dt_nascimento_cliente": "1990-01-01",
            "dt_atualizacao": "2024-02-01",
            "vl_renda": "6000",
        },
        {
            "cod_cliente": "2",
            "nm_cliente": "Carlos Ferreira",
            "telefone_cliente": "11999999999",
            "dt_nascimento_cliente": "1985-05-10",
            "dt_atualizacao": "2024-01-10",
            "vl_renda": "7000",
        },
    ]


def validar_telefone(valor):
    if valor and re.match(r"^\(\d{2}\)\d{5}-\d{4}$", valor):
        return valor
    return None

def transform_silver_python(data):
    # Uppercase
    for row in data:
        row["nm_cliente"] = row["nm_cliente"].upper()

    # Deduplicação
    dedup = {}
    for row in data:
        cod = row["cod_cliente"]
        dt = datetime.strptime(row["dt_atualizacao"], "%Y-%m-%d")
        if cod not in dedup or dt > dedup[cod]["_dt"]:
            row["_dt"] = dt
            dedup[cod] = row

    # Tratamento telefone + remoção campo antigo
    result = []
    for row in dedup.values():
        new_row = row.copy()
        new_row["num_telefone_cliente"] = validar_telefone(row["telefone_cliente"])
        new_row.pop("telefone_cliente", None)
        new_row.pop("_dt", None)
        result.append(new_row)

    return result


# TESTES CAMADA SILVER
def test_uppercase_nome(sample_data):
    result = transform_silver_python(sample_data)
    assert all(r["nm_cliente"].isupper() for r in result)


def test_deduplicacao_por_cliente(sample_data):
    result = transform_silver_python(sample_data)
    cods = [r["cod_cliente"] for r in result]
    assert cods.count("1") == 1
    assert len(result) == 2


def test_validacao_telefone(sample_data):
    result = transform_silver_python(sample_data)
    telefone_map = {r["cod_cliente"]: r["num_telefone_cliente"] for r in result}
    assert telefone_map["1"] == "(11)99999-9999"
    assert telefone_map["2"] is None


def test_coluna_telefone_removida(sample_data):
    result = transform_silver_python(sample_data)

    assert all("telefone_cliente" not in r for r in result)


# TESTES ANALÍTICOS
def test_cliente_com_mais_atualizacoes(sample_data):
    cods = [r["cod_cliente"] for r in sample_data]
    counter = Counter(cods)

    cliente, qtd = counter.most_common(1)[0]

    assert cliente == "1"
    assert qtd == 2


def test_media_idade_clientes(sample_data):
    hoje = date.today()

    idades = []
    for r in sample_data:
        if r["dt_nascimento_cliente"]:
            nasc = datetime.strptime(r["dt_nascimento_cliente"], "%Y-%m-%d").date()
            idade = hoje.year - nasc.year
            idades.append(idade)

    media = sum(idades) / len(idades)

    assert media > 0
