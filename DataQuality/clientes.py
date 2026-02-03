import re
from datetime import datetime, date
from collections import Counter


TELEFONE_REGEX = r"^\(\d{2}\)\d{5}-\d{4}$"
DATA_MAX_FUTURA = date.today()



# FUNÇÕES DE QUALIDADE
def validar_relatorio(dados, campos_obrigatorios):
    erros = []
    for idx, r in enumerate(dados):
        for campo in campos_obrigatorios:
            if campo not in r or r[campo] in (None, "", []):
                erros.append(
                    f"Registro {idx} com campo obrigatório ausente ou nulo: {campo}"
                )
    return erros


def validar_unicidade(dados, campo_chave):
    valores = [r[campo_chave] for r in dados]
    duplicados = [item for item, count in Counter(valores).items() if count > 1]
    return duplicados


def validar_telefone(dados):
    erros = []
    for idx, r in enumerate(dados):
        telefone = r.get("num_telefone_cliente")
        if telefone is not None and not re.match(TELEFONE_REGEX, telefone):
            erros.append(
                f"Registro {idx} com telefone inválido: {telefone}"
            )
    return erros


def validar_datas(dados):
    erros = []
    for idx, r in enumerate(dados):
        try:
            dt_atualizacao = datetime.strptime(
                r["dt_atualizacao"], "%Y-%m-%d"
            ).date()

            if dt_atualizacao > DATA_MAX_FUTURA:
                erros.append(
                    f"Registro {idx} com data futura inválida: {dt_atualizacao}"
                )

        except Exception:
            erros.append(
                f"Registro {idx} com dt_atualizacao inválida ou mal formatada"
            )
    return erros


def validar_faixa_renda(dados, renda_min=0, renda_max=1_000_000):
    erros = []
    for idx, r in enumerate(dados):
        try:
            renda = float(r.get("vl_renda", 0))
            if renda < renda_min or renda > renda_max:
                erros.append(
                    f"Registro {idx} com renda fora da faixa esperada: {renda}"
                )
        except Exception:
            erros.append(
                f"Registro {idx} com renda inválida"
            )
    return erros



def get_data_quality(dados):
    relatorio = {}

    relatorio["completude"] = validar_relatorio(
        dados,
        campos_obrigatorios=[
            "cod_cliente",
            "nm_cliente",
            "dt_atualizacao"
        ]
    )

    relatorio["unicidade"] = validar_unicidade(
        dados,
        campo_chave="cod_cliente"
    )

    relatorio["telefone"] = validar_telefone(dados)
    relatorio["datas"] = validar_datas(dados)
    relatorio["renda"] = validar_faixa_renda(dados)
    return relatorio



if __name__ == "__main__":
    dados_silver = [
        {
            "cod_cliente": "1",
            "nm_cliente": "LAURA XAVIER",
            "num_telefone_cliente": "(11)99999-9999",
            "dt_atualizacao": "2024-02-01",
            "vl_renda": "6000"
        },
        {
            "cod_cliente": "2",
            "nm_cliente": "CARLOS FERREIRA",
            "num_telefone_cliente": None,
            "dt_atualizacao": "2024-01-10",
            "vl_renda": "7000"
        }
    ]

    resultado = get_data_quality(dados_silver)

    print("=== RELATÓRIO DE DATA QUALITY ===")
    for regra, erros in resultado.items():
        if erros:
            print(f"[ERRO] {regra}:")
            for e in erros:
                print(" -", e)
        else:
            print(f"[OK] {regra}")
