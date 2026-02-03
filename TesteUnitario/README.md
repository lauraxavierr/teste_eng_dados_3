## Testes Unitários


Os testes unitários foram implementados em Python usando pytest, focando na validação das regras de negócio do ETL como:
- deduplicação
- padronização de dados
- validação de telefone

A execução com Spark foi tratada como teste de integração e não incluída nos testes unitários, garantindo que os testes sejam independentes de infraestrutura.
Todos os testes foram executados localmente com sucesso.

## Execução Local
```Python
python -m pytest test_clientes.py
```