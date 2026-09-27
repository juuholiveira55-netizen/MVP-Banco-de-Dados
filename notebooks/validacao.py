"""Validações executáveis de chaves e relacionamentos, antes das análises."""
from functools import reduce
from pyspark.sql import functions as F


def validar_modelo(spark, catalogo, camada):
    regras = []
    for tabela, meta in catalogo.items():
        if not tabela.startswith(camada + "."):
            continue
        df = spark.table(tabela)
        pk = meta["pk"]
        nulos = reduce(lambda a, b: a | b, (F.col(c).isNull() for c in pk))
        regras.append((f"{tabela}: PK não nula", df.where(nulos).count()))
        regras.append((f"{tabela}: PK única", df.groupBy(*pk).count().where("count > 1").count()))
        for coluna, destino in meta.get("fk", {}).items():
            chave = catalogo[destino]["pk"][0]
            pais = spark.table(destino).select(F.col(chave).alias(coluna)).distinct()
            regras.append((f"{tabela}.{coluna}: referência obrigatória em {destino}",
                           df.join(pais, coluna, "left_anti").count()))
    return regras


def deduplicar_sem_conflitos(df, chave, nome):
    """Remove apenas linhas iguais; PK repetida com conteúdo distinto é erro."""
    nulos = reduce(lambda a, b: a | b, (F.col(c).isNull() for c in chave))
    if df.where(nulos).limit(1).count():
        raise ValueError(f"{nome}: chave nula ou conversão de chave inválida")
    unicos = df.dropDuplicates()
    if unicos.groupBy(*chave).count().where("count > 1").limit(1).count():
        raise ValueError(f"{nome}: mesma chave com conteúdos diferentes; corrija a fonte")
    return unicos
