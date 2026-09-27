# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Catálogo de Dados no Unity Catalog
# MAGIC
# MAGIC O catálogo é definido **uma única vez** no arquivo `catalogo_def.py` (fonte da verdade) e este notebook o aplica no Unity Catalog:
# MAGIC
# MAGIC 1. **Confere** se toda coluna existente nas tabelas está documentada e se o tipo documentado bate com o tipo real.
# MAGIC 2. **Mede** o domínio observado (mínimo e máximo reais) das colunas numéricas e de data.
# MAGIC 3. **Grava** descrição de tabela e de cada coluna (descrição + domínio esperado + domínio observado + linhagem) com `COMMENT`.
# MAGIC 4. **Marca** as tabelas com tags (`camada`, `tipo_tabela`, `fonte`).
# MAGIC 5. **Cria** chaves primárias e estrangeiras informativas, o que habilita o diagrama entidade-relacionamento no Catalog Explorer.
# MAGIC
# MAGIC A linhagem entre tabelas (Bronze → Silver → Gold) é capturada automaticamente pelo Unity Catalog e pode ser vista na aba **Lineage** de cada tabela.

# COMMAND ----------

from pyspark.sql import functions as F
from config import *
from catalogo_def import CATALOGO

catalogo = preparar_ambiente(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 · Conferência: documentação × tabelas reais

# COMMAND ----------

TIPOS_EQUIVALENTES = {"long": "bigint", "integer": "int"}
problemas, domínios = [], []

for tabela, meta in CATALOGO.items():
    df = spark.table(tabela)
    reais = {c: TIPOS_EQUIVALENTES.get(t, t) for c, t in df.dtypes}
    documentadas = {c[0]: c[1] for c in meta["colunas"]}
    for c in reais.keys() - documentadas.keys():
        problemas.append((tabela, c, "coluna sem documentação"))
    for c in documentadas.keys() - reais.keys():
        problemas.append((tabela, c, "documentada mas inexistente"))
    for c in reais.keys() & documentadas.keys():
        if reais[c] != documentadas[c]:
            problemas.append((tabela, c, f"tipo real {reais[c]} ≠ documentado {documentadas[c]}"))

    # 2 · domínio observado
    medir = [c for c, t in reais.items() if t in ("int", "bigint", "double", "date") and not c.startswith("_")]
    if medir:
        linha = df.agg(*[F.min(c).alias(f"{c}__min") for c in medir], *[F.max(c).alias(f"{c}__max") for c in medir]).collect()[0]
        for c in medir:
            domínios.append((tabela, c, str(linha[f"{c}__min"]), str(linha[f"{c}__max"])))
    for c, t in reais.items():
        if t == "boolean":
            n = df.where(F.col(c)).count()
            domínios.append((tabela, c, f"{n} linhas verdadeiras", None))
        if t == "string" and not c.startswith("_"):
            valores = df.where(F.col(c).isNotNull()).select(c).distinct()
            n = valores.count()
            dominio = ", ".join(str(r[0]) for r in valores.orderBy(c).collect()) if n <= 100 else f"{n} valores distintos (texto livre/identificadores)"
            domínios.append((tabela, c, dominio, None))

print("Problemas de documentação:", problemas if problemas else "nenhum ✔")
if problemas:
    raise ValueError(f"Catálogo desatualizado em relação às tabelas: {problemas}")
observado = {(t, c): (mn, mx) for t, c, mn, mx in domínios}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2–5 · Aplicando descrições, tags e constraints

# COMMAND ----------

def texto_dominio(tabela, coluna, esperado):
    mn, mx = observado.get((tabela, coluna), (None, None))
    if mn is None:
        return f"Domínio esperado: {esperado}."
    obs = mn if mx is None else f"{mn} a {mx}"
    return f"Domínio esperado: {esperado}. Observado: {obs}."

if EM_DATABRICKS:
    for tabela, meta in CATALOGO.items():
        camada = tabela.split(".")[0]
        spark.sql(f"COMMENT ON TABLE {tabela} IS '{escapar(meta['descricao'])}'")
        for nome, tipo, desc, dominio, linhagem in meta["colunas"]:
            comentario = f"{desc} {texto_dominio(tabela, nome, dominio)} Linhagem: {linhagem}."
            spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN `{nome}` COMMENT '{escapar(comentario)}'")
        try:
            spark.sql(f"ALTER TABLE {tabela} SET TAGS ('camada' = '{camada}', 'tipo_tabela' = '{meta['tipo']}', 'fonte' = 'ergast_kaggle')")
        except Exception as e:
            print(f"Tags não aplicadas em {tabela}: {type(e).__name__}")
    print("✔ Descrições e tags aplicadas")
else:
    print("Execução local: comentários/tags só são aplicados no Databricks.")

# COMMAND ----------

def aplicar_constraints():
    # Primeiro as PKs (as FKs dependem delas)
    for tabela, meta in CATALOGO.items():
        if not tabela.startswith("gold."):
            continue
        nome_curto = tabela.split(".")[1]
        for c in meta["pk"]:
            spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {c} SET NOT NULL")
        spark.sql(f"ALTER TABLE {tabela} DROP CONSTRAINT IF EXISTS pk_{nome_curto} CASCADE")
        spark.sql(f"ALTER TABLE {tabela} ADD CONSTRAINT pk_{nome_curto} PRIMARY KEY ({', '.join(meta['pk'])})")
    for tabela, meta in CATALOGO.items():
        if not tabela.startswith("gold."):
            continue
        nome_curto = tabela.split(".")[1]
        for coluna, destino in meta["fk"].items():
            nome_fk = f"fk_{nome_curto}_{coluna}"
            spark.sql(f"ALTER TABLE {tabela} DROP CONSTRAINT IF EXISTS {nome_fk}")
            spark.sql(f"ALTER TABLE {tabela} ADD CONSTRAINT {nome_fk} FOREIGN KEY ({coluna}) "
                      f"REFERENCES {destino} ({CATALOGO[destino]['pk'][0]})")

if EM_DATABRICKS:
    try:
        aplicar_constraints()
        print("✔ Chaves primárias e estrangeiras criadas")
    except Exception as e:
        print("Constraints não aplicadas (recurso indisponível nesta conta):", str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catálogo resultante (evidência)
# MAGIC No Databricks, a mesma informação aparece em **Catalog Explorer → tabela → Overview** (descrições por coluna), **Details** (tags e constraints) e **Lineage**.

# COMMAND ----------

linhas = []
for tabela, meta in CATALOGO.items():
    for nome, tipo, desc, dominio, linhagem in meta["colunas"]:
        mn, mx = observado.get((tabela, nome), (None, None))
        linhas.append((tabela, nome, tipo, desc, dominio, mn if mx is None else f"{mn} a {mx}", linhagem))
catalogo_df = spark.createDataFrame(linhas, "tabela string, coluna string, tipo string, descricao string, "
                                            "dominio_esperado string, dominio_observado string, linhagem string")
salvar_tabela(catalogo_df, "qualidade.catalogo_dados", "Transcrição do catálogo de dados (Silver e Gold).", spark)
display(catalogo_df.where(F.col("tabela").startswith("gold.")))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta ao information_schema do Unity Catalog: comprova que as descrições foram gravadas
# MAGIC SELECT table_name, column_name, data_type, comment
# MAGIC FROM information_schema.columns
# MAGIC WHERE table_schema = 'gold'
# MAGIC ORDER BY table_name, ordinal_position
