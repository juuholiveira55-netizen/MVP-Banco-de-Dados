# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Execução do pipeline completo
# MAGIC
# MAGIC Roda todos os notebooks em sequência, na ordem das dependências. Pré-requisito: `00_setup` executado e os 14 CSVs enviados ao volume.
# MAGIC
# MAGIC ```
# MAGIC 01 Bronze  →  02 Qualidade (diagnóstico)  →  03 Silver  →  04 Gold  →  05 Catálogo  →  06 Análise
# MAGIC ```
# MAGIC
# MAGIC Alternativa: em **Jobs & Pipelines → Create job**, crie uma tarefa por notebook com as mesmas dependências. O job fica agendável e gera um gráfico de execução (bom para screenshot).

# COMMAND ----------

import time

ETAPAS = [
    "./01_bronze_ingestao",
    "./02_qualidade_diagnostico",
    "./03_silver_transformacao",
    "./04_gold_modelagem",
    "./05_catalogo_dados",
    "./06_analise",
]

resumo = []
for etapa in ETAPAS:
    inicio = time.time()
    print(f"▶ {etapa} ...")
    dbutils.notebook.run(etapa, 3600)
    resumo.append((etapa, round(time.time() - inicio, 1)))
    print(f"  ✔ concluída em {resumo[-1][1]} s")

display(spark.createDataFrame(resumo, "notebook string, segundos double"))
