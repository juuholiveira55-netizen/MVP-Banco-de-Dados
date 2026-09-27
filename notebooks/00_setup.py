# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do ambiente
# MAGIC
# MAGIC **MVP – Pipeline de Dados na Nuvem: Fórmula 1 (1950–2024)**
# MAGIC
# MAGIC Este notebook prepara a estrutura do Lakehouse no Unity Catalog:
# MAGIC
# MAGIC | Objeto | Nome | Função |
# MAGIC |---|---|---|
# MAGIC | Catálogo | `f1_mvp` (ou `workspace`, se não houver permissão) | Agrupa todo o projeto |
# MAGIC | Schema | `bronze` | Dados brutos, como vieram da fonte |
# MAGIC | Schema | `silver` | Dados limpos e tipados |
# MAGIC | Schema | `gold` | Modelo estrela para análise |
# MAGIC | Schema | `qualidade` | Resultados das checagens de qualidade |
# MAGIC | Volume | `bronze.arquivos_brutos` | Onde os 14 CSVs do Kaggle são enviados |
# MAGIC
# MAGIC Depois de rodar este notebook, envie os CSVs para o volume (instruções na última célula).

# COMMAND ----------

from config import *

catalogo = preparar_ambiente(spark)
print(f"Catálogo em uso: {catalogo}")

# COMMAND ----------

# Volume gerenciado do Unity Catalog para os arquivos brutos
if EM_DATABRICKS:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {catalogo}.bronze.{VOLUME}
        COMMENT 'Arquivos CSV originais do dataset Formula 1 World Championship (Kaggle / Ergast)'
    """)
    dbutils.fs.mkdirs(caminho_arquivos(catalogo))

print("Pasta de destino dos CSVs:", caminho_arquivos(catalogo))

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalogo}") if EM_DATABRICKS else spark.sql("SHOW DATABASES"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximo passo: enviar os CSVs
# MAGIC
# MAGIC 1. Baixe o dataset em https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020 (botão **Download**) e descompacte o zip.
# MAGIC 2. No Databricks, abra **Catalog → f1_mvp → bronze → Volumes → arquivos_brutos**.
# MAGIC 3. Crie/abra a pasta **f1** e clique em **Upload to this volume**. Selecione os 14 arquivos `.csv`.
# MAGIC 4. Rode a célula abaixo para conferir se todos chegaram.

# COMMAND ----------

if EM_DATABRICKS:
    presentes = {f.name.replace(".csv", "") for f in dbutils.fs.ls(caminho_arquivos(catalogo))}
else:
    presentes = {f.replace(".csv", "") for f in os.listdir(caminho_arquivos(catalogo))}

faltando = sorted(set(ARQUIVOS_FONTE) - presentes)
print(f"{len(set(ARQUIVOS_FONTE) & presentes)}/{len(ARQUIVOS_FONTE)} arquivos encontrados.")
print("Faltando:", faltando if faltando else "nenhum ✔")
if faltando:
    raise FileNotFoundError(f"Envie os CSVs para {caminho_arquivos(catalogo)}: {', '.join(faltando)}")
