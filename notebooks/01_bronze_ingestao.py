# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze – Ingestão dos dados brutos
# MAGIC
# MAGIC **ETL 1 de 3 (Extract → Load):** lê os 14 CSVs do volume e grava cada um como uma tabela Delta no schema `bronze`.
# MAGIC
# MAGIC Regras desta camada:
# MAGIC - **Nenhuma transformação de conteúdo.** Todas as colunas são lidas como texto (`inferSchema = false`), então o marcador de nulo `\N` usado pela fonte fica preservado exatamente como veio.
# MAGIC - Os nomes das tabelas e colunas são os originais da fonte.
# MAGIC - São adicionados três metadados de controle, para rastreabilidade:
# MAGIC
# MAGIC | Coluna | Conteúdo |
# MAGIC |---|---|
# MAGIC | `_arquivo_origem` | Caminho do CSV no volume |
# MAGIC | `_ingestao_ts` | Data e hora da ingestão |
# MAGIC | `_fonte` | Identificação do dataset de origem |

# COMMAND ----------

from pyspark.sql import functions as F
from config import *

catalogo = preparar_ambiente(spark)
pasta = caminho_arquivos(catalogo)
print("Lendo arquivos de:", pasta)

# COMMAND ----------

def ingerir_csv(nome_arquivo):
    """Lê um CSV sem inferir tipos e acrescenta os metadados de ingestão."""
    df = (spark.read
          .option("header", True)
          .option("inferSchema", False)   # tudo como string: a Bronze não interpreta os dados
          .option("encoding", "UTF-8")
          .option("mode", "FAILFAST")
          .csv(f"{pasta}/{nome_arquivo}.csv"))
    origem = F.col("_metadata.file_path") if EM_DATABRICKS else F.input_file_name()
    return (df.select("*", origem.alias("_arquivo_origem"))
              .withColumn("_ingestao_ts", F.current_timestamp())
              .withColumn("_fonte", F.lit(FONTE_DADOS)))


resumo = []
for nome in ARQUIVOS_FONTE:
    df = ingerir_csv(nome)
    tabela = salvar_tabela(
        df, f"bronze.{nome}",
        comentario=f"Bronze: cópia fiel do arquivo {nome}.csv ({FONTE_DADOS}). Todas as colunas em texto; nulos representados por \\N.",
        spark=spark,
    )
    qtd_colunas = len([c for c in df.columns if not c.startswith("_")])
    resumo.append((tabela, spark.table(tabela).count(), qtd_colunas))
    print(f"✔ {tabela}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidência da carga: quantidade de linhas e colunas por tabela Bronze

# COMMAND ----------

df_resumo = spark.createDataFrame(resumo, "tabela string, linhas long, colunas_originais int").orderBy("tabela")
display(df_resumo)
salvar_tabela(df_resumo, "qualidade.resumo_bronze", spark=spark)

# COMMAND ----------

# Amostra de uma tabela Bronze, mostrando o \N preservado e os metadados
display(spark.table("bronze.races").select("raceId", "year", "name", "date", "fp1_date", "_arquivo_origem", "_ingestao_ts").limit(5))
