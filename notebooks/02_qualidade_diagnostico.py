# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Qualidade de Dados – Diagnóstico da Bronze
# MAGIC
# MAGIC Antes de transformar, é preciso **medir** os problemas. Este notebook avalia os dados brutos nas cinco dimensões pedidas no MVP:
# MAGIC
# MAGIC | Dimensão | Pergunta | Como é verificada aqui |
# MAGIC |---|---|---|
# MAGIC | Completude | Há nulos ou vazios? Em que proporção? | Perfil de **todas as colunas** de todas as tabelas Bronze (Parte 1) |
# MAGIC | Consistência | Os valores seguem um padrão? | Formatos de data e tempo, grafias diferentes para o mesmo valor |
# MAGIC | Unicidade | Existem duplicatas? | Chaves primárias e combinações de negócio |
# MAGIC | Acurácia | Os valores fazem sentido? | Faixas válidas (latitude, ano, grid, pontos, idade) e integridade referencial |
# MAGIC | Outliers | Existem extremos que distorcem análises? | Distribuição da duração dos pit stops (IQR) |
# MAGIC
# MAGIC Os resultados são gravados em `qualidade.perfil_bronze` e `qualidade.regras_bronze` e orientam as transformações do notebook 03.

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F, DataFrame
from config import *

catalogo = preparar_ambiente(spark)

NULO_FONTE = "\\N"  # marcador de nulo usado pela Ergast

def num(coluna, tipo="double"):
    """Conversão tolerante: devolve NULL quando o texto não é numérico (ex.: \\N)."""
    return F.expr(f"try_cast(`{coluna}` AS {tipo})")

def eh_nulo(coluna):
    """Considera nulo: NULL real, texto vazio ou o marcador \\N da fonte."""
    return F.col(coluna).isNull() | (F.trim(F.col(coluna)) == "") | (F.col(coluna) == NULO_FONTE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1 · Perfil de completude e cardinalidade de todos os atributos

# COMMAND ----------

def perfilar(nome_tabela):
    df = spark.table(f"bronze.{nome_tabela}")
    colunas = [c for c in df.columns if not c.startswith("_")]
    agregacoes = [F.count(F.lit(1)).alias("__total")]
    for c in colunas:
        agregacoes += [
            F.sum(eh_nulo(c).cast("int")).alias(f"{c}__nulos"),
            F.countDistinct(F.when(~eh_nulo(c), F.col(c))).alias(f"{c}__distintos"),
            F.min(F.when(~eh_nulo(c), F.col(c))).alias(f"{c}__min"),
            F.max(F.when(~eh_nulo(c), F.col(c))).alias(f"{c}__max"),
        ]
    r = df.agg(*agregacoes).collect()[0]
    total = r["__total"]
    return [(nome_tabela, i + 1, c, total, (r[f"{c}__nulos"] or 0),
             round(100.0 * (r[f"{c}__nulos"] or 0) / total, 2) if total else 0.0,
             r[f"{c}__distintos"], r[f"{c}__min"], r[f"{c}__max"])
            for i, c in enumerate(colunas)]

linhas = [linha for t in ARQUIVOS_FONTE for linha in perfilar(t)]
perfil = spark.createDataFrame(
    linhas,
    "tabela string, ordem int, coluna string, total_linhas long, qtd_nulos long, pct_nulos double, "
    "qtd_distintos long, menor_valor_texto string, maior_valor_texto string")
salvar_tabela(perfil, "qualidade.perfil_bronze",
              "Perfil de completude/cardinalidade de cada coluna das tabelas Bronze (nulos incluem \\N e vazio).", spark)
display(perfil.orderBy("tabela", "ordem"))

# COMMAND ----------

# MAGIC %md
# MAGIC Colunas com nulos, da maior para a menor proporção (tabelas usadas no modelo):

# COMMAND ----------

TABELAS_DO_MODELO = ["circuits", "constructors", "drivers", "races", "results", "status", "pit_stops"]
display(spark.table("qualidade.perfil_bronze")
        .where(F.col("tabela").isin(TABELAS_DO_MODELO) & (F.col("qtd_nulos") > 0))
        .orderBy(F.desc("pct_nulos")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2 · Regras de consistência, unicidade, acurácia e integridade
# MAGIC
# MAGIC Cada regra devolve a quantidade de linhas que **violam** a condição. `0` significa aprovado.

# COMMAND ----------

b = lambda t: spark.table(f"bronze.{t}")
regras = []

def regra(dimensao, tabela, descricao, df_violacoes, esperado_zero=True):
    qtd = df_violacoes.count()
    status = "OK" if qtd == 0 else ("FALHA" if esperado_zero else "ATENÇÃO")
    regras.append((dimensao, tabela, descricao, qtd, status))

# --- Unicidade das chaves primárias ------------------------------------------------
chaves = {"circuits": ["circuitId"], "constructors": ["constructorId"], "drivers": ["driverId"],
          "races": ["raceId"], "results": ["resultId"], "status": ["statusId"],
          "pit_stops": ["raceId", "driverId", "stop"]}
for t, k in chaves.items():
    regra("Unicidade", t, f"Chave {'+'.join(k)} duplicada",
          b(t).groupBy(*k).count().where("count > 1"))

regra("Unicidade", "results", "Mesmo piloto com mais de um resultado na mesma corrida (raceId+driverId)",
      b("results").groupBy("raceId", "driverId").count().where("count > 1"), esperado_zero=False)
regra("Unicidade", "races", "Mais de uma corrida no mesmo ano+rodada",
      b("races").groupBy("year", "round").count().where("count > 1"))

# --- Consistência de formato -------------------------------------------------------
regra("Consistência", "races", "date fora do padrão AAAA-MM-DD",
      b("races").where(~F.col("date").rlike(r"^\d{4}-\d{2}-\d{2}$")))
regra("Consistência", "drivers", "dob fora do padrão AAAA-MM-DD (ignorando \\N)",
      b("drivers").where(~eh_nulo("dob") & ~F.col("dob").rlike(r"^\d{4}-\d{2}-\d{2}$")))
regra("Consistência", "results", "fastestLapTime fora do padrão M:SS.mmm (ignorando \\N)",
      b("results").where(~eh_nulo("fastestLapTime") & ~F.col("fastestLapTime").rlike(r"^\d+:\d{2}\.\d{3}$")))
regra("Consistência", "pit_stops", "duration em formato M:SS.mmm em vez de segundos (paradas > 60 s)",
      b("pit_stops").where(F.col("duration").contains(":")), esperado_zero=False)
regra("Consistência", "drivers", "nationality com espaços sobrando no início/fim",
      b("drivers").where(F.col("nationality") != F.trim(F.col("nationality"))), esperado_zero=False)
regra("Consistência", "drivers", "Mesma nacionalidade com duas grafias (Argentine x Argentinian)",
      b("drivers").where(F.trim("nationality") == "Argentinian"), esperado_zero=False)
regra("Consistência", "circuits", "País dos EUA com duas grafias (USA x United States)",
      b("circuits").where(F.col("country") == "United States"), esperado_zero=False)
regra("Consistência", "results", "position nula mas positionText numérico (divergência entre colunas)",
      b("results").where(eh_nulo("position") & F.col("positionText").rlike(r"^\d+$")))

# --- Acurácia (faixas válidas) -----------------------------------------------------
regra("Acurácia", "circuits", "Latitude fora de [-90, 90] ou longitude fora de [-180, 180]",
      b("circuits").where((F.abs(num("lat", "double")) > 90) | (F.abs(num("lng", "double")) > 180)))
regra("Acurácia", "races", "Ano fora do intervalo 1950 – ano atual",
      b("races").where((num("year", "int") < 1950) | (num("year", "int") > F.year(F.current_date()))))
regra("Acurácia", "results", "grid negativo",
      b("results").where(num("grid", "int") < 0))
regra("Acurácia", "results", "grid = 0 (largada do pit lane ou sem posição de largada)",
      b("results").where(num("grid", "int") == 0), esperado_zero=False)
regra("Acurácia", "results", "Pontos negativos",
      b("results").where(num("points", "double") < 0))
idade = (b("results").join(b("races").select("raceId", "date"), "raceId")
         .join(b("drivers").select("driverId", "dob"), "driverId")
         .where(~eh_nulo("dob"))
         .withColumn("idade", F.months_between(num("date", "date"), num("dob", "date")) / 12))
regra("Acurácia", "results", "Idade do piloto na corrida fora de [17, 60] anos",
      idade.where((F.col("idade") < 17) | (F.col("idade") > 60)), esperado_zero=False)

# --- Integridade referencial -------------------------------------------------------
regra("Integridade", "results", "driverId inexistente em drivers",
      b("results").join(b("drivers"), "driverId", "left_anti"))
regra("Integridade", "results", "constructorId inexistente em constructors",
      b("results").join(b("constructors"), "constructorId", "left_anti"))
regra("Integridade", "results", "raceId inexistente em races",
      b("results").join(b("races"), "raceId", "left_anti"))
regra("Integridade", "results", "statusId inexistente em status",
      b("results").join(b("status"), "statusId", "left_anti"))
regra("Integridade", "races", "circuitId inexistente em circuits",
      b("races").join(b("circuits"), "circuitId", "left_anti"))
regra("Integridade", "pit_stops", "Pit stop sem resultado correspondente (raceId+driverId)",
      b("pit_stops").join(b("results"), ["raceId", "driverId"], "left_anti"))
regra("Integridade", "races", "Corrida do calendário sem nenhum resultado (ex.: corridas futuras)",
      b("races").join(b("results"), "raceId", "left_anti"), esperado_zero=False)

df_regras = spark.createDataFrame(regras, "dimensao string, tabela string, regra string, qtd_violacoes long, status string")
salvar_tabela(df_regras, "qualidade.regras_bronze", "Resultado das regras de qualidade aplicadas sobre a Bronze.", spark)
display(df_regras)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 3 · Outliers – duração dos pit stops
# MAGIC
# MAGIC A duração registrada pela fonte é o tempo total no pit lane (entrada até saída), não só o tempo parado. Paradas durante bandeira vermelha ou reparos longos aparecem com minutos de duração e distorcem médias.

# COMMAND ----------

ps = b("pit_stops").select(num("milliseconds", "long").alias("ms")).where(F.col("ms").isNotNull())
quantis = ps.approxQuantile("ms", [0.25, 0.5, 0.75], 0.001)
if len(quantis) != 3:
    raise ValueError("Não há durações válidas para diagnosticar pit stops")
q1, mediana, q3 = quantis
limite_iqr = q3 + 1.5 * (q3 - q1)
estat = ps.agg(F.count("*").alias("paradas"), F.min("ms").alias("min_ms"), F.max("ms").alias("max_ms"),
               F.round(F.avg("ms")).alias("media_ms"),
               F.sum((F.col("ms") > limite_iqr).cast("int")).alias("acima_limite_iqr"),
               F.sum((F.col("ms") > 60000).cast("int")).alias("acima_60s"))
print(f"Q1={q1:.0f} ms | mediana={mediana:.0f} ms | Q3={q3:.0f} ms | limite IQR (Q3+1,5·IQR)={limite_iqr:.0f} ms")
display(estat)

# COMMAND ----------

# Distribuição por faixa de duração (evidência visual do outlier)
display(ps.withColumn("faixa", F.when(F.col("ms") < 20000, "1. < 20 s")
                                 .when(F.col("ms") < 30000, "2. 20–30 s")
                                 .when(F.col("ms") < 40000, "3. 30–40 s")
                                 .when(F.col("ms") < 60000, "4. 40–60 s")
                                 .when(F.col("ms") < 600000, "5. 1–10 min")
                                 .otherwise("6. > 10 min"))
          .groupBy("faixa").agg(F.count("*").alias("paradas")).orderBy("faixa"))
