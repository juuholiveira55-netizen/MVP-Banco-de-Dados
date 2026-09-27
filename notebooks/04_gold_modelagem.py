# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold – Modelo estrela
# MAGIC
# MAGIC **ETL 3 de 3 (Silver → Gold).** Monta o modelo dimensional usado nas análises.
# MAGIC
# MAGIC ```
# MAGIC                 dim_piloto        dim_equipe
# MAGIC                       \              /
# MAGIC   dim_status ──── fato_resultado ──── dim_corrida
# MAGIC                       |
# MAGIC                  dim_circuito ──── fato_pit_stop ──── (dim_piloto, dim_equipe, dim_corrida)
# MAGIC ```
# MAGIC
# MAGIC | Tabela | Tipo | Grão (uma linha por...) |
# MAGIC |---|---|---|
# MAGIC | `fato_resultado` | Fato | participação de um piloto em uma corrida (resultado) |
# MAGIC | `fato_pit_stop` | Fato | parada de box |
# MAGIC | `dim_corrida` | Dimensão | Grande Prêmio (também faz o papel de dimensão de tempo: ano, década, era) |
# MAGIC | `dim_piloto` | Dimensão | piloto |
# MAGIC | `dim_equipe` | Dimensão | equipe (construtor) |
# MAGIC | `dim_circuito` | Dimensão | circuito |
# MAGIC | `dim_status` | Dimensão | status final do resultado |
# MAGIC
# MAGIC **Por que estrela e não snowflake?** O `circuito_id` foi levado direto para os fatos (em vez de ficar só em `dim_corrida`), assim toda análise por circuito faz apenas um JOIN. As dimensões são pequenas, então a redundância é irrelevante e as consultas ficam mais simples.
# MAGIC
# MAGIC **Métricas derivadas criadas aqui** (não existem na fonte): posições ganhas, pontos no sistema atual, flags de vitória/pódio/pole/abandono, corrida em casa e idade do piloto.

# COMMAND ----------

from pyspark.sql import functions as F
from config import *
from catalogo_def import CATALOGO
from validacao import validar_modelo

catalogo = preparar_ambiente(spark)
sv = lambda t: spark.table(f"silver.{t}")

# Em reexecuções: remove as chaves estrangeiras criadas pelo notebook 05,
# para que as dimensões possam ser sobrescritas sem conflito.
if EM_DATABRICKS:
    for tabela, meta in CATALOGO.items():
        for coluna in meta.get("fk", {}):
            if spark.catalog.tableExists(tabela):
                spark.sql(f"ALTER TABLE {tabela} DROP CONSTRAINT IF EXISTS fk_{tabela.split('.')[1]}_{coluna}")
    for tabela in CATALOGO:
        if tabela.startswith("gold.") and spark.catalog.tableExists(tabela):
            spark.sql(f"ALTER TABLE {tabela} DROP PRIMARY KEY IF EXISTS CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensões

# COMMAND ----------

corridas_com_resultado = sv("resultados").select("corrida_id").distinct().withColumn("possui_resultado", F.lit(True))

dim_corrida = (sv("corridas")
    .join(corridas_com_resultado, "corrida_id", "left")
    .select(
        "corrida_id", "ano", "rodada", "nome_gp", "data", "circuito_id",
        F.concat((F.floor(F.col("ano") / 10) * 10).cast("string"), F.lit("s")).alias("decada"),
        F.when(F.col("ano") < 1961, "1. Pioneira (1950-60)")
         .when(F.col("ano") < 1983, "2. Motor traseiro e aerodinâmica (1961-82)")
         .when(F.col("ano") < 1995, "3. Turbo e eletrônica (1983-94)")
         .when(F.col("ano") < 2006, "4. V10 e guerra de pneus (1995-2005)")
         .when(F.col("ano") < 2014, "5. V8 (2006-13)")
         .when(F.col("ano") < 2022, "6. Híbrida (2014-21)")
         .otherwise("7. Efeito solo (2022+)").alias("era"),
        F.col("data_sprint").isNotNull().alias("tem_sprint"),
        (F.col("nome_gp") == "Indianapolis 500").alias("eh_indy500"),
        F.coalesce(F.col("possui_resultado"), F.lit(False)).alias("possui_resultado")))
salvar_tabela(dim_corrida, "gold.dim_corrida", spark=spark)

dim_piloto = (sv("pilotos").join(sv("ref_nacionalidade_pais"), "nacionalidade", "left")
    .select("piloto_id", "nome_completo", "codigo", "numero_permanente", "data_nascimento",
            "nacionalidade", F.col("pais").alias("pais_nacionalidade")))
salvar_tabela(dim_piloto, "gold.dim_piloto", spark=spark)

dim_equipe = sv("equipes").select("equipe_id", "nome", "nacionalidade")
salvar_tabela(dim_equipe, "gold.dim_equipe", spark=spark)

dim_circuito = sv("circuitos").select("circuito_id", "nome", "cidade", "pais", "latitude", "longitude", "altitude_m")
salvar_tabela(dim_circuito, "gold.dim_circuito", spark=spark)

dim_status = sv("status").select("status_id", "descricao", "categoria")
salvar_tabela(dim_status, "gold.dim_status", spark=spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fato: resultado
# MAGIC
# MAGIC Regras das métricas derivadas:
# MAGIC - `grid_largada`: posição de largada; `grid = 0` (largada do pit lane / sem posição) vira `NULL` e `largou_sem_grid = true` quando o status indica largada; não comprova pit lane.
# MAGIC - `posicoes_ganhas = grid_largada − posicao_final`, só para pilotos classificados com grid conhecido.
# MAGIC - `pontos_sistema_atual`: pontos que o resultado valeria no sistema de 2010+ (25-18-15-12-10-8-6-4-2-1), permitindo comparar eras com regras de pontuação diferentes. Não inclui ponto de volta mais rápida nem sprint.
# MAGIC - `largou`: status diferente de "Não largou". `abandonou`: largou e terminou por Mecânica, Acidente/Incidente, Piloto/Segurança ou Abandono não especificado.
# MAGIC - `correu_em_casa`: país do circuito = país da nacionalidade do piloto.

# COMMAND ----------

PONTOS_ATUAIS = F.array(*[F.lit(p) for p in [25, 18, 15, 12, 10, 8, 6, 4, 2, 1]])

fato_resultado = (sv("resultados").alias("r")
    .join(sv("corridas").select("corrida_id", "circuito_id", F.col("data").alias("data_corrida")), "corrida_id")
    .join(sv("circuitos").select("circuito_id", F.col("pais").alias("pais_circuito")), "circuito_id")
    .join(dim_piloto.select("piloto_id", "data_nascimento", "pais_nacionalidade"), "piloto_id")
    .join(sv("status").select("status_id", "categoria"), "status_id")
    .withColumn("grid_largada", F.when(F.col("grid") > 0, F.col("grid")))
    .withColumn("largou", F.col("categoria") != "Não largou")
    .select(
        "resultado_id", "corrida_id", "piloto_id", "equipe_id", "circuito_id", "status_id",
        "grid_largada",
        ((F.col("grid") == 0) & F.col("largou")).alias("largou_sem_grid"),
        F.col("posicao").alias("posicao_final"),
        "posicao_ordem",
        F.when(F.col("posicao").isNotNull() & F.col("grid_largada").isNotNull(),
               F.col("grid_largada") - F.col("posicao")).alias("posicoes_ganhas"),
        F.col("pontos").alias("pontos_oficiais"),
        F.when(F.col("posicao").between(1, 10), F.element_at(PONTOS_ATUAIS, F.col("posicao")))
         .otherwise(F.lit(0)).cast("int").alias("pontos_sistema_atual"),
        F.col("voltas").alias("voltas_completadas"),
        "tempo_total_ms", "tempo_volta_rapida_ms",
        (F.col("posicao") == 1).alias("venceu"),
        (F.col("posicao") <= 3).alias("podio"),
        (F.col("grid_largada") == 1).alias("largou_pole"),
        F.col("posicao").isNotNull().alias("classificado"),
        "largou",
        (F.col("largou") & F.col("categoria").isin("Mecânica", "Acidente/Incidente",
                                                   "Piloto/Segurança", "Abandono não especificado")).alias("abandonou"),
        (F.col("pais_circuito") == F.col("pais_nacionalidade")).alias("correu_em_casa"),
        F.round(F.months_between("data_corrida", "data_nascimento") / 12, 1).alias("idade_piloto_anos"))
    .withColumn("podio", F.coalesce("podio", F.lit(False)))
    .withColumn("venceu", F.coalesce("venceu", F.lit(False)))
    .withColumn("largou_pole", F.coalesce("largou_pole", F.lit(False))))
salvar_tabela(fato_resultado, "gold.fato_resultado", spark=spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fato: pit stop
# MAGIC A equipe vem de `resultados` (JOIN por corrida + piloto). Nos poucos casos de carro compartilhado isso poderia duplicar linhas, mas os registros de pit stop são de décadas em que isso já não acontecia (a versão do Kaggle começa em 2011); o código rejeita associações ambíguas antes do JOIN e a validação abaixo confirma que nenhuma parada foi duplicada.

# COMMAND ----------

equipes_paradas = (sv("resultados")
    .join(sv("pit_stops").select("corrida_id", "piloto_id").distinct(),
          ["corrida_id", "piloto_id"], "left_semi")
    .select("corrida_id", "piloto_id", "equipe_id").distinct())
if equipes_paradas.groupBy("corrida_id", "piloto_id").count().where("count > 1").limit(1).count():
    raise ValueError("Pit stop com mais de uma equipe possível; não é seguro escolher uma arbitrariamente")
equipe_por_corrida = equipes_paradas

fato_pit_stop = (sv("pit_stops")
    .join(equipe_por_corrida, ["corrida_id", "piloto_id"], "left")
    .join(sv("corridas").select("corrida_id", "circuito_id"), "corrida_id")
    .select("corrida_id", "piloto_id", "numero_parada", "equipe_id", "circuito_id",
            "volta", "duracao_ms", "parada_atipica"))
salvar_tabela(fato_pit_stop, "gold.fato_pit_stop", spark=spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidências e validações da Gold

# COMMAND ----------

gold = ["dim_corrida", "dim_piloto", "dim_equipe", "dim_circuito", "dim_status", "fato_resultado", "fato_pit_stop"]
display(spark.createDataFrame([(f"gold.{t}", spark.table(f"gold.{t}").count(), len(spark.table(f"gold.{t}").columns)) for t in gold],
                              "tabela string, linhas long, colunas int"))

# COMMAND ----------

g = lambda t: spark.table(f"gold.{t}")
checagens = {
    "fato_resultado tem o mesmo nº de linhas da silver.resultados": g("fato_resultado").count() - sv("resultados").count(),
    "fato_pit_stop tem o mesmo nº de linhas da silver.pit_stops": g("fato_pit_stop").count() - sv("pit_stops").count(),
    "resultado_id único na fato_resultado": g("fato_resultado").count() - g("fato_resultado").select("resultado_id").distinct().count(),
    "Toda corrida com resultado tem ao menos um vencedor":
        g("fato_resultado").groupBy("corrida_id").agg(F.sum(F.col("venceu").cast("int")).alias("v")).where("v = 0").count(),
    "Pit stop sem equipe": g("fato_pit_stop").where(F.col("equipe_id").isNull()).count(),
}
for regra_txt, qtd in checagens.items():
    print(("✔" if qtd == 0 else "✘"), regra_txt, "| diferença/violações:", qtd)
checagens.update(dict(validar_modelo(spark, CATALOGO, "gold")))
df_validacao = spark.createDataFrame(
    [(regra, int(qtd), "OK" if qtd == 0 else "FALHA") for regra, qtd in checagens.items()],
    "regra string, qtd_violacoes long, status string")
salvar_tabela(df_validacao, "qualidade.validacao_gold", spark=spark)
display(df_validacao)
if any(v != 0 for v in checagens.values()):
    raise ValueError("Validação da Gold falhou; consulte qualidade.validacao_gold")

# COMMAND ----------

# Amostra do modelo já unido: primeira corrida de 2024
display(spark.sql("""
    SELECT c.ano, c.nome_gp, p.nome_completo, e.nome AS equipe, f.grid_largada, f.posicao_final,
           f.posicoes_ganhas, f.pontos_oficiais, f.pontos_sistema_atual, s.categoria
    FROM gold.fato_resultado f
    JOIN gold.dim_corrida c USING (corrida_id)
    JOIN gold.dim_piloto p USING (piloto_id)
    JOIN gold.dim_equipe e USING (equipe_id)
    JOIN gold.dim_status s USING (status_id)
    WHERE c.ano = 2024 AND c.rodada = 1
    ORDER BY f.posicao_ordem
    LIMIT 10"""))
