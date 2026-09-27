# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Análise – Respondendo às perguntas de negócio
# MAGIC
# MAGIC **Problema:** entender quais fatores determinam o resultado de uma corrida de Fórmula 1 e como o peso de cada um mudou ao longo de 75 temporadas.
# MAGIC
# MAGIC | # | Pergunta |
# MAGIC |---|---|
# MAGIC | P1 | Largar na pole position garante a vitória? Com que frequência o vencedor sai da pole, e isso mudou ao longo das décadas? |
# MAGIC | P2 | O quanto a posição de largada explica a posição de chegada? Em quais circuitos da era atual mais se ganha ou perde posições? |
# MAGIC | P3 | Os carros ficaram mais confiáveis? Como evoluíram os abandonos por falha mecânica e por acidente? |
# MAGIC | P4 | Os pit stops ficaram mais rápidos desde 2011? Quais equipes têm as paradas mais rápidas? |
# MAGIC | P5 | A F1 ficou mais ou menos previsível? Qual a concentração de vitórias da equipe dominante em cada temporada? |
# MAGIC | P6 | Existe "fator casa"? Pilotos rendem mais correndo no próprio país? |
# MAGIC
# MAGIC **Recorte:** temporadas de 1950 até `ANO_FIM_ANALISE` (2024, última completa no dataset). As 500 Milhas de Indianápolis (1950–1960) são excluídas porque, na prática, eram disputadas por outro grupo de pilotos e equipes e distorcem as estatísticas de largada e vitória.

# COMMAND ----------

import matplotlib.pyplot as plt
from config import *

catalogo = preparar_ambiente(spark)
spark.sql(f"CREATE OR REPLACE TEMP VIEW parametros AS SELECT {ANO_FIM_ANALISE} AS ano_fim")

# Visão de apoio: fato + dimensões já unidos, no recorte da análise
spark.sql("""
CREATE OR REPLACE TEMP VIEW base AS
SELECT f.*, c.ano, c.decada, c.era, c.nome_gp, s.categoria,
       p.nome_completo AS piloto, e.nome AS equipe, ci.nome AS circuito, ci.pais AS pais_circuito
FROM gold.fato_resultado f
JOIN gold.dim_corrida  c  USING (corrida_id)
JOIN gold.dim_status   s  USING (status_id)
JOIN gold.dim_piloto   p  USING (piloto_id)
JOIN gold.dim_equipe   e  USING (equipe_id)
JOIN gold.dim_circuito ci ON ci.circuito_id = f.circuito_id
WHERE c.ano <= (SELECT ano_fim FROM parametros) AND NOT c.eh_indy500
""")
print("Linhas na base de análise:", spark.table("base").count())

spark.sql("""
CREATE OR REPLACE TEMP VIEW vencedores_corrida AS
SELECT corrida_id, decada,
       MAX(CAST(largou_pole AS INT)) AS vitoria_pole,
       MAX(CASE WHEN grid_largada <= 3 THEN 1 ELSE 0 END) AS vitoria_top3,
       MAX(CASE WHEN grid_largada > 10 THEN 1 ELSE 0 END) AS vitoria_fora_top10,
       AVG(grid_largada) AS grid_vencedor
FROM base WHERE venceu GROUP BY corrida_id, decada
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 · Largar na pole garante a vitória?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT decada, COUNT(*) AS corridas,
# MAGIC        ROUND(100 * AVG(vitoria_pole), 1) AS pct_corridas_vencidas_da_pole,
# MAGIC        ROUND(100 * AVG(vitoria_top3), 1) AS pct_corridas_vencidas_do_top3,
# MAGIC        ROUND(100 * AVG(vitoria_fora_top10), 1) AS pct_corridas_vencidas_de_fora_top10,
# MAGIC        ROUND(AVG(grid_vencedor), 2) AS grid_medio_do_vencedor
# MAGIC FROM vencedores_corrida GROUP BY decada ORDER BY decada

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visão geral: taxa de vitória por posição de largada (1950–2024)
# MAGIC SELECT grid_largada,
# MAGIC        COUNT(DISTINCT corrida_id)                       AS largadas,
# MAGIC        COUNT(DISTINCT CASE WHEN venceu THEN corrida_id END) AS vitorias,
# MAGIC        ROUND(100 * COUNT(DISTINCT CASE WHEN venceu THEN corrida_id END) / COUNT(DISTINCT corrida_id), 1)        AS pct_vitoria,
# MAGIC        ROUND(100 * COUNT(DISTINCT CASE WHEN podio THEN corrida_id END) / COUNT(DISTINCT corrida_id), 1)         AS pct_podio
# MAGIC FROM base
# MAGIC WHERE grid_largada BETWEEN 1 AND 10 AND largou
# MAGIC GROUP BY grid_largada
# MAGIC ORDER BY grid_largada

# COMMAND ----------

p1 = spark.sql("""
    SELECT decada, 100 * AVG(vitoria_pole) AS pct
    FROM vencedores_corrida GROUP BY decada ORDER BY decada""").toPandas()
fig, ax = plt.subplots(figsize=(9, 4))
ax.bar(p1["decada"], p1["pct"], color="#1f4e79")
for x, y in zip(p1["decada"], p1["pct"]):
    ax.text(x, y + 1, f"{y:.0f}%", ha="center")
ax.set_title("P1 · % das vitórias conquistadas por quem largou na pole")
ax.set_ylabel("% das vitórias"); ax.set_ylim(0, 70); ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 · O quanto a largada explica a chegada?
# MAGIC Associação descritiva, sem inferência causal; exclui abandonos não classificados e tem viés de seleção. Correlação entre `grid_largada` e `posicao_final` (pilotos classificados). Quanto mais perto de 1, mais a corrida "termina como começou".

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT era,
# MAGIC        COUNT(*)                                         AS resultados_classificados,
# MAGIC        ROUND(CORR(grid_largada, posicao_final), 3)      AS correlacao_grid_chegada,
# MAGIC        ROUND(AVG(ABS(posicoes_ganhas)), 2)              AS media_posicoes_trocadas,
# MAGIC        ROUND(100 * AVG(CAST(posicoes_ganhas = 0 AS INT)), 1) AS pct_terminou_onde_largou
# MAGIC FROM base
# MAGIC WHERE classificado AND grid_largada IS NOT NULL
# MAGIC GROUP BY era
# MAGIC ORDER BY era

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Circuitos da era híbrida + efeito solo (2014–2024) com pelo menos 5 corridas
# MAGIC SELECT circuito, pais_circuito AS pais,
# MAGIC        COUNT(DISTINCT corrida_id)                                    AS corridas,
# MAGIC        ROUND(AVG(ABS(posicoes_ganhas)), 2)                           AS media_posicoes_trocadas,
# MAGIC        ROUND(CORR(grid_largada, posicao_final), 3)                   AS correlacao_grid_chegada,
# MAGIC        ROUND(100 * COUNT(DISTINCT CASE WHEN venceu AND largou_pole THEN corrida_id END) / COUNT(DISTINCT corrida_id), 0) AS pct_vitorias_da_pole
# MAGIC FROM base
# MAGIC WHERE ano >= 2014 AND classificado AND grid_largada IS NOT NULL
# MAGIC GROUP BY circuito, pais_circuito
# MAGIC HAVING COUNT(DISTINCT corrida_id) >= 5
# MAGIC ORDER BY media_posicoes_trocadas DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 · Os carros ficaram mais confiáveis?
# MAGIC Percentual dos registros de resultado cujo status indica largada. Carros compartilhados podem gerar múltiplos registros por piloto/corrida nos primeiros anos; não representa contagem de carros. Status não comprova sozinho a participação física em todos os casos históricos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT decada,
# MAGIC        COUNT(*)                                                                 AS registros_de_largada,
# MAGIC        ROUND(100 * AVG(CAST(categoria = 'Finalizou' AS INT)), 1)                AS pct_finalizou,
# MAGIC        ROUND(100 * AVG(CAST(categoria = 'Mecânica' AS INT)), 1)                 AS pct_abandono_mecanico,
# MAGIC        ROUND(100 * AVG(CAST(categoria = 'Acidente/Incidente' AS INT)), 1)       AS pct_abandono_acidente,
# MAGIC        ROUND(100 * AVG(CAST(categoria = 'Abandono não especificado' AS INT)), 1) AS pct_abandono_nao_especificado
# MAGIC FROM base
# MAGIC WHERE largou
# MAGIC GROUP BY decada
# MAGIC ORDER BY decada

# COMMAND ----------

p3 = spark.sql("""
    SELECT decada,
           100 * AVG(CAST(categoria = 'Mecânica' AS INT))           AS mecanica,
           100 * AVG(CAST(categoria = 'Acidente/Incidente' AS INT)) AS acidente
    FROM base WHERE largou GROUP BY decada ORDER BY decada""").toPandas()
fig, ax = plt.subplots(figsize=(9, 4))
ax.plot(p3["decada"], p3["mecanica"], marker="o", label="Falha mecânica", color="#c00000")
ax.plot(p3["decada"], p3["acidente"], marker="o", label="Acidente/incidente", color="#7f7f7f")
ax.set_title("P3 · % das largadas que terminaram em abandono")
ax.set_ylabel("% das largadas"); ax.legend(); ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 · Os pit stops ficaram mais rápidos?
# MAGIC Recorte: 2011 em diante, primeiro ano com pit stops registrados na versão do Kaggle.
# MAGIC
# MAGIC A fonte mede o **tempo total no pit lane** (entrada até saída), que inclui o trajeto na via dos boxes. Por isso os valores (~20–30 s) são maiores que os famosos "2 segundos" de troca de pneus. Paradas atípicas (> 60 s) são excluídas. Usamos a **mediana**, que é robusta a outliers.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.ano,
# MAGIC        COUNT(*)                                        AS paradas,
# MAGIC        ROUND(PERCENTILE_APPROX(p.duracao_ms, 0.5) / 1000, 2) AS mediana_s,
# MAGIC        ROUND(PERCENTILE_APPROX(p.duracao_ms, 0.1) / 1000, 2) AS p10_s,
# MAGIC        ROUND(COUNT(*) / COUNT(DISTINCT p.corrida_id), 1)    AS paradas_por_corrida
# MAGIC FROM gold.fato_pit_stop p
# MAGIC JOIN gold.dim_corrida c USING (corrida_id)
# MAGIC WHERE NOT p.parada_atipica AND c.ano BETWEEN 2011 AND (SELECT ano_fim FROM parametros)
# MAGIC GROUP BY c.ano
# MAGIC ORDER BY c.ano

# COMMAND ----------

# MAGIC %md
# MAGIC Como o tempo de pit lane depende muito do circuito (comprimento da via dos boxes), a comparação entre equipes usa o **tempo relativo**: a duração de cada parada dividida pela mediana daquela corrida. Um valor de 0,97 significa uma parada 3% mais rápida que a mediana da corrida.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH paradas AS (
# MAGIC   SELECT p.*, c.ano,
# MAGIC          p.duracao_ms / PERCENTILE_APPROX(p.duracao_ms, 0.5) OVER (PARTITION BY p.corrida_id) AS tempo_relativo
# MAGIC   FROM gold.fato_pit_stop p JOIN gold.dim_corrida c USING (corrida_id)
# MAGIC   WHERE NOT p.parada_atipica AND c.ano BETWEEN (SELECT ano_fim FROM parametros) - 2 AND (SELECT ano_fim FROM parametros)
# MAGIC )
# MAGIC SELECT e.nome AS equipe,
# MAGIC        COUNT(*)                                   AS paradas,
# MAGIC        ROUND(PERCENTILE_APPROX(tempo_relativo, 0.5), 3) AS tempo_relativo_mediano
# MAGIC FROM paradas JOIN gold.dim_equipe e USING (equipe_id)
# MAGIC GROUP BY e.nome
# MAGIC HAVING COUNT(*) >= 50
# MAGIC ORDER BY tempo_relativo_mediano

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 · A F1 ficou mais ou menos previsível?

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dominancia AS
# MAGIC WITH vitorias AS (
# MAGIC   SELECT ano, equipe, COUNT(DISTINCT corrida_id) AS vitorias
# MAGIC   FROM base WHERE venceu GROUP BY ano, equipe
# MAGIC ), corridas_ano AS (
# MAGIC   SELECT ano, COUNT(DISTINCT corrida_id) AS corridas
# MAGIC   FROM base GROUP BY ano
# MAGIC ), ranking AS (
# MAGIC   SELECT v.ano, v.equipe, v.vitorias, c.corridas,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY v.ano ORDER BY v.vitorias DESC, v.equipe) AS rk
# MAGIC   FROM vitorias v JOIN corridas_ano c ON v.ano = c.ano
# MAGIC )
# MAGIC SELECT r.ano, r.equipe AS equipe_dominante, r.vitorias, r.corridas,
# MAGIC        ROUND(100 * r.vitorias / r.corridas, 1) AS pct_vitorias_equipe_dominante,
# MAGIC        (SELECT COUNT(DISTINCT piloto) FROM base b WHERE b.ano = r.ano AND b.venceu) AS pilotos_vencedores_distintos
# MAGIC FROM ranking r WHERE rk = 1;
# MAGIC
# MAGIC SELECT CONCAT(CAST(FLOOR(ano / 10) * 10 AS STRING), 's') AS decada,
# MAGIC        ROUND(AVG(pct_vitorias_equipe_dominante), 1)   AS media_pct_vitorias_equipe_dominante,
# MAGIC        ROUND(AVG(pilotos_vencedores_distintos), 1)    AS media_pilotos_vencedores_por_temporada
# MAGIC FROM dominancia GROUP BY 1 ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- As 10 temporadas mais dominadas por uma única equipe
# MAGIC SELECT * FROM dominancia ORDER BY pct_vitorias_equipe_dominante DESC, ano LIMIT 10

# COMMAND ----------

p5 = spark.table("dominancia").orderBy("ano").toPandas()
fig, ax = plt.subplots(figsize=(11, 4))
ax.bar(p5["ano"], p5["pct_vitorias_equipe_dominante"], color="#1f4e79")
ax.plot(p5["ano"], p5["pct_vitorias_equipe_dominante"].rolling(5, center=True).mean(), color="#c00000", label="Média móvel de 5 anos")
ax.set_title("P5 · % das vitórias da equipe que mais venceu em cada temporada")
ax.set_ylabel("% das vitórias"); ax.legend(); ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P6 · Existe fator casa?
# MAGIC Comparar médias gerais "em casa × fora" seria enganoso: países com muitos pilotos fracos (ou fortes) distorcem o resultado. Por isso a comparação é **pareada por piloto**: cada piloto é comparado com ele mesmo. Recorte: 1980–2024, pilotos com pelo menos 3 largadas em casa e 20 fora. Métrica: pontos no sistema atual, que permite somar eras diferentes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW fator_casa AS
# MAGIC SELECT piloto_id, piloto,
# MAGIC        SUM(CAST(correu_em_casa AS INT))                                        AS largadas_em_casa,
# MAGIC        SUM(CAST(NOT correu_em_casa AS INT))                                    AS largadas_fora,
# MAGIC        AVG(CASE WHEN correu_em_casa THEN pontos_sistema_atual END)             AS pontos_medios_casa,
# MAGIC        AVG(CASE WHEN NOT correu_em_casa THEN pontos_sistema_atual END)         AS pontos_medios_fora,
# MAGIC        AVG(CASE WHEN correu_em_casa THEN CAST(abandonou AS INT) END)           AS taxa_abandono_casa,
# MAGIC        AVG(CASE WHEN NOT correu_em_casa THEN CAST(abandonou AS INT) END)       AS taxa_abandono_fora
# MAGIC FROM base
# MAGIC WHERE ano >= 1980 AND largou AND correu_em_casa IS NOT NULL
# MAGIC GROUP BY piloto_id, piloto
# MAGIC HAVING SUM(CAST(correu_em_casa AS INT)) >= 3 AND SUM(CAST(NOT correu_em_casa AS INT)) >= 20;
# MAGIC
# MAGIC SELECT COUNT(*)                                                                 AS pilotos_analisados,
# MAGIC        ROUND(AVG(pontos_medios_casa), 2)                                        AS media_pontos_casa,
# MAGIC        ROUND(AVG(pontos_medios_fora), 2)                                        AS media_pontos_fora,
# MAGIC        ROUND(AVG(pontos_medios_casa - pontos_medios_fora), 2)                   AS diferenca_media_pareada,
# MAGIC        ROUND(100 * AVG(CAST(pontos_medios_casa > pontos_medios_fora AS INT)), 1) AS pct_pilotos_melhores_em_casa,
# MAGIC        ROUND(100 * AVG(taxa_abandono_casa), 1)                                  AS pct_abandono_casa,
# MAGIC        ROUND(100 * AVG(taxa_abandono_fora), 1)                                  AS pct_abandono_fora
# MAGIC FROM fator_casa

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pilotos com maior diferença (casa − fora), entre os que têm ao menos 8 largadas em casa
# MAGIC SELECT piloto, largadas_em_casa, largadas_fora,
# MAGIC        ROUND(pontos_medios_casa, 2) AS pontos_medios_casa,
# MAGIC        ROUND(pontos_medios_fora, 2) AS pontos_medios_fora,
# MAGIC        ROUND(pontos_medios_casa - pontos_medios_fora, 2) AS diferenca
# MAGIC FROM fator_casa
# MAGIC WHERE largadas_em_casa >= 8
# MAGIC ORDER BY diferenca DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Teste de robustez: a diferença pareada é distinguível de zero?
# MAGIC -- Estatística t de uma amostra pareada: média / (desvio padrão / raiz(n))
# MAGIC SELECT COUNT(*) AS n,
# MAGIC        ROUND(AVG(d), 3) AS media_diferenca,
# MAGIC        ROUND(STDDEV(d), 3) AS desvio_padrao,
# MAGIC        ROUND(AVG(d) / NULLIF(STDDEV_SAMP(d) / SQRT(COUNT(*)), 0), 2) AS estatistica_t
# MAGIC FROM (SELECT pontos_medios_casa - pontos_medios_fora AS d FROM fator_casa)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limites de interpretação
# MAGIC As análises são descritivas. Correlação de grid/chegada não mede causalidade, posições ganhas não equivalem a ultrapassagens e concentração de vitórias não mede acurácia de previsão. P4 mede tempo no pit lane. P6 compara médias de carreira e não controla equipe, temporada, circuito ou calendário; ausência de significância não comprova ausência de efeito.
