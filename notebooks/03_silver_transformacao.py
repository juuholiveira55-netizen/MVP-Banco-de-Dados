# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver – Limpeza, tipagem e padronização
# MAGIC
# MAGIC **ETL 2 de 3 (Bronze → Silver).** Cada tabela usada no modelo passa pelos tratamentos abaixo, que respondem diretamente aos problemas encontrados no notebook 02.
# MAGIC
# MAGIC | # | Problema encontrado (notebook 02) | Tratamento aplicado |
# MAGIC |---|---|---|
# MAGIC | T1 | Nulos representados pelo texto `\N` e espaços sobrando | `\N` e texto vazio viram `NULL`; `trim` em todos os textos |
# MAGIC | T2 | Todas as colunas são texto | Tipagem explícita (int, long, double, date) com `try_cast` |
# MAGIC | T3 | Nomes de colunas em inglês/camelCase | Renomeação para `snake_case` em português |
# MAGIC | T4 | `Argentine` x `Argentinian`, `USA` x `United States` | Padronização para uma única grafia |
# MAGIC | T5 | 2 resultados com `position` nula, mas `positionText` numérico | `position` recebe o valor de `positionText` |
# MAGIC | T6 | Tempos como texto (`1:27.452`) | Conversão para milissegundos (`long`) |
# MAGIC | T7 | Pit stops com duração de minutos (bandeira vermelha/reparo) | Mantidos, mas marcados em `parada_atipica` (> 60 s) |
# MAGIC | T8 | 140+ descrições de status diferentes | Criação de `categoria` (Finalizou, Mecânica, Acidente/Incidente...) |
# MAGIC | T9 | Nacionalidade do piloto ≠ país do circuito (British x UK) | Tabela de referência `ref_nacionalidade_pais` |
# MAGIC | T10 | Possibilidade de linhas repetidas | Remoção de linhas idênticas; chave com conteúdos conflitantes interrompe a carga |
# MAGIC
# MAGIC Ao final, validações automáticas confirmam que os tratamentos funcionaram (`qualidade.validacao_silver`).

# COMMAND ----------

from pyspark.sql import functions as F
from config import *
from validacao import deduplicar_sem_conflitos, validar_modelo
from catalogo_def import CATALOGO

catalogo = preparar_ambiente(spark)
b = lambda t: spark.table(f"bronze.{t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funções de apoio

# COMMAND ----------

def limpar(df):
    """T1: remove metadados da Bronze, aplica trim e converte '\\N'/vazio em NULL."""
    colunas = []
    for c, tipo in df.dtypes:
        if c.startswith("_"):
            continue
        if tipo == "string":
            colunas.append(F.when(F.trim(F.col(c)).isin("\\N", ""), None).otherwise(F.trim(F.col(c))).alias(c))
        else:
            colunas.append(F.col(c))
    return df.select(*colunas)


def tc(coluna, tipo):
    """T2: conversão tolerante (try_cast)."""
    return F.expr(f"try_cast(`{coluna}` AS {tipo})")


def tempo_para_ms(coluna):
    """T6: '1:27.452' ou '27.452' -> milissegundos."""
    c = F.col(coluna)
    return (F.when(c.isNull(), None)
             .when(c.contains(":"),
                   F.expr(f"try_cast(split(`{coluna}`, ':')[0] AS long)") * 60000
                   + F.round(F.expr(f"try_cast(split(`{coluna}`, ':')[1] AS double)") * 1000).cast("long"))
             .otherwise(F.round(tc(coluna, "double") * 1000).cast("long")))


def finalizar(df, chave, nome):
    """T10: deduplica pela chave, adiciona auditoria e grava. Retorna (tabela, linhas, removidas)."""
    antes = df.count()
    df = deduplicar_sem_conflitos(df, chave, nome).withColumn("_processado_em", F.current_timestamp())
    depois = df.count()
    salvar_tabela(df, f"silver.{nome}", spark=spark)
    return (f"silver.{nome}", depois, antes - depois)


log = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Circuitos

# COMMAND ----------

circuitos = (limpar(b("circuits"))
    .select(
        tc("circuitId", "int").alias("circuito_id"),
        F.col("circuitRef").alias("circuito_ref"),
        F.col("name").alias("nome"),
        F.col("location").alias("cidade"),
        # T4: 'United States' e 'USA' representam o mesmo país
        F.when(F.col("country") == "United States", "USA").otherwise(F.col("country")).alias("pais"),
        tc("lat", "double").alias("latitude"),
        tc("lng", "double").alias("longitude"),
        tc("alt", "int").alias("altitude_m"),
        F.col("url").alias("url_wikipedia")))
log.append(finalizar(circuitos, ["circuito_id"], "circuitos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Equipes (construtores)

# COMMAND ----------

equipes = (limpar(b("constructors"))
    .select(
        tc("constructorId", "int").alias("equipe_id"),
        F.col("constructorRef").alias("equipe_ref"),
        F.col("name").alias("nome"),
        F.col("nationality").alias("nacionalidade"),
        F.col("url").alias("url_wikipedia")))
log.append(finalizar(equipes, ["equipe_id"], "equipes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pilotos

# COMMAND ----------

pilotos = (limpar(b("drivers"))
    .select(
        tc("driverId", "int").alias("piloto_id"),
        F.col("driverRef").alias("piloto_ref"),
        tc("number", "int").alias("numero_permanente"),
        F.col("code").alias("codigo"),
        F.col("forename").alias("nome"),
        F.col("surname").alias("sobrenome"),
        F.concat_ws(" ", "forename", "surname").alias("nome_completo"),
        tc("dob", "date").alias("data_nascimento"),
        # T4: 'Argentinian' (com espaço no fim, já removido pelo trim) -> 'Argentine'
        F.when(F.col("nationality") == "Argentinian", "Argentine").otherwise(F.col("nationality")).alias("nacionalidade"),
        F.col("url").alias("url_wikipedia")))
log.append(finalizar(pilotos, ["piloto_id"], "pilotos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Corridas
# MAGIC Os horários de treinos livres (fp1–fp3) só existem a partir de 2021 e não são usados nas perguntas; por isso ficam apenas na Bronze. A data da sprint é mantida para marcar fins de semana com corrida sprint.

# COMMAND ----------

corridas = (limpar(b("races"))
    .select(
        tc("raceId", "int").alias("corrida_id"),
        tc("year", "int").alias("ano"),
        tc("round", "int").alias("rodada"),
        tc("circuitId", "int").alias("circuito_id"),
        F.col("name").alias("nome_gp"),
        tc("date", "date").alias("data"),
        F.col("time").alias("hora_utc"),
        tc("sprint_date", "date").alias("data_sprint"),
        F.col("url").alias("url_wikipedia")))
log.append(finalizar(corridas, ["corrida_id"], "corridas"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Status (com categorização – T8)
# MAGIC
# MAGIC A fonte tem mais de 140 descrições de status (motor, câmbio, colisão, "+3 Laps"...). Para medir confiabilidade, elas foram agrupadas em categorias:
# MAGIC
# MAGIC | Categoria | Regra |
# MAGIC |---|---|
# MAGIC | Finalizou | `Finished`, `+N Lap(s)` ou `Lapped` |
# MAGIC | Acidente/Incidente | Acidente, colisão, rodada, furo, detritos, asa quebrada |
# MAGIC | Desclassificado | `Disqualified`, `Excluded`, `Underweight` |
# MAGIC | Não largou | Não se classificou, retirou-se, regra dos 107% |
# MAGIC | Não classificado | `Not classified` (correu, mas não completou a distância mínima) |
# MAGIC | Piloto/Segurança | Lesão, doença, condições físicas, questões de segurança |
# MAGIC | Abandono não especificado | `Retired` ou `Not restarted` (causa não informada) |
# MAGIC | Mecânica | Lista explícita de falhas; status novo interrompe a validação |

# COMMAND ----------

ACIDENTE = ["Accident", "Collision", "Collision damage", "Spun off", "Fatal accident", "Damage",
            "Debris", "Puncture", "Tyre puncture", "Broken wing"]
DESCLASSIFICADO = ["Disqualified", "Excluded", "Underweight"]
NAO_LARGOU = ["Did not qualify", "Did not prequalify", "Withdrew", "107% Rule"]
PILOTO = ["Injured", "Injury", "Eye injury", "Illness", "Physical", "Driver unwell", "Safety concerns", "Safety"]
MECANICA = [
    "Engine", "Gearbox", "Transmission", "Clutch", "Hydraulics", "Electrical",
    "Radiator", "Suspension", "Brakes", "Differential", "Overheating", "Mechanical",
    "Tyre", "Driver Seat", "Driveshaft", "Fuel pressure", "Front wing", "Water pressure",
    "Refuelling", "Wheel", "Throttle", "Steering", "Technical", "Electronics",
    "Heat shield fire", "Exhaust", "Oil leak", "Wheel rim", "Water leak", "Fuel pump",
    "Track rod", "Oil pressure", "Engine fire", "Engine misfire", "Out of fuel", "Wheel nut",
    "Pneumatics", "Handling", "Rear wing", "Fire", "Wheel bearing", "Fuel system",
    "Oil line", "Fuel rig", "Launch control", "Fuel", "Power loss", "Vibrations",
    "Drivetrain", "Ignition", "Chassis", "Battery", "Stalled", "Halfshaft", "Crankshaft",
    "Alternator", "Safety belt", "Oil pump", "Fuel leak", "Injection", "Distributor",
    "Turbo", "CV joint", "Water pump", "Spark plugs", "Fuel pipe", "Oil pipe", "Axle",
    "Water pipe", "Magneto", "Supercharger", "Power Unit", "ERS", "Brake duct", "Seat",
    "Undertray", "Cooling system",
]

s = F.col("status")
status = (limpar(b("status"))
    .select(
        tc("statusId", "int").alias("status_id"),
        s.alias("descricao"),
        F.when(s.rlike(r"^(Finished|\+\d+ Laps?|Lapped)$"), "Finalizou")
         .when(s.isin(ACIDENTE), "Acidente/Incidente")
         .when(s.isin(DESCLASSIFICADO), "Desclassificado")
         .when(s.isin(NAO_LARGOU), "Não largou")
         .when(s == "Not classified", "Não classificado")
         .when(s.isin(PILOTO), "Piloto/Segurança")
         .when(s.isin("Retired", "Not restarted"), "Abandono não especificado")
         .when(s.isin(MECANICA), "Mecânica")
         .otherwise(F.lit(None).cast("string")).alias("categoria")))
log.append(finalizar(status, ["status_id"], "status"))
display(spark.table("silver.status").groupBy("categoria").agg(F.count("*").alias("qtd_status"), F.slice(F.collect_list("descricao"), 1, 6).alias("exemplos")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resultados
# MAGIC Observação importante: nos anos 1950 era permitido **dividir o carro** entre pilotos, por isso um mesmo piloto pode ter mais de um resultado na mesma corrida (85 casos). A chave única é `resultado_id`, e não a combinação corrida + piloto.

# COMMAND ----------

resultados = (limpar(b("results"))
    # T5: corrige a divergência entre position e positionText
    .withColumn("position", F.when(F.col("position").isNull() & F.col("positionText").rlike(r"^\d+$"),
                                   F.col("positionText")).otherwise(F.col("position")))
    .select(
        tc("resultId", "int").alias("resultado_id"),
        tc("raceId", "int").alias("corrida_id"),
        tc("driverId", "int").alias("piloto_id"),
        tc("constructorId", "int").alias("equipe_id"),
        F.col("number").alias("numero_carro"),
        tc("grid", "int").alias("grid"),
        tc("position", "int").alias("posicao"),
        F.col("positionText").alias("posicao_texto"),
        tc("positionOrder", "int").alias("posicao_ordem"),
        tc("points", "double").alias("pontos"),
        tc("laps", "int").alias("voltas"),
        tc("milliseconds", "long").alias("tempo_total_ms"),
        tc("fastestLap", "int").alias("volta_mais_rapida"),
        tc("rank", "int").alias("rank_volta_rapida"),
        tempo_para_ms("fastestLapTime").alias("tempo_volta_rapida_ms"),
        tc("fastestLapSpeed", "double").alias("velocidade_volta_rapida_kmh"),
        tc("statusId", "int").alias("status_id")))
log.append(finalizar(resultados, ["resultado_id"], "resultados"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pit stops
# MAGIC A duração é convertida para milissegundos a partir da coluna numérica `milliseconds` (a coluna `duration` mistura os formatos `SS.mmm` e `M:SS.mmm`). Paradas acima de 60 s **não são apagadas**: elas são reais (bandeira vermelha, reparos), mas distorcem médias, então ficam marcadas para serem filtradas nas análises.

# COMMAND ----------

LIMITE_PARADA_ATIPICA_MS = 60_000

pit_stops = (limpar(b("pit_stops"))
    .select(
        tc("raceId", "int").alias("corrida_id"),
        tc("driverId", "int").alias("piloto_id"),
        tc("stop", "int").alias("numero_parada"),
        tc("lap", "int").alias("volta"),
        F.col("time").alias("hora_local"),
        F.coalesce(tc("milliseconds", "long"), tempo_para_ms("duration")).alias("duracao_ms"))
    .withColumn("parada_atipica", F.col("duracao_ms") > LIMITE_PARADA_ATIPICA_MS))
log.append(finalizar(pit_stops, ["corrida_id", "piloto_id", "numero_parada"], "pit_stops"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Referência: nacionalidade → país (T9)
# MAGIC Tabela criada manualmente para ligar a nacionalidade do piloto (`British`) ao país do circuito (`UK`), usada para identificar corridas "em casa". Para nacionalidades duplas (ex.: `American-Italian`), foi adotado o primeiro país.

# COMMAND ----------

MAPA_NACIONALIDADE = {
    "American": "USA", "American-Italian": "USA", "Argentine": "Argentina", "Argentine-Italian": "Argentina",
    "Australian": "Australia", "Austrian": "Austria", "Belgian": "Belgium", "Brazilian": "Brazil",
    "British": "UK", "Canadian": "Canada", "Chilean": "Chile", "Chinese": "China", "Colombian": "Colombia",
    "Czech": "Czech Republic", "Danish": "Denmark", "Dutch": "Netherlands", "East German": "Germany",
    "Finnish": "Finland", "French": "France", "German": "Germany", "Hungarian": "Hungary", "Indian": "India",
    "Indonesian": "Indonesia", "Irish": "Ireland", "Italian": "Italy", "Japanese": "Japan",
    "Liechtensteiner": "Liechtenstein", "Malaysian": "Malaysia", "Mexican": "Mexico", "Monegasque": "Monaco",
    "New Zealander": "New Zealand", "Polish": "Poland", "Portuguese": "Portugal", "Rhodesian": "Rhodesia",
    "Russian": "Russia", "South African": "South Africa", "Spanish": "Spain", "Swedish": "Sweden",
    "Swiss": "Switzerland", "Thai": "Thailand", "Uruguayan": "Uruguay", "Venezuelan": "Venezuela",
}
ref = spark.createDataFrame(list(MAPA_NACIONALIDADE.items()), "nacionalidade string, pais string")
log.append(finalizar(ref, ["nacionalidade"], "ref_nacionalidade_pais"))

sem_mapa = spark.table("silver.pilotos").join(ref, "nacionalidade", "left_anti").select("nacionalidade").distinct()
print("Nacionalidades sem mapeamento:", [r[0] for r in sem_mapa.collect()] or "nenhuma ✔")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumo da carga Silver

# COMMAND ----------

display(spark.createDataFrame(log, "tabela string, linhas long, duplicatas_removidas long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validações pós-tratamento
# MAGIC Cada regra deve retornar **0** violações. Se alguma falhar, o notebook para com erro, impedindo que dados ruins cheguem à Gold.

# COMMAND ----------

sv = lambda t: spark.table(f"silver.{t}")
validacoes = [
    ("Nenhum texto '\\N' restante em resultados", sv("resultados").where(F.col("posicao_texto") == "\\N").count()),
    ("Nenhum texto '\\N' restante em pilotos (código)", sv("pilotos").where(F.col("codigo") == "\\N").count()),
    ("Grafia única para EUA em circuitos", sv("circuitos").where(F.col("pais") == "United States").count()),
    ("Grafia única para Argentina em pilotos", sv("pilotos").where(F.col("nacionalidade") == "Argentinian").count()),
    ("position x positionText consistentes", sv("resultados").where(F.col("posicao").isNull() & F.col("posicao_texto").rlike(r"^\d+$")).count()),
    ("Chaves primárias não nulas (resultados)", sv("resultados").where(F.col("resultado_id").isNull() | F.col("corrida_id").isNull() | F.col("piloto_id").isNull()).count()),
    ("Datas de corrida válidas", sv("corridas").where(F.col("data").isNull()).count()),
    ("Pontos preenchidos e não negativos", sv("resultados").where(F.col("pontos").isNull() | F.isnan("pontos") | (F.col("pontos") < 0)).count()),
    ("Duração de pit stop preenchida e positiva", sv("pit_stops").where(F.col("duracao_ms").isNull() | (F.col("duracao_ms") <= 0)).count()),
    ("Status sem categoria", sv("status").where(F.col("categoria").isNull()).count()),
    ("Pilotos sem país de nacionalidade mapeado", sem_mapa.count()),
]
validacoes += validar_modelo(spark, CATALOGO, "silver")
for tabela, campos in {
    "resultados": ["grid", "voltas", "posicao_ordem"],
    "pit_stops": ["numero_parada", "volta"],
    "corridas": ["ano", "rodada"],
}.items():
    for campo in campos:
        minimo = 0 if campo in {"grid", "voltas"} else 1
        invalidos = F.col(campo).isNull() | (F.col(campo) < minimo)
        validacoes.append((f"{tabela}.{campo}: obrigatório e >= {minimo}", sv(tabela).where(invalidos).count()))
validacoes.append(("Pit stop com resultado correspondente",
    sv("pit_stops").join(sv("resultados").select("corrida_id", "piloto_id").distinct(),
                         ["corrida_id", "piloto_id"], "left_anti").count()))
df_val = spark.createDataFrame([(r, q, "OK" if q == 0 else "FALHA") for r, q in validacoes],
                               "regra string, qtd_violacoes long, status string")
salvar_tabela(df_val, "qualidade.validacao_silver", "Validações executadas após a transformação Silver.", spark)
display(df_val)
if df_val.where("status = 'FALHA'").count():
    raise ValueError("Há validações com falha na Silver; consulte qualidade.validacao_silver")
