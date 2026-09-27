"""
Catálogo de Dados do MVP F1 – fonte única da verdade.

Usado por:
  * notebook 05_catalogo_dados -> aplica descrições, tags e constraints no Unity Catalog
  * docs/gerar_catalogo_md.py  -> gera a transcrição do catálogo no README

Cada coluna: (nome, tipo, descrição, domínio esperado, linhagem)
"""

FONTE = "Ergast via Kaggle"

CATALOGO = {
    # =====================================================================================
    # GOLD
    # =====================================================================================
    "gold.fato_resultado": {
        "tipo": "fato",
        "descricao": ("Fato central do modelo. Uma linha por resultado de um piloto em uma corrida do "
                      "Campeonato Mundial (1950 em diante). Nos anos 1950, um piloto pode ter mais de um "
                      "resultado na mesma corrida (carro compartilhado)."),
        "pk": ["resultado_id"],
        "fk": {"corrida_id": "gold.dim_corrida", "piloto_id": "gold.dim_piloto", "equipe_id": "gold.dim_equipe",
               "circuito_id": "gold.dim_circuito", "status_id": "gold.dim_status"},
        "colunas": [
            ("resultado_id", "int", "Identificador único do resultado (PK).", "Inteiro ≥ 1, único", "results.resultId"),
            ("corrida_id", "int", "Corrida (FK para dim_corrida).", "Existente em dim_corrida", "results.raceId"),
            ("piloto_id", "int", "Piloto (FK para dim_piloto).", "Existente em dim_piloto", "results.driverId"),
            ("equipe_id", "int", "Equipe pela qual o piloto correu (FK para dim_equipe).", "Existente em dim_equipe", "results.constructorId"),
            ("circuito_id", "int", "Circuito da corrida (FK para dim_circuito), trazido para o fato para evitar snowflake.", "Existente em dim_circuito", "races.circuitId via JOIN por raceId"),
            ("status_id", "int", "Status final do piloto (FK para dim_status).", "Existente em dim_status", "results.statusId"),
            ("grid_largada", "int", "Posição de largada. NULL quando largou do pit lane ou sem posição registrada.", "1 a 34; NULL", "results.grid (0 → NULL)"),
            ("largou_sem_grid", "boolean", "Grid não informado (grid = 0 e status indica largada); não comprova saída do pit lane.", "true/false", "Derivado de results.grid e da categoria do status"),
            ("posicao_final", "int", "Posição de chegada oficial. NULL se não classificado.", "1 a 33; NULL", "results.position (corrigida por positionText em 2 casos)"),
            ("posicao_ordem", "int", "Ordem final incluindo não classificados (sempre preenchida).", "1 a 39", "results.positionOrder"),
            ("posicoes_ganhas", "int", "grid_largada − posicao_final. Positivo = ganhou posições.", "-30 a 30; NULL se não classificado", "Derivado"),
            ("pontos_oficiais", "double", "Pontos oficiais recebidos, conforme o regulamento da época.", "0 a 50 (GP de pontos dobrados de 2014)", "results.points"),
            ("pontos_sistema_atual", "int", "Pontos que o resultado valeria no sistema 25-18-15-12-10-8-6-4-2-1, para comparar eras.", "0,1,2,4,6,8,10,12,15,18,25", "Derivado de posicao_final"),
            ("voltas_completadas", "int", "Voltas completadas pelo piloto.", "0 a 200", "results.laps"),
            ("tempo_total_ms", "bigint", "Tempo total de prova em milissegundos (apenas para quem terminou na volta do líder).", "> 0; NULL", "results.milliseconds"),
            ("tempo_volta_rapida_ms", "bigint", "Melhor volta do piloto em milissegundos (disponível a partir de 2004).", "> 0; NULL", "results.fastestLapTime convertido de M:SS.mmm"),
            ("venceu", "boolean", "Verdadeiro se posicao_final = 1.", "true/false", "Derivado"),
            ("podio", "boolean", "Verdadeiro se posicao_final ≤ 3.", "true/false", "Derivado"),
            ("largou_pole", "boolean", "Verdadeiro se grid_largada = 1.", "true/false", "Derivado"),
            ("classificado", "boolean", "Verdadeiro se o piloto recebeu posição oficial.", "true/false", "Derivado de results.position"),
            ("largou", "boolean", "Verdadeiro se participou da corrida (status diferente de 'Não largou').", "true/false", "Derivado de dim_status.categoria"),
            ("abandonou", "boolean", "Verdadeiro se largou e terminou por Mecânica, Acidente/Incidente, Piloto/Segurança ou Abandono não especificado.", "true/false", "Derivado de dim_status.categoria"),
            ("correu_em_casa", "boolean", "Verdadeiro se o país do circuito é o país da nacionalidade do piloto; NULL se o país não estiver mapeado.", "true/false/NULL", "Derivado: circuits.country × silver.ref_nacionalidade_pais"),
            ("idade_piloto_anos", "double", "Idade do piloto na data da corrida, em anos (1 casa decimal).", "17 a 60", "Derivado: races.date − drivers.dob"),
        ],
    },
    "gold.fato_pit_stop": {
        "tipo": "fato",
        "descricao": "Uma linha por parada de box. Na versão do Kaggle, os registros começam na temporada 2011.",
        "pk": ["corrida_id", "piloto_id", "numero_parada"],
        "fk": {"corrida_id": "gold.dim_corrida", "piloto_id": "gold.dim_piloto", "equipe_id": "gold.dim_equipe",
               "circuito_id": "gold.dim_circuito"},
        "colunas": [
            ("corrida_id", "int", "Corrida (FK para dim_corrida).", "Existente em dim_corrida", "pit_stops.raceId"),
            ("piloto_id", "int", "Piloto (FK para dim_piloto).", "Existente em dim_piloto", "pit_stops.driverId"),
            ("numero_parada", "int", "Número sequencial da parada do piloto na corrida.", "1 a 8", "pit_stops.stop"),
            ("equipe_id", "int", "Equipe do piloto naquela corrida (FK para dim_equipe).", "Existente em dim_equipe", "results.constructorId via JOIN por raceId + driverId"),
            ("circuito_id", "int", "Circuito da corrida (FK para dim_circuito).", "Existente em dim_circuito", "races.circuitId via JOIN por raceId"),
            ("volta", "int", "Volta em que a parada aconteceu.", "1 a 78", "pit_stops.lap"),
            ("duracao_ms", "bigint", "Tempo total no pit lane (entrada à saída) em milissegundos.", "≈ 8 000 a 60 000 em paradas normais", "pit_stops.milliseconds"),
            ("parada_atipica", "boolean", "Verdadeiro se duracao_ms > 60 000 (bandeira vermelha, reparo, penalidade). Filtrar em médias.", "true/false", "Derivado de duracao_ms"),
        ],
    },
    "gold.dim_corrida": {
        "tipo": "dimensão",
        "descricao": "Uma linha por Grande Prêmio do calendário. Também cumpre o papel de dimensão de tempo (ano, década, era regulamentar).",
        "pk": ["corrida_id"],
        "fk": {"circuito_id": "gold.dim_circuito"},
        "colunas": [
            ("corrida_id", "int", "Identificador da corrida (PK).", "Inteiro ≥ 1, único", "races.raceId"),
            ("ano", "int", "Temporada.", "1950 a ano atual", "races.year"),
            ("rodada", "int", "Número da etapa na temporada.", "1 a 24", "races.round"),
            ("nome_gp", "string", "Nome oficial do Grande Prêmio (em inglês, como na fonte).", "Ex.: 'Brazilian Grand Prix'", "races.name"),
            ("data", "date", "Data da corrida.", "1950-05-13 em diante", "races.date"),
            ("circuito_id", "int", "Circuito onde a corrida aconteceu (FK para dim_circuito).", "Existente em dim_circuito", "races.circuitId"),
            ("decada", "string", "Década da temporada.", "'1950s' a '2020s'", "Derivado de ano"),
            ("era", "string", "Agrupamento histórico simplificado por ano; não representa regras uniformes em toda a faixa.", "7 eras, de '1. Pioneira (1950-60)' a '7. Efeito solo (2022+)'", "Derivado de ano"),
            ("tem_sprint", "boolean", "Verdadeiro se o fim de semana teve corrida sprint.", "true/false (sprints desde 2021)", "Derivado de races.sprint_date"),
            ("eh_indy500", "boolean", "Verdadeiro para as 500 Milhas de Indianápolis (contaram para o Mundial de 1950 a 1960).", "true/false", "Derivado de races.name"),
            ("possui_resultado", "boolean", "Verdadeiro se há resultados carregados para a corrida (falso para corridas futuras).", "true/false", "Derivado: existência em results"),
        ],
    },
    "gold.dim_piloto": {
        "tipo": "dimensão",
        "descricao": "Uma linha por piloto que participou de ao menos uma inscrição no Mundial.",
        "pk": ["piloto_id"],
        "fk": {},
        "colunas": [
            ("piloto_id", "int", "Identificador do piloto (PK).", "Inteiro ≥ 1, único", "drivers.driverId"),
            ("nome_completo", "string", "Nome e sobrenome.", "Texto", "drivers.forename + ' ' + drivers.surname"),
            ("codigo", "string", "Código de três letras usado nas transmissões.", "3 letras maiúsculas; NULL para pilotos antigos", "drivers.code"),
            ("numero_permanente", "int", "Número permanente do carro (regra criada em 2014).", "1 a 99; NULL para pilotos antigos", "drivers.number"),
            ("data_nascimento", "date", "Data de nascimento.", "1896 a 2008", "drivers.dob"),
            ("nacionalidade", "string", "Nacionalidade (em inglês, padronizada).", "42 valores, ex.: 'British', 'Brazilian'", "drivers.nationality (trim + 'Argentinian'→'Argentine')"),
            ("pais_nacionalidade", "string", "País correspondente à nacionalidade, na mesma grafia usada em dim_circuito.pais.", "Ex.: 'UK', 'Brazil'", "silver.ref_nacionalidade_pais"),
        ],
    },
    "gold.dim_equipe": {
        "tipo": "dimensão",
        "descricao": "Uma linha por equipe (construtor) da história da F1.",
        "pk": ["equipe_id"],
        "fk": {},
        "colunas": [
            ("equipe_id", "int", "Identificador da equipe (PK).", "Inteiro ≥ 1, único", "constructors.constructorId"),
            ("nome", "string", "Nome da equipe.", "Ex.: 'Ferrari', 'McLaren'", "constructors.name"),
            ("nacionalidade", "string", "Nacionalidade da equipe.", "24 valores, ex.: 'Italian', 'British'", "constructors.nationality"),
        ],
    },
    "gold.dim_circuito": {
        "tipo": "dimensão",
        "descricao": "Uma linha por circuito que já recebeu corrida do Mundial.",
        "pk": ["circuito_id"],
        "fk": {},
        "colunas": [
            ("circuito_id", "int", "Identificador do circuito (PK).", "Inteiro ≥ 1, único", "circuits.circuitId"),
            ("nome", "string", "Nome do circuito.", "Texto", "circuits.name"),
            ("cidade", "string", "Cidade/localidade.", "Texto", "circuits.location"),
            ("pais", "string", "País (padronizado: 'United States' → 'USA').", "~35 países", "circuits.country"),
            ("latitude", "double", "Latitude em graus decimais.", "-90 a 90", "circuits.lat"),
            ("longitude", "double", "Longitude em graus decimais.", "-180 a 180", "circuits.lng"),
            ("altitude_m", "int", "Altitude em metros.", "-10 a 2 300", "circuits.alt"),
        ],
    },
    "gold.dim_status": {
        "tipo": "dimensão",
        "descricao": "Uma linha por status final possível, com agrupamento em categorias de análise.",
        "pk": ["status_id"],
        "fk": {},
        "colunas": [
            ("status_id", "int", "Identificador do status (PK).", "Inteiro ≥ 1, único", "status.statusId"),
            ("descricao", "string", "Descrição original do status.", "~140 valores, ex.: 'Engine', '+1 Lap'", "status.status"),
            ("categoria", "string", "Agrupamento para análise.", "Finalizou, Mecânica, Acidente/Incidente, Desclassificado, Não largou, Não classificado, Piloto/Segurança, Abandono não especificado", "Derivado por regra sobre status.status"),
        ],
    },
    # =====================================================================================
    # SILVER
    # =====================================================================================
    "silver.circuitos": {
        "tipo": "silver", "descricao": "Circuitos limpos e tipados.", "pk": ["circuito_id"], "fk": {},
        "colunas": [
            ("circuito_id", "int", "Identificador do circuito.", "Inteiro ≥ 1", "circuits.circuitId"),
            ("circuito_ref", "string", "Chave textual do circuito na fonte.", "Texto", "circuits.circuitRef"),
            ("nome", "string", "Nome do circuito.", "Texto", "circuits.name"),
            ("cidade", "string", "Cidade.", "Texto", "circuits.location"),
            ("pais", "string", "País padronizado.", "~35 valores", "circuits.country"),
            ("latitude", "double", "Latitude.", "-90 a 90", "circuits.lat"),
            ("longitude", "double", "Longitude.", "-180 a 180", "circuits.lng"),
            ("altitude_m", "int", "Altitude em metros.", "-10 a 2 300", "circuits.alt"),
            ("url_wikipedia", "string", "Página na Wikipédia.", "URL", "circuits.url"),
            ("_processado_em", "timestamp", "Data/hora do processamento Silver.", "Timestamp", "Pipeline"),
        ],
    },
    "silver.equipes": {
        "tipo": "silver", "descricao": "Equipes (construtores) limpas e tipadas.", "pk": ["equipe_id"], "fk": {},
        "colunas": [
            ("equipe_id", "int", "Identificador da equipe.", "Inteiro ≥ 1", "constructors.constructorId"),
            ("equipe_ref", "string", "Chave textual da equipe na fonte.", "Texto", "constructors.constructorRef"),
            ("nome", "string", "Nome da equipe.", "Texto", "constructors.name"),
            ("nacionalidade", "string", "Nacionalidade da equipe.", "24 valores", "constructors.nationality"),
            ("url_wikipedia", "string", "Página na Wikipédia.", "URL", "constructors.url"),
            ("_processado_em", "timestamp", "Data/hora do processamento Silver.", "Timestamp", "Pipeline"),
        ],
    },
    "silver.pilotos": {
        "tipo": "silver", "descricao": "Pilotos limpos, tipados e com nacionalidade padronizada.", "pk": ["piloto_id"], "fk": {},
        "colunas": [
            ("piloto_id", "int", "Identificador do piloto.", "Inteiro ≥ 1", "drivers.driverId"),
            ("piloto_ref", "string", "Chave textual do piloto na fonte.", "Texto", "drivers.driverRef"),
            ("numero_permanente", "int", "Número permanente.", "1 a 99; NULL", "drivers.number"),
            ("codigo", "string", "Código de três letras.", "3 letras; NULL", "drivers.code"),
            ("nome", "string", "Primeiro nome.", "Texto", "drivers.forename"),
            ("sobrenome", "string", "Sobrenome.", "Texto", "drivers.surname"),
            ("nome_completo", "string", "Nome completo.", "Texto", "forename + surname"),
            ("data_nascimento", "date", "Data de nascimento.", "1896 a 2008", "drivers.dob"),
            ("nacionalidade", "string", "Nacionalidade padronizada.", "42 valores", "drivers.nationality"),
            ("url_wikipedia", "string", "Página na Wikipédia.", "URL", "drivers.url"),
            ("_processado_em", "timestamp", "Data/hora do processamento Silver.", "Timestamp", "Pipeline"),
        ],
    },
    "silver.corridas": {
        "tipo": "silver", "descricao": "Calendário de corridas limpo e tipado.", "pk": ["corrida_id"], "fk": {},
        "colunas": [
            ("corrida_id", "int", "Identificador da corrida.", "Inteiro ≥ 1", "races.raceId"),
            ("ano", "int", "Temporada.", "1950 a ano atual", "races.year"),
            ("rodada", "int", "Etapa.", "1 a 24", "races.round"),
            ("circuito_id", "int", "Circuito.", "Existente em circuitos", "races.circuitId"),
            ("nome_gp", "string", "Nome do GP.", "Texto", "races.name"),
            ("data", "date", "Data da corrida.", "1950 em diante", "races.date"),
            ("hora_utc", "string", "Horário de largada (UTC), HH:MM:SS.", "NULL antes de 2005", "races.time"),
            ("data_sprint", "date", "Data da sprint, se houver.", "NULL antes de 2021", "races.sprint_date"),
            ("url_wikipedia", "string", "Página na Wikipédia.", "URL", "races.url"),
            ("_processado_em", "timestamp", "Data/hora do processamento Silver.", "Timestamp", "Pipeline"),
        ],
    },
    "silver.status": {
        "tipo": "silver", "descricao": "Status com categoria de análise.", "pk": ["status_id"], "fk": {},
        "colunas": [
            ("status_id", "int", "Identificador do status.", "Inteiro ≥ 1", "status.statusId"),
            ("descricao", "string", "Descrição original.", "~140 valores", "status.status"),
            ("categoria", "string", "Categoria de análise.", "8 categorias", "Regra sobre status.status"),
            ("_processado_em", "timestamp", "Data/hora do processamento Silver.", "Timestamp", "Pipeline"),
        ],
    },
    "silver.resultados": {
        "tipo": "silver", "descricao": "Resultados de corrida limpos e tipados, com tempos convertidos para milissegundos.", "pk": ["resultado_id"], "fk": {},
        "colunas": [
            ("resultado_id", "int", "Identificador do resultado.", "Inteiro ≥ 1", "results.resultId"),
            ("corrida_id", "int", "Corrida.", "Existente em corridas", "results.raceId"),
            ("piloto_id", "int", "Piloto.", "Existente em pilotos", "results.driverId"),
            ("equipe_id", "int", "Equipe.", "Existente em equipes", "results.constructorId"),
            ("numero_carro", "string", "Número do carro na corrida.", "Texto numérico", "results.number"),
            ("grid", "int", "Posição de largada (0 = pit lane/sem posição).", "0 a 34", "results.grid"),
            ("posicao", "int", "Posição final oficial.", "1 a 33; NULL", "results.position / positionText"),
            ("posicao_texto", "string", "Posição ou código (R=abandono, D=desclassificado, W=retirou-se, N=não classificado, F=não se classificou, E=excluído).", "Número ou R/D/W/N/F/E", "results.positionText"),
            ("posicao_ordem", "int", "Ordem final incluindo não classificados.", "1 a 39", "results.positionOrder"),
            ("pontos", "double", "Pontos oficiais.", "0 a 50", "results.points"),
            ("voltas", "int", "Voltas completadas.", "0 a 200", "results.laps"),
            ("tempo_total_ms", "bigint", "Tempo total de prova em ms.", "> 0; NULL", "results.milliseconds"),
            ("volta_mais_rapida", "int", "Número da volta mais rápida do piloto.", "≥ 1; NULL", "results.fastestLap"),
            ("rank_volta_rapida", "int", "Ranking da volta mais rápida na corrida.", "0 a 24; NULL", "results.rank"),
            ("tempo_volta_rapida_ms", "bigint", "Tempo da melhor volta em ms.", "> 0; NULL", "results.fastestLapTime"),
            ("velocidade_volta_rapida_kmh", "double", "Velocidade média da melhor volta (km/h).", "~90 a 260; NULL", "results.fastestLapSpeed"),
            ("status_id", "int", "Status final.", "Existente em status", "results.statusId"),
            ("_processado_em", "timestamp", "Data/hora do processamento Silver.", "Timestamp", "Pipeline"),
        ],
    },
    "silver.pit_stops": {
        "tipo": "silver", "descricao": "Paradas de box com duração em milissegundos e marcação de paradas atípicas.", "pk": ["corrida_id", "piloto_id", "numero_parada"], "fk": {},
        "colunas": [
            ("corrida_id", "int", "Corrida.", "Existente em corridas", "pit_stops.raceId"),
            ("piloto_id", "int", "Piloto.", "Existente em pilotos", "pit_stops.driverId"),
            ("numero_parada", "int", "Número da parada.", "1 a 8", "pit_stops.stop"),
            ("volta", "int", "Volta da parada.", "≥ 1", "pit_stops.lap"),
            ("hora_local", "string", "Horário local da parada (HH:MM:SS).", "Texto", "pit_stops.time"),
            ("duracao_ms", "bigint", "Tempo no pit lane em ms.", "> 0", "pit_stops.milliseconds (ou duration convertida)"),
            ("parada_atipica", "boolean", "Duração acima de 60 s.", "true/false", "Derivado"),
            ("_processado_em", "timestamp", "Data/hora do processamento Silver.", "Timestamp", "Pipeline"),
        ],
    },
    "silver.ref_nacionalidade_pais": {
        "tipo": "silver", "descricao": "Tabela de referência criada manualmente: nacionalidade do piloto → país na grafia de circuits.country.", "pk": ["nacionalidade"], "fk": {},
        "colunas": [
            ("nacionalidade", "string", "Nacionalidade como em silver.pilotos.", "42 valores", "Curadoria manual"),
            ("pais", "string", "País correspondente.", "Grafia de circuits.country", "Curadoria manual"),
            ("_processado_em", "timestamp", "Data/hora do processamento Silver.", "Timestamp", "Pipeline"),
        ],
    },
}

# Relacionamentos da Silver também são validados antes de montar os fatos.
CATALOGO["silver.corridas"]["fk"] = {"circuito_id": "silver.circuitos"}
CATALOGO["silver.resultados"]["fk"] = {
    "corrida_id": "silver.corridas", "piloto_id": "silver.pilotos",
    "equipe_id": "silver.equipes", "status_id": "silver.status",
}
CATALOGO["silver.pit_stops"]["fk"] = {"corrida_id": "silver.corridas", "piloto_id": "silver.pilotos"}
