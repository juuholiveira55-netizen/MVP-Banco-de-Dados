"""
Configuração compartilhada por todos os notebooks do MVP (Fórmula 1 - Databricks).

Este arquivo é um módulo Python comum (não é notebook). Os notebooks o importam com
`from config import *`, o que funciona porque, em uma Git folder do Databricks, o
diretório do notebook entra automaticamente no sys.path.
"""
import os
import re
from pathlib import Path

# Detecta se o código está rodando no Databricks ou localmente (testes)
# O destino deste projeto é o Databricks, inclusive serverless, onde variáveis
# de cluster podem não existir. Testes locais devem optar explicitamente por local.
AMBIENTE = os.environ.get("F1_AMBIENTE", "databricks")
if AMBIENTE not in {"local", "databricks"}:
    raise ValueError("F1_AMBIENTE deve ser local ou databricks")
EM_DATABRICKS = AMBIENTE == "databricks"

# ---------------------------------------------------------------------------
# Unity Catalog
# ---------------------------------------------------------------------------
# Catálogo preferido. Se a conta não permitir CREATE CATALOG, o pipeline usa o
# catálogo padrão "workspace" do Databricks Free Edition.
CATALOGO_PREFERIDO = os.environ.get("F1_CATALOGO", "f1_mvp")
CATALOGO_FALLBACK = "workspace"
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOGO_PREFERIDO):
    raise ValueError("F1_CATALOGO deve ser um identificador SQL simples")

SCHEMAS = {
    "bronze": "Camada Bronze: dados brutos do dataset F1 exatamente como recebidos (todas as colunas como texto) + metadados de ingestão.",
    "silver": "Camada Silver: dados limpos, tipados, deduplicados e padronizados, com nomes de colunas em português.",
    "gold": "Camada Gold: modelo estrela (fatos e dimensões) pronto para responder às perguntas de negócio.",
    "qualidade": "Resultados das verificações de qualidade de dados (perfil da Bronze e validações da Silver).",
}

VOLUME = "arquivos_brutos"          # volume do Unity Catalog onde os CSVs são enviados
SUBPASTA_VOLUME = "f1"              # /Volumes/<catalogo>/bronze/arquivos_brutos/f1/

# ---------------------------------------------------------------------------
# Fonte dos dados
# ---------------------------------------------------------------------------
FONTE_DADOS = "Kaggle - rohanrao/formula-1-world-championship-1950-2020 (origem: Ergast Developer API)"

ARQUIVOS_FONTE = [
    "circuits", "constructor_results", "constructor_standings", "constructors",
    "driver_standings", "drivers", "lap_times", "pit_stops", "qualifying",
    "races", "results", "seasons", "sprint_results", "status",
]

# Última temporada COMPLETA considerada nas análises (a versão do Kaggle vai até 2024)
ANO_FIM_ANALISE = 2024

# Formato de armazenamento: Delta Lake no Databricks, Parquet nos testes locais
FORMATO = "delta" if EM_DATABRICKS else "parquet"


def preparar_ambiente(spark):
    """Cria/seleciona catálogo e schemas. Retorna o nome do catálogo em uso."""
    if not EM_DATABRICKS:
        for schema in SCHEMAS:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        return "spark_catalog"

    catalogo = CATALOGO_PREFERIDO
    # USE primeiro: não exige privilégio de criação em um catálogo existente.
    try:
        spark.sql(f"USE CATALOG {catalogo}")
    except Exception as erro:
        classe = (getattr(erro, "getErrorClass", lambda: "")() or "")
        if "CATALOG_NOT_FOUND" not in classe and "NO_SUCH_CATALOG" not in classe:
            raise
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalogo} COMMENT 'MVP Engenharia de Dados PUC-Rio - Pipeline F1'")
        except Exception as criacao:
            classe = (getattr(criacao, "getErrorClass", lambda: "")() or "")
            if "INSUFFICIENT_PERMISSIONS" not in classe and "PERMISSION_DENIED" not in classe:
                raise
            if "F1_CATALOGO" in os.environ:
                raise  # catálogo explicitamente escolhido não deve mudar silenciosamente
            print(f"Sem permissão para criar {catalogo}; usando {CATALOGO_FALLBACK}.")
            catalogo = CATALOGO_FALLBACK

    spark.sql(f"USE CATALOG {catalogo}")
    for schema, comentario in SCHEMAS.items():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT '{comentario}'")
    return catalogo


def caminho_arquivos(catalogo):
    """Pasta onde estão os CSVs brutos."""
    if EM_DATABRICKS:
        return f"/Volumes/{catalogo}/bronze/{VOLUME}/{SUBPASTA_VOLUME}"
    return os.environ.get("F1_DADOS_LOCAIS", str(Path(__file__).resolve().parents[1] / "archive"))


def escapar(texto):
    """Escapa aspas simples para uso em literais SQL."""
    return texto.replace("\\", "\\\\").replace("'", "\\'")


def salvar_tabela(df, nome_tabela, comentario=None, spark=None):
    """Grava um DataFrame como tabela gerenciada (Delta), sobrescrevendo dados e schema."""
    (df.write.format(FORMATO)
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(nome_tabela))
    if comentario and spark is not None and EM_DATABRICKS:
        spark.sql(f"COMMENT ON TABLE {nome_tabela} IS '{escapar(comentario)}'")
    return nome_tabela
