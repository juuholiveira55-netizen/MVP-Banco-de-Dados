"""Renderiza tabelas exportadas do Job Databricks em imagens legíveis.

Fonte: evidencias_job.json, extraído com `databricks jobs export-run`.
As imagens são apresentações dos resultados reais, não capturas do Catalog Explorer.
"""

import json
import textwrap
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parent
DATA = json.loads((ROOT / "evidencias_job.json").read_text(encoding="utf-8"))
OUT = ROOT / "img"
FONT_PATH = Path("C:/Windows/Fonts/arial.ttf")
if not FONT_PATH.exists():
    FONT_PATH = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
FONT = ImageFont.truetype(str(FONT_PATH), 18)
BOLD_PATH = str(FONT_PATH).replace("arial.ttf", "arialbd.ttf")
BOLD = ImageFont.truetype(BOLD_PATH if Path(BOLD_PATH).exists() else str(FONT_PATH), 20)
TITLE = ImageFont.truetype(BOLD_PATH if Path(BOLD_PATH).exists() else str(FONT_PATH), 26)


def result(stage, command, rows=None, columns=None):
    item = DATA["etapas"][stage]["resultados"][str(command)]
    names = item["colunas"]
    values = item["linhas"] if rows is None else rows
    if columns is not None:
        idx = [names.index(x) for x in columns]
        names = columns
        values = [[row[i] for i in idx] for row in values]
    return names, values


def render(name, heading, sections):
    width = 1800
    margin = 32
    gap = 30
    def lines(value, cell_width):
        return textwrap.wrap(str(value).replace("None", "—").replace("_", " "), width=max(9, int(cell_width / 10))) or [""]

    layouts = []
    height = 115
    for caption, (headers, rows) in sections:
        cell_width = (width - margin * 2) / len(headers)
        header_lines = [lines(x, cell_width) for x in headers]
        body_lines = [[lines(x, cell_width) for x in row] for row in rows]
        header_height = max(len(x) for x in header_lines) * 24 + 10
        body_heights = [max(len(x) for x in row) * 24 + 8 for row in body_lines]
        layouts.append((caption, header_lines, body_lines, header_height, body_heights, cell_width))
        height += 40 + header_height + sum(body_heights) + gap
    im = Image.new("RGB", (width, height), "#ffffff")
    draw = ImageDraw.Draw(im)
    draw.rectangle((0, 0, width, 74), fill="#123250")
    draw.text((margin, 20), heading, font=TITLE, fill="white")
    y = 92
    for caption, headers, rows, header_height, body_heights, cell_width in layouts:
        draw.text((margin, y), caption, font=BOLD, fill="#123250")
        y += 40
        draw.rectangle((margin, y, width - margin, y + header_height), fill="#dceaf2")
        for j, cell in enumerate(headers):
            for k, value in enumerate(cell):
                draw.text((margin + j * cell_width + 5, y + 4 + k * 24), value, font=FONT, fill="#123250")
        y += header_height
        for i, row in enumerate(rows):
            if i % 2:
                draw.rectangle((margin, y, width - margin, y + body_heights[i]), fill="#f2f6f8")
            for j, cell in enumerate(row):
                for k, value in enumerate(cell):
                    draw.text((margin + j * cell_width + 5, y + 4 + k * 24), value, font=FONT, fill="#1c2833")
            y += body_heights[i]
        y += gap
    draw.text((margin, height - 28), f"Fonte: Databricks Job {DATA['job_id']} · execução {DATA['run_id']} · saída exportada via CLI", font=FONT, fill="#566573")
    im.save(OUT / name)


render("03_bronze_resumo_carga.png", "Carga Bronze · 14 tabelas", [("Linhas e colunas carregadas", result("bronze", 4))])
render("04_qualidade_regras.png", "Diagnóstico de qualidade · Bronze", [("Regras avaliadas", result("qualidade", 7))])
render("05_qualidade_outliers.png", "Diagnóstico de outliers · pit stops", [
    ("Estatísticas da duração", result("qualidade", 9)),
    ("Distribuição por faixa", result("qualidade", 10)),
])
render("06_silver_validacoes.png", "Validações após transformação · Silver", [("Regras avaliadas", result("silver", 23))])
gold_checks = result("gold", 10)
gold_summary = [["Regras", len(gold_checks[1])], ["Com violações", sum(row[1] > 0 for row in gold_checks[1])]]
render("07_gold_tabelas.png", "Modelo Gold · tabelas persistidas", [
    ("Contagem de linhas", result("gold", 9)),
    ("Resumo das validações", (["métrica", "valor"], gold_summary)),
])
render("12_p2_grid_chegada.png", "P2 · largada e chegada", [
    ("Correlação por era", result("analise", 7)),
    ("Circuitos da era recente", result("analise", 8)),
])
render("14_p4_pit_stops.png", "P4 · duração de pit stops", [
    ("Evolução anual", result("analise", 13)),
    ("Equipes em 2022–2024", result("analise", 15)),
])
render("16_p6_fator_casa.png", "P6 · fator casa", [
    ("Comparação pareada", result("analise", 21)),
    ("Teste t", result("analise", 23)),
])
