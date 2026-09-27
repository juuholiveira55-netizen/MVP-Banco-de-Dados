"""Gera a transcrição do Catálogo de Dados (Markdown) a partir de notebooks/catalogo_def.py.

Uso:  python docs/gerar_catalogo_md.py   -> atualiza o trecho entre os marcadores no README.md
"""
import os, re, sys

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, "notebooks"))
from catalogo_def import CATALOGO  # noqa: E402


def tabela_md(nome, meta):
    pk = ", ".join(meta["pk"])
    fks = ", ".join(f"`{c}` → `{d}`" for c, d in meta["fk"].items()) or "—"
    linhas = [f"#### `{nome}`", "", meta["descricao"], "",
              f"**Chave primária:** `{pk}` · **Chaves estrangeiras:** {fks}", "",
              "| Coluna | Tipo | Descrição | Domínio esperado | Linhagem (origem) |",
              "|---|---|---|---|---|"]
    for c, t, d, dom, lin in meta["colunas"]:
        linhas.append(f"| `{c}` | {t} | {d} | {dom} | {lin} |")
    return "\n".join(linhas) + "\n"


def gerar():
    gold = [tabela_md(n, m) for n, m in CATALOGO.items() if n.startswith("gold.")]
    silver = [tabela_md(n, m) for n, m in CATALOGO.items() if n.startswith("silver.")]
    return ("\n".join(gold)
            + "\n<details>\n<summary><b>Catálogo das tabelas Silver (clique para expandir)</b></summary>\n\n"
            + "\n".join(silver) + "\n</details>\n")


if __name__ == "__main__":
    caminho = os.path.join(RAIZ, "README.md")
    readme = open(caminho, encoding="utf-8").read()
    novo = re.sub(r"(<!-- CATALOGO:INICIO -->\n).*?(<!-- CATALOGO:FIM -->)",
                  lambda m: m.group(1) + gerar() + "\n" + m.group(2), readme, flags=re.S)
    open(caminho, "w", encoding="utf-8").write(novo)
    print("Catálogo atualizado no README.md")
