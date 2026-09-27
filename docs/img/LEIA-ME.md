# Evidências da execução

O Job Databricks `320932637139683`, execução `298517320094023`, terminou com sucesso após o reparo da tarefa `06_analise`.

- `01`, `02`, `08`, `09` e `10`: capturas reais do Catalog Explorer fornecidas pela autora. As partes adicionais de `08` mostram mais colunas documentadas; as de `10` mostram a continuação do gráfico de linhagem.
- `03` a `07`, `12`, `14` e `16`: tabelas reais do Job, exportadas pela CLI e renderizadas com `docs/gerar_evidencias.py` a partir de `docs/evidencias_job.json`. São apresentações dos resultados, não capturas da interface.
- `11`, `13` e `15`: gráficos PNG exportados diretamente das saídas do notebook `06_analise`.
- `17`: captura real da página da execução no Databricks, com as sete tarefas e o status `Succeeded`.

Não há dados simulados nestas figuras. O arquivo `docs/evidencias_job.json` registra o identificador da execução e os resultados tabulares que sustentam as imagens geradas.
