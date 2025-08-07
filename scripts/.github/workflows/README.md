# Options Extractor

Como usar (sem instalar nada):

1. Vá em "Actions" → "Process Options Data" → "Run workflow".
2. No campo "download_url", cole o link direto do arquivo (.zip ou .txt). Se for .zip, o workflow descompacta sozinho.
3. Aguarde terminar (uns minutos). No fim, clique no job → "Artifacts" → baixe `processed-csvs.zip` com os CSVs prontos.

Regras de filtro:
- QUOTE_READTIME: apenas 09:30 e 15:45
- DTE: 0 a 14
- Para cada (QUOTE_READTIME, EXPIRE_DATE): 10 strikes abaixo e 10 acima mais próximos do UNDERLYING_LAST.
