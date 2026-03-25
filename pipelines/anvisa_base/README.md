# Pipeline ANVISA

## Execucao recomendada

```bash
python 1_download_anvisa.py
```

Esse comando executa o fluxo completo:

1. download e consolidacao bruta (1.0),
2. processamento e engenharia (1.5),
3. processamento avancado (2B).

## Estrutura

- `workflows/`: implementacao canonica das etapas 1.0 e 1.5.
- `src/`: processamento avancado (2B) e modulos de engenharia.
- `scripts/`: wrappers de compatibilidade para integracoes antigas.
- `main.py`: orquestrador unico do pipeline ANVISA.
- `download.py`: executa apenas download/consolidacao (1.0).
- `config_anvisa.py`: configuracoes de periodo, paralelismo e paths.

## Flags uteis

- `python 1_download_anvisa.py --skip-download`: reutiliza arquivos ja baixados.
- `python 1_download_anvisa.py --force-refresh`: limpa `data/raw` antes de baixar.

