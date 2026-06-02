# Pipeline ANVISA

Fluxo recomendado a partir da raiz:

```powershell
python scripts/run_anvisa_completo.py
```

Esse comando executa:

1. download e consolidacao bruta;
2. processamento e engenharia;
3. processamento avancado que gera `output/anvisa/baseANVISA.csv`.

## Estrutura

- `main.py`: orquestrador unico do pipeline ANVISA.
- `download.py`: funcao de download/consolidacao bruta.
- `workflows/`: etapas canonicas de download e engenharia.
- `src/`: processamento avancado e modulos de tratamento.
- `scripts/`: wrappers internos/compatibilidade.
- `config_anvisa.py`: periodo, paralelismo e caminhos.

## Comandos uteis

```powershell
python scripts/run_anvisa_completo.py --skip-download
python scripts/run_anvisa_completo.py --force-refresh
python scripts/run_anvisa_apenas_download.py
```
