# 📁 Reorganização de Estrutura - Output ANVISA

## ✅ Mudanças Realizadas

A partir de agora, os outputs dos dois pipelines estão separados:

### 1. **Pipeline ANVISA (Construtor de Base)**
   - **Entrada**: `data/processed/base_anvisa_precos_vigencias.csv`
   - **Saída**: `output/anvisa/`
   - **Arquivos**:
     - `baseANVISA.csv` - Base consolidada (input para NFe pipeline)
     - `baseANVISA_dtypes.json` - Tipos de dados
     - `dfprodutos.csv` - Dataset completo
     - `dfpro_correcao_manual.xlsx` - Para análise manual
     - `principios_ativos_unicos.txt` - Lista de ativos
     - `produtos_unicos.txt` - Lista de produtos (movido via script anterior)

### 2. **Pipeline NFe (Matching)**
   - **Entrada**: `output/anvisa/baseANVISA.csv`
   - **Saída**: `data/processed/` (etapas 01-13)
   - **Arquivos**:
     - `nfe_etapa01_processado.csv`
     - `nfe_etapa03_limpo.csv`
     - `nfe_etapa04_enriquecido.csv`
     - `nfe_etapa07_matched.csv`
     - `nfe_etapa08_matched_manual.csv`
     - `df_etapa09_*.zip` ... `df_etapa13_*.zip`

## 📝 Arquivos Modificados

1. **`src/config.py`**
   - `ARQUIVO_SAIDA` → `output/anvisa/baseANVISA.csv`

2. **`src/modules/finalizacao.py`**
   - `exportar_para_pipeline()` → `output/anvisa/baseANVISA.csv`
   - `exportar_completo()` → `output/anvisa/dfprodutos.csv`
   - `exportar_para_analise_manual()` → `output/anvisa/dfpro_correcao_manual.xlsx`

3. **`src/modules/principio_ativo.py`**
   - `exportar_principios_ativos_unicos()` → `output/anvisa/principios_ativos_unicos.txt`

4. **`src/modules/produto.py`**
   - `exportar_produtos_unicos()` → `output/anvisa/produtos_unicos.txt`

5. **`src/nfe_unificacao_matching.py`**
   - Leitura de base ANVISA → `output/anvisa/baseANVISA.csv`

6. **`reprocessar_base_anvisa.py`**
   - Todas as referências → `output/anvisa/`

## 🚀 Como Usar

### Regenerar base ANVISA
```bash
python src/processar_dados.py
```
Outputs vão para `output/anvisa/`

### Usar base ANVISA no pipeline NFe
```bash
python main_nfe.py
```
Lê automaticamente de `output/anvisa/baseANVISA.csv`

## 📊 Resultado da Reorganização

```
output/
├── anvisa/              ← ANVISA pipeline outputs
│   ├── baseANVISA.csv
│   ├── baseANVISA_dtypes.json
│   ├── dfprodutos.csv
│   ├── dfpro_correcao_manual.xlsx
│   ├── principios_ativos_unicos.txt
│   └── baseANVISA_backup_*.csv
│
└── (NFe outputs no futuro - em data/processed/)
```

## ✨ Benefícios

- ✅ Separação clara de responsabilidades
- ✅ Fácil localização de outputs
- ✅ Melhor rastreabilidade
- ✅ Preparação para possíveis dashboards ou relatórios específicos por pipeline
