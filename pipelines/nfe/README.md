# 🚀 Pipeline Principal de NFe - main_nfe.py

Script orquestrador que executa **todo o pipeline de processamento de Notas Fiscais em uma única execução**.

## ⚡ Uso Rápido

```bash
python main_nfe.py
```

É isso! O script vai:
1. ✅ Validar se o arquivo de entrada existe
2. ✅ Executar todas as etapas automaticamente
3. ✅ Validar dados em cada etapa
4. ✅ Gerar relatório final completo

## 📋 Etapas Executadas

### Etapa 1: Carregamento e Pré-processamento
- Carrega arquivo CSV com detecção automática de encoding
- Remove BOMs e caracteres especiais
- Normaliza colunas
- Filtra datas inválidas
- Converte tipos de dados
- **Saída**: `nfe_processado_*.parquet`

### Etapa 2: Processamento de Vencimento
- Limpa e padroniza datas
- Calcula métricas de vida útil
- Categoriza status de vencimento (5 categorias)
- Particiona dados para análise
- **Saída**: `nfe_vencimento_*.parquet`

### Etapas Futuras
(Virão aqui conforme implementadas)

## 📊 Exemplo de Saída

```
██████████████████████████████████████████████████████████████████████
█               PIPELINE COMPLETO DE NOTAS FISCAIS (NFe)            █
██████████████████████████████████████████████████████████████████████

Início: 2025-11-13 09:51:32

============================================================
ETAPA 1: CARREGAMENTO E PRÉ-PROCESSAMENTO
============================================================
[✅ SUCESSO] (3.3s)

============================================================
ETAPA 2: PROCESSAMENTO DE VENCIMENTO
============================================================
[✅ SUCESSO] (2.3s)

======================================================================
               RELATÓRIO FINAL DO PIPELINE
======================================================================

✅ [1] Carregamento e Pré-processamento                      3.3s
✅ [2] Processamento de Vencimento                           2.3s

✅ Nenhum erro encontrado!

Arquivos Gerados:
  📄 nfe_processado_20251113_095134.parquet             (   2.3 MB)
  📄 nfe_vencimento_20251113_095136.parquet             (   0.9 MB)

Tempo Total de Execução: 0.1 minutos (6 segundos)

🎉 PIPELINE CONCLUÍDO COM SUCESSO! 🎉
```

## 📁 Pré-requisitos

### Arquivo de Entrada
Coloque seu arquivo CSV de NFe em:
```
nfe/nfe.csv
```

### Estrutura Esperada
- 21 colunas (será expandido para 24 após processamento)
- Separador: `;` (ponto-e-vírgula)
- Encoding: latin1 (detectado automaticamente)
- Opcional: com ou sem cabeçalho (será adicionado se necessário)

## ⚙️ Recursos

### Validação Automática
- Valida dados após cada etapa
- Verifica tipos de dados
- Detecta valores inválidos
- Gera estatísticas detalhadas

### Relatório Final
- Tempo de execução por etapa
- Lista de todos os arquivos gerados
- Tamanho dos arquivos em MB
- Resumo de erros (se houver)
- Status geral do pipeline

### Tratamento de Erros
- Captura exceções em cada etapa
- Interrompe graciosamente em caso de erro
- Fornece mensagens de erro claras
- Retorna código de saída apropriado (0 = sucesso, 1 = erro)

## 🔧 Customização

### Adicionar Nova Etapa

Edit `main_nfe.py` e adicione um método `etapa_N_`:

```python
def etapa_3_limpeza_nomes(self):
    """Etapa 3: Limpeza de nomes de produtos"""
    inicio = datetime.now()
    
    print("\n" + "="*60)
    print("ETAPA 3: LIMPEZA DE NOMES")
    print("="*60)
    
    try:
        # Seu código aqui
        sucesso = self.executar_script(
            "scripts/processar_limpeza.py",
            "Limpeza de Nomes"
        )
        
        if not sucesso:
            raise Exception("Script de limpeza falhou")
        
        # ...resto do código
        duracao = (datetime.now() - inicio).total_seconds()
        self.log_etapa(3, "Limpeza de Nomes", "SUCESSO", duracao)
        return True
        
    except Exception as e:
        duracao = (datetime.now() - inicio).total_seconds()
        self.log_etapa(3, "Limpeza de Nomes", "ERRO", duracao)
        self.log_erro("Etapa 3", str(e))
        return False
```

Depois, adicione à lista de etapas em `executar()`:

```python
etapas = [
    ("Carregamento e Pré-processamento", self.etapa_1_carregamento),
    ("Processamento de Vencimento", self.etapa_2_vencimento),
    ("Limpeza de Nomes", self.etapa_3_limpeza_nomes),  # ← Nova etapa
]
```

### Mudar Timeout

Edit o `timeout` em `executar_script()` (padrão: 600 segundos = 10 minutos):

```python
timeout=1200  # 20 minutos
```

## 📊 Saída Esperada

### Arquivos Gerados
- `data/processed/nfe_processado_YYYYMMDD_HHMMSS.parquet` (2-3 MB)
- `data/processed/nfe_processado_YYYYMMDD_HHMMSS.csv` (5-8 MB)
- `data/processed/nfe_vencimento_YYYYMMDD_HHMMSS.parquet` (0.8-1 MB)
- `data/processed/nfe_vencimento_YYYYMMDD_HHMMSS.csv` (2-3 MB)

### Tempo de Execução
- Etapa 1: ~3-5 segundos
- Etapa 2: ~2-3 segundos
- **Total**: ~5-8 segundos (para 46k registros)

### Estatísticas
```
Total de registros: 46.389
Período: 2025-10-01 a 2025-10-30

Vencimento:
  - PRAZO ACEITAVEL: 58,5%
  - INDETERMINADO: 28,1%
  - PROXIMO AO VENCIMENTO: 10,9%
  - MUITO PROXIMO: 2,4%
  - VENCIDO: 0,1%
```

## 🚨 Troubleshooting

### Erro: "Arquivo 'nfe/nfe.csv' não encontrado!"
```
Solução: Coloque seu arquivo CSV em nfe/nfe.csv
```

### Erro: "Script de carregamento falhou"
```
Solução: Verifique:
  1. O arquivo CSV está bem formatado
  2. Tem as colunas esperadas
  3. O encoding está correto (latin1)
```

### Pipeline muito lento
```
Solução:
  1. Verificar tamanho do arquivo (>500 MB?)
  2. Aumentar timeout se necessário
  3. Verificar recursos disponíveis (RAM, CPU)
```

## 📈 Próximos Passos

Novas etapas que podem ser adicionadas:
1. Limpeza de nomes de produtos
2. Matching com base ANVISA
3. Classificação terapêutica
4. Análise exploratória
5. Geração de relatórios visuais
6. Exportação para BI/Dashboard

## � Modo Pipeline Rápido

O pipeline suporta um **modo rápido** que desativa exportações intermediárias, mantendo todo o processamento em memória. Isso acelera significativamente a execução quando você só precisa do resultado final.

### Ativar via Linha de Comando
```bash
python main_nfe.py --modo-rapido
```

### Ativar via Configuração
Edite `pipeline_config.json`:
```json
{
  "pipeline": {
    "modo_rapido": true
  }
}
```

### Comportamento no Modo Rápido
- ✅ Etapas 12-21 não exportam arquivos intermediários
- ✅ Somente a etapa 21 exporta (necessário para particionamento)
- ✅ Dados fluem 100% em memória entre etapas
- ✅ Economia de tempo de I/O em disco
- ⚠️ Requer mais memória RAM disponível

## 🔧 Utilitários Centralizados

O pipeline utiliza funções centralizadas para I/O em `pipelines/common/io_utils.py`:

### Funções Disponíveis
```python
from pipelines.common.io_utils import (
    ler_csv,            # Lê CSV com detecção de encoding
    ler_csv_chunked,    # Lê CSV em chunks (arquivos grandes)
    ler_zip_csv,        # Lê CSV compactado em ZIP
    salvar_csv,         # Salva DataFrame como CSV
    salvar_zip_csv,     # Salva DataFrame como ZIP+CSV
    exportar_condicional,  # Exporta apenas se flag=True
    carregar_ou_processar, # Cache inteligente
    limpar_memoria,     # Garbage collection
)
```

### Exemplo de Uso
```python
from pipelines.common.io_utils import ler_zip_csv, salvar_zip_csv

# Ler arquivo compactado
df = ler_zip_csv("data/processed/arquivo.zip", sep=";")

# Salvar com compressão
salvar_zip_csv(df, "data/processed/saida.zip")
```

## �📝 Exemplos de Uso

### Executar pipeline completo
```bash
python main_nfe.py
```

### Verificar saída em detalhes
```bash
python main_nfe.py > log_pipeline.txt 2>&1
```

### Cronograma automático (Linux/Mac)
```bash
# Executar todo dia às 2 da manhã
0 2 * * * cd /caminho/projeto && python main_nfe.py >> logs/pipeline.log 2>&1
```

### Cronograma automático (Windows)
```powershell
# Agendar como tarefa Windows
$trigger = New-JobTrigger -Daily -At 2:00AM
Register-ScheduledJob -Name NFePipeline -Trigger $trigger -ScriptBlock {
    cd C:\caminho\projeto
    python main_nfe.py >> logs/pipeline.log 2>&1
}
```

---

**Última atualização:** Jan 30, 2026  
**Versão:** 1.1
