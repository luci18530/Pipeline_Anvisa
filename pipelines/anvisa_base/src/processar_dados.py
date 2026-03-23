# -*- coding: utf-8 -*-
"""
Script principal para processar os dados da Anvisa.
Orquestra todo o pipeline de processamento, executando em sequência ao baixar.py.
"""

import os
import sys
from datetime import datetime
from typing import List, Tuple

import pandas as pd
from pandas.errors import ParserError

from config import ARQUIVO_ENTRADA, ARQUIVO_SAIDA, configurar_pandas
from modules.apresentacao import criar_flag_substancia_composta, processar_apresentacao
from modules.classificacao_terapeutica import processar_classificacao_terapeutica
from modules.dosagem import processar_dosagem
from modules.finalizacao import processar_finalizacao
from modules.grupo_terapeutico import processar_grupo_terapeutico
from modules.laboratorio import processar_laboratorio
from modules.limpeza_dados import limpar_padronizar_dados
from modules.principio_ativo import exportar_principios_ativos_unicos, processar_principio_ativo
from modules.produto import exportar_produtos_unicos, processar_produto
from modules.tipo_produto import processar_tipo_produto
from modules.unificacao_vigencias import unificar_vigencias_consecutivas

MAX_BAD_LINES_RATIO = 0.005  # 0.5%
MAX_BAD_LINES_ABSOLUTE = 5000


def verificar_arquivo_entrada() -> bool:
    if not os.path.exists(ARQUIVO_ENTRADA):
        print(f"[ERRO] Arquivo '{ARQUIVO_ENTRADA}' não encontrado!")
        print("Certifique-se de executar o script 'baixar.py' primeiro.")
        return False

    print(f"[OK] Arquivo '{ARQUIVO_ENTRADA}' encontrado.")
    return True


def _ler_csv_com_telemetria(caminho: str, sep: str, encoding: str) -> Tuple[pd.DataFrame, int, List[str]]:
    try:
        df = pd.read_csv(
            caminho,
            sep=sep,
            encoding=encoding,
            on_bad_lines="error",
            low_memory=False,
        )
        return df, 0, []
    except ParserError:
        linhas_invalidas = 0
        amostra_invalidas: List[str] = []

        def _capturar_bad_line(campos: List[str]) -> None:
            nonlocal linhas_invalidas
            linhas_invalidas += 1
            if len(amostra_invalidas) < 5:
                amostra_invalidas.append(" | ".join(campos[:10]))
            return None

        df = pd.read_csv(
            caminho,
            sep=sep,
            encoding=encoding,
            on_bad_lines=_capturar_bad_line,  # type: ignore[arg-type]
            engine="python",
            low_memory=False,
        )
        return df, linhas_invalidas, amostra_invalidas


def _validar_linhas_invalidas(total_linhas: int, linhas_invalidas: int) -> None:
    if total_linhas <= 0:
        return

    ratio = linhas_invalidas / total_linhas
    if linhas_invalidas > MAX_BAD_LINES_ABSOLUTE or ratio > MAX_BAD_LINES_RATIO:
        raise ValueError(
            "Quantidade de linhas inválidas acima do limite: "
            f"{linhas_invalidas:,} ({ratio:.2%})"
        )


def carregar_dados() -> pd.DataFrame | None:
    try:
        print(f"\nCarregando dados de '{ARQUIVO_ENTRADA}'...")

        tentativas = [
            (";", "utf-8"),
            (",", "utf-8"),
            ("\t", "utf-8"),
            (";", "latin1"),
        ]

        erros: List[str] = []
        df: pd.DataFrame | None = None
        linhas_invalidas = 0
        amostra_invalidas: List[str] = []

        for sep, encoding in tentativas:
            try:
                print(f"[INFO] Tentativa de leitura: sep='{sep}' encoding='{encoding}'")
                df_tmp, bad_lines, sample = _ler_csv_com_telemetria(
                    ARQUIVO_ENTRADA, sep=sep, encoding=encoding
                )
                total_estimado = len(df_tmp) + bad_lines
                _validar_linhas_invalidas(total_estimado, bad_lines)

                df = df_tmp
                linhas_invalidas = bad_lines
                amostra_invalidas = sample
                break
            except Exception as exc:
                erros.append(f"sep='{sep}' encoding='{encoding}': {exc}")

        if df is None:
            raise ValueError(
                "Nenhuma estratégia de leitura funcionou.\n" + "\n".join(erros)
            )

        print("\n[OK] Dados carregados com sucesso!")
        if linhas_invalidas > 0:
            total_estimado = len(df) + linhas_invalidas
            pct = (linhas_invalidas / total_estimado) * 100 if total_estimado else 0
            print(
                "[AVISO] Linhas inválidas detectadas e descartadas com telemetria: "
                f"{linhas_invalidas:,} ({pct:.2f}%)"
            )
            if amostra_invalidas:
                print("[AVISO] Amostra de linhas inválidas:")
                for i, linha in enumerate(amostra_invalidas, start=1):
                    print(f"  {i}. {linha}")

        print("Informações do DataFrame:")
        df.info()
        print("\nPrimeiras 5 linhas:")
        print(df.head())

        return df
    except Exception as exc:
        print(f"[ERRO] Erro ao carregar dados: {exc}")
        return None


def salvar_dados_processados(df: pd.DataFrame) -> bool:
    try:
        print(f"\nSalvando dados processados em '{ARQUIVO_SAIDA}'...")
        df.to_csv(ARQUIVO_SAIDA, index=False, sep=";", encoding="utf-8")
        print(f"[OK] Dados salvos com sucesso em '{ARQUIVO_SAIDA}'!")
        print(f"Arquivo contém {len(df):,} registros.")
        return True
    except Exception as exc:
        print(f"[ERRO] Erro ao salvar dados: {exc}")
        return False


def exibir_estatisticas_finais(df_original: pd.DataFrame, df_processado: pd.DataFrame) -> None:
    print("\n" + "=" * 80)
    print("ESTATÍSTICAS FINAIS DO PROCESSAMENTO")
    print("=" * 80)

    print(f"Registros originais: {len(df_original):,}")
    print(f"Registros processados: {len(df_processado):,}")

    reducao = len(df_original) - len(df_processado)
    percentual = (reducao / len(df_original)) * 100 if len(df_original) > 0 else 0
    print(f"Redução: {reducao:,} registros ({percentual:.2f}%)")

    if "GRUPO ANATOMICO" in df_processado.columns:
        print(
            f"\n[OK] Coluna 'GRUPO ANATOMICO' criada com "
            f"{df_processado['GRUPO ANATOMICO'].nunique()} grupos únicos."
        )

    if "CLASSE_TERAPEUTICA_ORIGINAL" in df_processado.columns:
        print("[OK] Backup da classe terapêutica original mantido.")

    print("\nColunas no DataFrame final:")
    print(list(df_processado.columns))


def main() -> None:
    print("=" * 80)
    print("PIPELINE DE PROCESSAMENTO DOS DADOS ANVISA")
    print("=" * 80)
    print(f"Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    configurar_pandas()

    if not verificar_arquivo_entrada():
        sys.exit(1)

    df_original = carregar_dados()
    if df_original is None:
        sys.exit(1)

    df_processado = df_original.copy()

    try:
        print("\n" + "=" * 80)
        print("ETAPA 1/10: LIMPEZA E PADRONIZAÇÃO")
        print("=" * 80)
        df_processado = limpar_padronizar_dados(df_processado)

        print("\n" + "=" * 80)
        print("ETAPA 2/10: UNIFICAÇÃO DE VIGÊNCIAS")
        print("=" * 80)
        df_processado = unificar_vigencias_consecutivas(df_processado)

        print("\n" + "=" * 80)
        print("ETAPA 3/10: PROCESSAMENTO DA CLASSIFICAÇÃO TERAPÊUTICA")
        print("=" * 80)
        df_processado = processar_classificacao_terapeutica(df_processado)

        print("\n" + "=" * 80)
        print("ETAPA 4/10: PROCESSAMENTO DO PRINCÍPIO ATIVO")
        print("=" * 80)
        df_processado = processar_principio_ativo(df_processado, executar_fuzzy_matching=False)

        print("\n" + "=" * 80)
        print("ETAPA 5/10: PROCESSAMENTO DO PRODUTO")
        print("=" * 80)
        df_processado = processar_produto(df_processado)

        print("\n" + "=" * 80)
        print("ETAPA 6/10: PROCESSAMENTO DA APRESENTAÇÃO")
        print("=" * 80)
        df_processado = criar_flag_substancia_composta(df_processado)
        df_processado = processar_apresentacao(df_processado)

        print("\n" + "=" * 80)
        print("ETAPA 7/10: CATEGORIZAÇÃO E EXTRAÇÃO DE DOSAGENS")
        print("=" * 80)
        df_processado = processar_tipo_produto(df_processado)
        df_processado = processar_dosagem(df_processado, debug=False)

        print("\n" + "=" * 80)
        print("ETAPA 8/10: PROCESSAMENTO DO LABORATÓRIO")
        print("=" * 80)
        df_processado = processar_laboratorio(df_processado)

        print("\n" + "=" * 80)
        print("ETAPA 9/10: PROCESSAMENTO DO GRUPO TERAPÊUTICO")
        print("=" * 80)
        df_processado = processar_grupo_terapeutico(df_processado, criar_debug=True)

        print("\n" + "=" * 80)
        print("ETAPA 10/10: FINALIZAÇÃO E EXPORTAÇÃO")
        print("=" * 80)
        df_processado = processar_finalizacao(df_processado)

        print("\nExportando listas de referência...")
        exportar_principios_ativos_unicos(df_processado)
        exportar_produtos_unicos(df_processado)

        exibir_estatisticas_finais(df_original, df_processado)

        print("\n" + "=" * 80)
        print("[OK] PIPELINE CONCLUÍDO COM SUCESSO!")
        print("=" * 80)
        print(f"Finalizado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as exc:
        print(f"\n[ERRO] Erro durante o processamento: {exc}")
        print("\nDetalhes do erro:")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
