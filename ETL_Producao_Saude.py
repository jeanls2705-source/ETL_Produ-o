import os
import re
import json
import shutil
import sqlite3
import unicodedata
import pandas as pd
import numpy as np
import random  
from datetime import datetime
from rapidfuzz import process, fuzz

"""
SCRIPT DE ETL DE PRODUÇÃO HOSPITALAR
====================================
Este script realiza a leitura, limpeza e consolidação de planilhas de produção
hospitalar (Ambulatório, Internação, Cirurgia, etc.) que não possuem formatação padronizada.

Funcionalidades Principais:
1. Identificação automática do tipo de setor (aba do Excel) via palavras-chave.
2. Localização dinâmica de cabeçalhos (busca nas primeiras 25 linhas).
3. Correção de células mescladas (preenchimento para baixo - ffill).
4. Normalização de nomes de especialidades via Fuzzy Matching (RapidFuzz).
5. Consolidação dos dados em um arquivo Excel mestre, preservando histórico em novas abas.

Nota: Código higienizado e adaptado para fins de portfólio. Dados sensíveis e
regras de negócio específicas do cliente foram substituídas por dados fictícios/genéricos.
"""

# ==============================================================================
# 1. CONFIGURAÇÃO E DIRETÓRIOS
# ==============================================================================

class GerenciadorConfiguracao:
    """
    Gerencia as configurações estáticas, caminhos de diretórios e listas de referência
    utilizadas para normalização e limpeza de dados.
    """

    # Estrutura de pastas do projeto
    DIRETORIOS = {
        "ENTRADA": "./Entrada",          
        "PROCESSADOS": "./Processados",  
        "SAIDA": "./Saida",              
        "CONFIG": "./Config",            
        "DADOS": "./Data"                
    }

    # Caminhos completos dos arquivos principais
    ARQUIVOS = {
        "BD": os.path.join(DIRETORIOS["DADOS"], "dbProducao.db"),
        "EXCEL_FINAL": os.path.join(DIRETORIOS["SAIDA"], "Producao_Consolidada.xlsx"),
        "AUDITORIA": os.path.join(DIRETORIOS["SAIDA"], "Relatorio_Auditoria.txt"),
        "MEMORIA_JSON": os.path.join(DIRETORIOS["CONFIG"], "aprendizado.json"),
    }

    # Palavras a serem ignoradas durante a leitura das linhas (filtros de lixo)
    PALAVRAS_PARADA = [
        "HOSPITAL", "DIRETORIA", "GESTAO", "INSTITUICAO", "CODIGO",
        "PAGINA", "PAGE", "HOSPITAL_EXEMPLO", "FILIAL",
        "RELATORIO", "PERIODO", "UNIDADE", "SISTEMA",
        "TOTAL GERAL", "RESUMO GERAL", "TOTALIZADOR",
        "A PAGAR", "SALDO", "DIFERENCA"
    ]

    # Lista Mestra ("Gold Standard") para correção ortográfica de especialidades
    LISTA_OFICIAL_ESPECIALIDADES = [
        "CLINICA MEDICA", "PEDIATRIA", "CIRURGIA GERAL", "ENFERMAGEM",
        "ORTOPEDIA", "GINECOLOGIA", "OBSTETRICIA", "CARDIOLOGIA",
        "NEUROLOGIA", "PSIQUIATRIA", "DERMATOLOGIA", "INFECTOLOGIA",
        "ONCOLOGIA", "UROLOGIA", "NEFROLOGIA", "OTORRINOLARINGOLOGIA",
        "OFTALMOLOGIA", "TERAPIA INTENSIVA", "ANESTESIOLOGIA",
        "BUCO MAXILO", "NEONATOLOGIA"
    ]

    @staticmethod
    def garantir_diretorios():
        for caminho in GerenciadorConfiguracao.DIRETORIOS.values():
            if not os.path.exists(caminho):
                os.makedirs(caminho, exist_ok=True)


# ==============================================================================
# 2. INTELIGÊNCIA DE LEITURA
# ==============================================================================

class MotorDados:
    """
    Responsável pelas operações de baixo nível de limpeza e estruturação
    dos DataFrames do Pandas.
    """

    @staticmethod
    def remover_lixo(df):
        if df.empty:
            return df
        df = df.dropna(how='all')

        palavras_fatais = ["TOTAL GERAL", "RESUMO FINANCEIRO", "ASSINATURA", "DIRETOR"]
        mascara_manter = pd.Series(True, index=df.index)

        cols_checar = df.columns[:3]
        for col in cols_checar:
            str_col = df[col].astype(str).str.upper().str.strip()
            for palavra in palavras_fatais:
                mascara_manter &= ~str_col.str.match(rf'^{palavra}\s*$', na=False)
        return df[mascara_manter].copy()

    @staticmethod
    def reparar_celulas_mescladas(df, indices_cols_valor):
        """
        [Lógica Crítica] Corrige células mescladas do Excel.
        """
        if df.empty:
            return df

        if indices_cols_valor:
            primeiro_idx_val = min(indices_cols_valor)
            idx_limite = max(primeiro_idx_val, 3)
            cols_para_preencher = df.columns[:idx_limite]
        else:
            cols_para_preencher = df.columns[:3]

        for col in cols_para_preencher:
            df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)
            df[col] = df[col].ffill()

        cols_valores = [c for c in df.columns if c not in cols_para_preencher]
        for col in cols_valores:
            df[col] = df[col].replace([r'^\s*-\s*$', r'^\s*\.\s*$', r'^\s*$'], 0, regex=True)

        return df


class BuscadorColuna:
    """
    Utilitário para encontrar colunas de forma 'fuzzy' (aproximada/normalizada).
    """

    @staticmethod
    def normalizar(texto):
        if pd.isna(texto):
            return ""
        texto = str(texto)
        nfkd = unicodedata.normalize('NFKD', texto)
        limpo = nfkd.encode('ASCII', 'ignore').decode('utf-8')
        return re.sub(r'[^a-zA-Z0-9]', '', limpo).lower()

    @staticmethod
    def encontrar_col_por_candidatos(df, candidatos):
        mapa_df = {BuscadorColuna.normalizar(c): c for c in df.columns}
        for cand in candidatos:
            cand_norm = BuscadorColuna.normalizar(cand)
            if cand_norm in mapa_df:
                return mapa_df[cand_norm]
            for norm_real, nome_real in mapa_df.items():
                if cand_norm in norm_real:
                    return nome_real
        return None

    @staticmethod
    def obter_indice_col(df, nome_col):
        try:
            return df.columns.get_loc(nome_col)
        except:
            return 999


# ==============================================================================
# 3. ESTRATÉGIAS DE LEITURA (AGORA COM METADADOS)
# ==============================================================================

class EstrategiasAba:
    """
    Define as 'Regras de Negócio' para cada tipo de setor hospitalar.
    Mapeia palavras-chave da aba para as colunas que devem ser extraídas.
    """

    REGRAS = {
        "AMBULATORIO": {
            "palavras_chave": ["ambulatorio", "amb", "consulta"],
            "col_chave": ["Especialidade", "Area Medica", "Clinica"],  
            "col_meta": ["Profissional", "Sala"],  
            "metricas": [
                {"nome": "Atendimentos", "cols": ["Quantitativo de Atendimentos", "Qtd Atend", "Atendimentos"]},
                {"nome": "Consultorios", "cols": ["Quantitativo de Consultorios", "Consultorios"]}
            ]
        },
        "EXAMES": {
            "palavras_chave": ["exames", "sadt", "diagnostico", "imagem"],
            # Em alguns modelos: Coluna Codigo é o Exame (Chave), Coluna SADT é a Origem (Meta)
            "col_chave": ["Descricao Exame", "Procedimento", "Codigo Procedimento"],
            "col_meta": ["SADT", "Setor Solicitante", "Origem"],
            "metricas": [
                {"nome": "Realizado", "cols": ["Quantitativo realizado", "Qtd Realizada", "Total Exames"]}
            ]
        },
        "CENTRO_CIRURGICO": {
            "palavras_chave": ["cirurgico", "cc", "bloco"],
            # Em alguns modelos: Especialidade é a Chave Macro, Descrição é o Procedimento (Meta)
            "col_chave": ["Especialidade", "Cirurgia"],
            "col_meta": ["Descricao Procedimento", "Procedimento", "Codigo Interno"],
            "metricas": [
                {"nome": "Cir_Emergencia", "cols": ["Quantitativo de Cirurgias de Emergencia", "Emergencia"]},
                {"nome": "Cir_Eletivas", "cols": ["Quantitativo de Cirurgias Eletivas", "Eletivas"]},
                {"nome": "Salas_Cir", "cols": ["QTD. Salas Cirurgicas"]},
                {"nome": "Salas_Parto", "cols": ["QTD. Salas Partos"]}
            ]
        },
        "INTERNACAO": {
            "palavras_chave": ["internacao", "int", "hospitalar"],
            "col_chave": ["Especialidade", "Clinica"],
            "col_meta": ["CID de Internacao", "Descricao CID"],
            "metricas": [
                {"nome": "Internacoes", "cols": ["Quantitativo de Internacao", "Internacoes"]},
                {"nome": "Leitos_Op", "cols": ["QTD. Leitos Operacionais", "Leitos Operacionais"]},
                {"nome": "Leitos_Inst", "cols": ["QTD. Leitos Instalados"]}
            ]
        },
        "PRONTO_SOCORRO": {
            "palavras_chave": ["prontosocorro", "pronto socorro", "ps", "urgencia", "upa"],
            "col_chave": ["Especialidade", "Area"],
            "col_meta": ["Origem", "Procedencia"],
            "metricas": [
                {"nome": "PS_Atendimentos", "cols": ["Quantitativo de Atendimentos", "Atendimentos"]}
            ]
        }
    }

    @classmethod
    def obter_estrategia(cls, nome_aba):
        aba_norm = BuscadorColuna.normalizar(nome_aba)
        for setor, regra in cls.REGRAS.items():
            for k in regra["palavras_chave"]:
                k_norm = BuscadorColuna.normalizar(k)
                if k_norm in aba_norm:
                    return setor, regra
        return None, None


# ==============================================================================
# 4. PROCESSADOR PRINCIPAL
# ==============================================================================

class Processador:
    def __init__(self):
        self.memoria = self._carregar_memoria()

    def _carregar_memoria(self):
        caminho = GerenciadorConfiguracao.ARQUIVOS["MEMORIA_JSON"]

        if not os.path.exists(caminho):
            memoria_vazia = {"mapeamentos": {}, "ignorar": []}
            with open(caminho, 'w', encoding='utf-8') as f:
                json.dump(memoria_vazia, f, ensure_ascii=False, indent=4)
            return memoria_vazia

        with open(caminho, 'r', encoding='utf-8') as f:
            memoria = json.load(f)

        memoria.setdefault("mapeamentos", {})
        memoria.setdefault("ignorar", [])

        return memoria

    def _salvar_memoria(self):
        with open(GerenciadorConfiguracao.ARQUIVOS["MEMORIA_JSON"], 'w', encoding='utf-8') as f:
            json.dump(self.memoria, f, ensure_ascii=False, indent=4)

    def normalizar_termo(self, termo):

        if pd.isna(termo) or str(termo).strip() == "":
            return None

        termo_original = str(termo).strip()
        limpo = BuscadorColuna.normalizar(termo_original)

        if limpo in [BuscadorColuna.normalizar(p) for p in GerenciadorConfiguracao.PALAVRAS_PARADA]:
            return None

        if limpo in self.memoria["ignorar"]:
            return None

        if limpo in self.memoria["mapeamentos"]:
            return self.memoria["mapeamentos"][limpo]

        correspondencia, pontuacao, _ = process.extractOne(
            termo_original.upper(),
            GerenciadorConfiguracao.LISTA_OFICIAL_ESPECIALIDADES,
            scorer=fuzz.token_set_ratio
        )

        if pontuacao >= 92:
            return correspondencia

        if pontuacao >= 75:
            print(f"\n⚠️ Termo encontrado: '{termo_original}'")
            print(f"Sugestão: {correspondencia} (Score: {pontuacao})")
        else:
            print(f"\n❓ Termo desconhecido: '{termo_original}'")
            print(f"Sugestão mais próxima: {correspondencia} (Score: {pontuacao})")

        print("\nEscolha uma opção:")
        print("1 - Confirmar sugestão")
        print("2 - Digitar novo termo oficial")
        print("3 - Ignorar permanentemente")
        print("4 - Usar o termo atual como novo oficial")

        escolha = input("Opção: ").strip()

        if escolha == "1":
            self.memoria["mapeamentos"][limpo] = correspondencia
            self._salvar_memoria()
            return correspondencia

        elif escolha == "2":
            novo = input("Digite o termo oficial: ").strip().upper()
            self.memoria["mapeamentos"][limpo] = novo
            if novo not in GerenciadorConfiguracao.LISTA_OFICIAL_ESPECIALIDADES:
                GerenciadorConfiguracao.LISTA_OFICIAL_ESPECIALIDADES.append(novo)
            self._salvar_memoria()
            return novo

        elif escolha == "3":
            self.memoria["ignorar"].append(limpo)
            self._salvar_memoria()
            return None

        elif escolha == "4":
            oficial = termo_original.upper()
            self.memoria["mapeamentos"][limpo] = oficial
            GerenciadorConfiguracao.LISTA_OFICIAL_ESPECIALIDADES.append(oficial)
            self._salvar_memoria()
            return oficial

        return termo_original.upper()

    def processar_arquivo(self, caminho_arquivo):
        resultados = []
        nome_arquivo = os.path.basename(caminho_arquivo)
        print(f"--> Processando: {nome_arquivo}")

        try:
            with pd.ExcelFile(caminho_arquivo) as xl:
                for aba in xl.sheet_names:
                    setor, regra = EstrategiasAba.obter_estrategia(aba)
                    if not setor:
                        continue

                    try:
                        df_bruto = pd.read_excel(xl, sheet_name=aba, header=None, nrows=25)
                        idx_cabecalho = 0
                        candidatos_chave = regra["col_chave"]
                        encontrou_cabecalho = False

                        for idx, linha in df_bruto.iterrows():
                            texto_linha = linha.astype(str).str.cat(sep=' ')
                            linha_norm = BuscadorColuna.normalizar(texto_linha)

                            for k in candidatos_chave:
                                k_norm = BuscadorColuna.normalizar(k)
                                if k_norm in linha_norm:
                                    idx_cabecalho = idx
                                    encontrou_cabecalho = True
                                    break
                            if encontrou_cabecalho:
                                break

                        if not encontrou_cabecalho:
                            print(f"    [AVISO] Cabeçalho não encontrado na aba {aba}")
                            continue

                        df = pd.read_excel(xl, sheet_name=aba, header=idx_cabecalho)
                        col_chave = BuscadorColuna.encontrar_col_por_candidatos(df, regra["col_chave"])

                        col_meta = None
                        if "col_meta" in regra:
                            col_meta = BuscadorColuna.encontrar_col_por_candidatos(df, regra["col_meta"])

                        metricas_ativas = []
                        indices_val = []
                        for m in regra["metricas"]:
                            nome_c = BuscadorColuna.encontrar_col_por_candidatos(df, m["cols"])
                            if nome_c:
                                metricas_ativas.append((m["nome"], nome_c))
                                indices_val.append(BuscadorColuna.obter_indice_col(df, nome_c))

                        if not col_chave or not metricas_ativas:
                            continue

                        df = MotorDados.remover_lixo(df)
                        df = MotorDados.reparar_celulas_mescladas(df, indices_val)

                        # Substituído a variável e string 'CNES' por 'Cod_Unidade'
                        cod_unidade = df.iloc[0, 0] if not df.empty else "DESC"
                        if str(cod_unidade).lower() in ['nan', 'cod', 'codigo']:
                            cod_unidade = nome_arquivo.split('_')[0]

                        for idx, linha in df.iterrows():
                            termo_bruto = linha[col_chave]
                            termo_limpo = self.normalizar_termo(termo_bruto)
                            if not termo_limpo:
                                continue

                            val_meta = ""
                            if col_meta:
                                meta_bruta = linha[col_meta]
                                if pd.notna(meta_bruta) and str(meta_bruta).strip() != "":
                                    val_meta = str(meta_bruta).strip()

                            registro = {
                                "Cod_Unidade": cod_unidade,
                                "Arquivo": nome_arquivo,
                                "Aba": aba,
                                "Setor": setor,
                                "Item": termo_limpo,
                                "Detalhe": val_meta  
                            }

                            tem_dados = False
                            for nome_metrica, col_metrica in metricas_ativas:
                                try:
                                    val = float(linha[col_metrica])
                                except:
                                    val = 0.0
                                registro[nome_metrica] = val
                                if val > 0:
                                    tem_dados = True

                            if tem_dados:
                                resultados.append(registro)

                    except Exception as e:
                        print(f"    Erro na aba {aba}: {e}")

        except Exception as e:
            print(f"Erro fatal no arquivo {nome_arquivo}: {e}")

        return resultados


# ==============================================================================
# 5. EXECUÇÃO
# ==============================================================================

def executar_etl():
    GerenciadorConfiguracao.garantir_diretorios()
    processador = Processador()
    todos_dados = []

    arquivos = [f for f in os.listdir(GerenciadorConfiguracao.DIRETORIOS["ENTRADA"]) if f.endswith(('.xls', '.xlsx'))]

    if not arquivos:
        print("Nenhum arquivo na pasta Entrada.")
        return

    for f in arquivos:
        caminho_f = os.path.join(GerenciadorConfiguracao.DIRETORIOS["ENTRADA"], f)
        dados = processador.processar_arquivo(caminho_f)
        todos_dados.extend(dados)

        destino = os.path.join(GerenciadorConfiguracao.DIRETORIOS["PROCESSADOS"], f"PROC_{datetime.now().strftime('%d%H%M')}_{f}")
        shutil.move(caminho_f, destino)

    if not todos_dados:
        print("Nenhum dado extraído.")
        return

    print("\nConsolidando dados...")
    df_agrupado = pd.DataFrame(todos_dados)

    cols_meta = ["Cod_Unidade", "Arquivo", "Aba", "Setor", "Item", "Detalhe"]
    cols_num = [c for c in df_agrupado.columns if c not in cols_meta]

    df_agrupado = df_agrupado.groupby(cols_meta)[cols_num].sum().reset_index()

    saida = GerenciadorConfiguracao.ARQUIVOS["EXCEL_FINAL"]
    id_execucao = f"Rodada_{random.randint(10000, 99999)}"

    try:
        if os.path.exists(saida):
            with pd.ExcelWriter(saida, mode='a', engine='openpyxl', if_sheet_exists='overlay') as escritor:
                df_agrupado.to_excel(escritor, sheet_name=id_execucao, index=False)
        else:
            df_agrupado.to_excel(saida, sheet_name=id_execucao, index=False)

        print(f"✅ SUCESSO! Arquivo salvo em: {saida}")
        print(f"   Aba criada: {id_execucao}")
    except Exception as e:
        print(f"Erro ao salvar Excel: {e}")
        df_agrupado.to_csv(saida.replace('.xlsx', f'_{id_execucao}.csv'), sep=';', encoding='utf-8-sig')


if __name__ == "__main__":
    executar_etl()

