import pandas as pd
import os
from datetime import datetime
from typing import Tuple

# --- Configurações de Caminho ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "app", "data")

RAW_DIR = os.path.join(DATA_DIR, "raw")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")

# --- Funções de Processamento (Bronze -> Silver) ---

def process_telemetria(tele_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Processa os dados brutos de telemetria (Bronze) para a camada Silver.
    - Renomeia colunas para padronização.
    - Converte tipos de dados.
    """
    print("Processando Telemetria (Bronze -> Silver)...")
    tele = tele_raw.copy()
    
    # Padronização de nomes
    tele.columns = [col.lower().replace(" ", "_").replace("(", "").replace(")", "").replace(".", "_") for col in tele.columns]
    
    # Renomear colunas chave
    tele = tele.rename(columns={
        "timestamp": "timestamp",
        "maquina": "maquina_id",
        "pressao_mpa": "pressao_mpa",
        "umidade": "umidade_pct",
        "temperatura_matriz_c": "temp_matriz_c",
        "defeito": "flag_defeito"
    })
    
    # Conversão de tipos
    tele["timestamp"] = pd.to_datetime(tele["timestamp"], errors="coerce")
    tele = tele.dropna(subset=["timestamp"])
    
    # Filtro de dados inválidos (ex: pressao negativa)
    tele = tele[tele["pressao_mpa"] >= 0]
    
    return tele

def process_producao(prod_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Processa os dados brutos de produção (Bronze) para a camada Silver.
    - Renomeia colunas para padronização.
    - Converte tipos de dados.
    - Cria coluna de turno (simulada).
    """
    print("Processando Produção (Bronze -> Silver)...")
    prod = prod_raw.copy()
    
    # Padronização de nomes
    prod.columns = [col.lower().replace(" ", "_").replace("(", "").replace(")", "").replace(".", "_") for col in prod.columns]
    
    # Renomear colunas chave
    prod = prod.rename(columns={
        "timestamp": "timestamp",
        "maquina": "maquina_id",
        "pecas_produzidas": "pecas_produzidas",
        "pecas_refugadas": "pecas_refugadas",
        "consumo_kwh": "consumo_kwh"
    })
    
    # Conversão de tipos
    prod["timestamp"] = pd.to_datetime(prod["timestamp"], errors="coerce")
    prod = prod.dropna(subset=["timestamp"])
    
    # Criação de Turno (Simulação - 3 turnos de 8h)
    def get_turno(hour):
        if 6 <= hour < 14:
            return "Turno A (06h-14h)"
        elif 14 <= hour < 22:
            return "Turno B (14h-22h)"
        else:
            return "Turno C (22h-06h)"
            
    prod["turno"] = prod["timestamp"].dt.hour.apply(get_turno)
    
    return prod

def process_eventos(evt_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Processa os dados brutos de eventos (Bronze) para a camada Silver.
    - Renomeia colunas para padronização.
    - Converte tipos de dados.
    """
    print("Processando Eventos (Bronze -> Silver)...")
    evt = evt_raw.copy()
    
    # Padronização de nomes
    evt.columns = [col.lower().replace(" ", "_").replace("(", "").replace(")", "").replace(".", "_") for col in evt.columns]
    
    # Renomear colunas chave
    evt = evt = evt.rename(columns={
        "timestamp": "timestamp",
        "maquina": "maquina_id",
        "evento": "evento",
        "severidade": "severidade",
        "sev_codigo": "sev_codigo"
    })
    
    # Conversão de tipos
    evt["timestamp"] = pd.to_datetime(evt["timestamp"], errors="coerce")
    evt = evt.dropna(subset=["timestamp"])
    
    # Garantir que sev_codigo é numérico para filtros
    evt["sev_codigo"] = pd.to_numeric(evt["sev_codigo"], errors="coerce").fillna(0).astype(int)
    
    return evt

# --- Funções de Processamento (Silver -> Gold) ---

def create_gold_kpis(prod_silver: pd.DataFrame) -> pd.DataFrame:
    """
    Cria a tabela Gold de KPIs diários agregados.
    - Calcula OEE, Produção Total e Refugo por dia.
    """
    print("Criando KPIs Diários (Silver -> Gold)...")
    
    if prod_silver.empty:
        return pd.DataFrame()
        
    # Agregação diária
    prod_silver["data"] = prod_silver["timestamp"].dt.date
    
    daily_agg = prod_silver.groupby("data").agg(
        pecas_produzidas=("pecas_produzidas", "sum"),
        pecas_refugadas=("pecas_refugadas", "sum"),
        horas_operando=("pecas_produzidas", lambda x: (x > 0).sum()) # Contagem de horas com produção
    ).reset_index()
    
    # Cálculo de KPIs simplificados para a camada Gold
    daily_agg["pecas_boas"] = daily_agg["pecas_produzidas"] - daily_agg["pecas_refugadas"]
    daily_agg["qualidade"] = daily_agg["pecas_boas"] / daily_agg["pecas_produzidas"].replace(0, 1)
    
    # Assumindo 24 horas programadas por dia (para simplificar)
    daily_agg["disponibilidade"] = daily_agg["horas_operando"] / 24
    
    # Performance (assumindo capacidade nominal de 1000 peças/hora)
    daily_agg["performance"] = daily_agg["pecas_produzidas"] / (24 * 1000)
    
    daily_agg["oee"] = daily_agg["disponibilidade"] * daily_agg["performance"] * daily_agg["qualidade"]
    
    return daily_agg.round(4)

# --- Função Principal de Execução ---

def run_etl_pipeline():
    """Executa o pipeline completo Bronze -> Silver -> Gold."""
    print(f"--- Iniciando Pipeline ETL ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---")
    
    # 1. BRONZE (Leitura dos arquivos brutos)
    try:
        tele_raw = pd.read_csv(os.path.join(RAW_DIR, "telemetria_raw.csv"))
        prod_raw = pd.read_csv(os.path.join(RAW_DIR, "producao_raw.csv"))
        evt_raw = pd.read_csv(os.path.join(RAW_DIR, "eventos_raw.csv"))
    except FileNotFoundError as e:
        print(f"ERRO: Arquivo Bronze não encontrado. Certifique-se de que os arquivos raw estão na pasta {RAW_DIR}. Erro: {e}")
        return
    except Exception as e:
        print(f"ERRO ao ler arquivos Bronze: {e}")
        return
        
    # 2. SILVER (Processamento e Limpeza)
    tele_silver = process_telemetria(tele_raw)
    prod_silver = process_producao(prod_raw)
    evt_silver = process_eventos(evt_raw)
    
    # 3. GOLD (Agregação e KPIs de Negócio)
    kpis_gold = create_gold_kpis(prod_silver)
    
    # 4. SALVAR ARQUIVOS (Silver e Gold)
    print("Salvando arquivos Silver...")
    tele_silver.to_csv(os.path.join(SILVER_DIR, "telemetria_silver.csv"), index=False)
    prod_silver.to_csv(os.path.join(SILVER_DIR, "producao_silver.csv"), index=False)
    evt_silver.to_csv(os.path.join(SILVER_DIR, "eventos_silver.csv"), index=False)
    
    print("Salvando arquivos Gold...")
    kpis_gold.to_csv(os.path.join(GOLD_DIR, "kpis_daily_gold.csv"), index=False)
    
    print("--- Pipeline ETL concluído com sucesso! ---")

if __name__ == "__main__":
    # Garante que as pastas Silver e Gold existem
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(GOLD_DIR, exist_ok=True)
    
    # Simulação de criação dos arquivos raw (se não existirem)
    # Isso é necessário porque o repositório não tem os arquivos raw, apenas os processed
    # Vamos renomear os arquivos processed para raw para simular a origem
    
    if not os.path.exists(os.path.join(RAW_DIR, "telemetria_raw.csv")):
        print("AVISO: Arquivos raw não encontrados. Tentando usar os arquivos silver como raw para simulação.")
        try:
            # Renomeia os arquivos silver (antigos processed) para raw
            os.rename(os.path.join(SILVER_DIR, "telemetria_silver.csv"), os.path.join(RAW_DIR, "telemetria_raw.csv"))
            os.rename(os.path.join(SILVER_DIR, "producao_silver.csv"), os.path.join(RAW_DIR, "producao_raw.csv"))
            os.rename(os.path.join(SILVER_DIR, "eventos_silver.csv"), os.path.join(RAW_DIR, "eventos_raw.csv"))
            print("Arquivos renomeados com sucesso. Executando pipeline.")
        except FileNotFoundError:
            print("ERRO: Arquivos silver (antigos processed) também não encontrados. Não é possível executar o pipeline.")
            exit()
            
    run_etl_pipeline()