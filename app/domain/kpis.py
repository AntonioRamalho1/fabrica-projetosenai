"""
Módulo de KPIs e Agregações - EcoData Monitor
Responsável por toda a lógica de negócios, cálculos e agregações de dados.
"""

import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Any, Optional

# ==============================================================================
# 1. LÓGICA INTELIGENTE DE PERÍODO (RESOLVE O PROBLEMA DE ZERO PRODUÇÃO)
# ==============================================================================

def selecionar_periodo_inteligente(producao_df: pd.DataFrame, modo: str = "auto") -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp], str]:
    """
    Seleciona o período de análise de forma dinâmica para evitar mostrar zeros
    quando a simulação para no início do dia.
    
    Args:
        producao_df: DataFrame de produção
        modo: "auto" (inteligente), "ontem" (fechado), "24h" (janela móvel)
        
    Returns:
        (data_inicio, data_fim, descricao_periodo)
    """
    if producao_df is None or producao_df.empty:
        return None, None, "Sem Dados"
    
    agora = pd.Timestamp.now()
    
    if modo == "24h":
        # Últimas 24 horas corridas
        fim = producao_df["timestamp"].max()
        inicio = fim - pd.Timedelta(hours=24)
        descricao = "Últimas 24 Horas"
        
    elif modo == "ontem":
        # Ontem completo (00:00 a 23:59)
        ontem = (agora - pd.Timedelta(days=1)).date()
        inicio = pd.Timestamp(ontem)
        fim = inicio + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        descricao = f"Ontem ({ontem.strftime('%d/%m')})"
        
    else:  # modo == "auto" (O Padrão Recomendado)
        # Verifica se temos produção válida
        prod_valida = producao_df[producao_df["pecas_produzidas"] > 0]
        
        if prod_valida.empty:
            # Se nunca produziu nada, retorna o range total
            return producao_df["timestamp"].min(), producao_df["timestamp"].max(), "Histórico Total"
        
        ultimo_dia_dados = prod_valida["timestamp"].max().date()
        hoje_real = agora.date()
        
        # Analisa o último dia disponível nos dados
        prod_ultimo = producao_df[producao_df["timestamp"].dt.date == ultimo_dia_dados]
        total_ultimo = prod_ultimo["pecas_produzidas"].sum()
        
        # LÓGICA DE DECISÃO:
        # Se o último dia tem muito pouca produção (< 500 peças) OU é Domingo (dia 6),
        # provavelmente é um início de turno ou dia parado. Melhor mostrar o dia anterior.
        # DEPOIS (com proteção):
        if total_ultimo < 500 or ultimo_dia_dados.weekday() == 6:
            # Retrocede para o dia anterior (Ontem)
            dia_ref = ultimo_dia_dados - pd.Timedelta(days=1)
            
            # PROTEÇÃO: Se ontem também não tem produção, volta até 7 dias
            tentativas = 0
            while tentativas < 7:
                prod_ref = producao_df[producao_df["timestamp"].dt.date == dia_ref]
                if prod_ref["pecas_produzidas"].sum() > 0:
                    break
                dia_ref -= pd.Timedelta(days=1)
                tentativas += 1
            
            inicio = pd.Timestamp(dia_ref)
            fim = inicio + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            descricao = f"Fechamento Anterior ({dia_ref.strftime('%d/%m')})"
        else:
            # O dia atual tem dados bons, mostra ele mesmo
            inicio = pd.Timestamp(ultimo_dia_dados)
            fim = inicio + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            descricao = f"Hoje ({ultimo_dia_dados.strftime('%d/%m')})"
    
    return inicio, fim, descricao


# ==============================================================================
# 2. CÁLCULO DE KPIs PRINCIPAIS
# ==============================================================================

def compute_kpis(producao_df: pd.DataFrame, telemetria_df: pd.DataFrame, modo_periodo: str = "auto") -> Tuple[int, int, int, float, str]:
    """
    Calcula KPIs (Peças, Refugo, Defeitos, Temp) filtrados pelo período inteligente.
    """
    if producao_df is None or producao_df.empty:
        return 0, 0, 0, np.nan, "N/D"
    
    # 1. Define o período
    inicio, fim, descricao = selecionar_periodo_inteligente(producao_df, modo_periodo)
    
    if inicio is None:
        return 0, 0, 0, np.nan, descricao
    
    # 2. Filtra Produção
    mask_prod = (producao_df["timestamp"] >= inicio) & (producao_df["timestamp"] <= fim)
    prod_periodo = producao_df.loc[mask_prod]
    
    pecas = int(prod_periodo["pecas_produzidas"].sum())
    refugo = int(prod_periodo["pecas_refugadas"].sum())
    
    # 3. Filtra Telemetria (Defeitos e Temperatura)
    defeitos = 0
    temp_media = np.nan
    
    if telemetria_df is not None and not telemetria_df.empty:
        mask_tele = (telemetria_df["timestamp"] >= inicio) & (telemetria_df["timestamp"] <= fim)
        tele_periodo = telemetria_df.loc[mask_tele]
        
        if not tele_periodo.empty:
            if "flag_defeito" in tele_periodo.columns:
                defeitos = int(tele_periodo["flag_defeito"].sum())
            if "temp_matriz_c" in tele_periodo.columns:
                temp_media = tele_periodo["temp_matriz_c"].mean()
    
    return pecas, refugo, defeitos, temp_media, descricao


def compute_oee_kpis(producao_df: pd.DataFrame, telemetria_df: pd.DataFrame, 
                     capacidade_nominal_hora: int = 1000, modo_periodo: str = "auto") -> Tuple[float, float, float, float]:
    """
    Calcula OEE (Disponibilidade, Performance, Qualidade) baseado no período inteligente.
    """
    if producao_df is None or producao_df.empty:
        return 0.0, 0.0, 0.0, 0.0
    
    inicio, fim, _ = selecionar_periodo_inteligente(producao_df, modo_periodo)
    
    if inicio is None:
        return 0.0, 0.0, 0.0, 0.0
    
    prod_periodo = producao_df[(producao_df["timestamp"] >= inicio) & (producao_df["timestamp"] <= fim)]
    
    if prod_periodo.empty:
        return 0.0, 0.0, 0.0, 0.0
    
    # --- CÁLCULOS OEE ---
    total_produzido = prod_periodo["pecas_produzidas"].sum()
    total_refugado = prod_periodo["pecas_refugadas"].sum()
    pecas_boas = total_produzido - total_refugado
    
    # 1. Qualidade
    qualidade = pecas_boas / total_produzido if total_produzido > 0 else 0.0
    
    # 2. Disponibilidade
    tempo_programado_horas = len(prod_periodo) # Assumindo dados horários
    tempo_operando_horas = (prod_periodo["pecas_produzidas"] > 0).sum()
    disponibilidade = tempo_operando_horas / tempo_programado_horas if tempo_programado_horas > 0 else 0.0
    
    # 3. Performance
    capacidade_teorica_total = tempo_programado_horas * capacidade_nominal_hora
    performance = total_produzido / capacidade_teorica_total if capacidade_teorica_total > 0 else 0.0
    
    # 4. OEE Global
    oee = disponibilidade * performance * qualidade
    
    return disponibilidade, performance, qualidade, oee


# ==============================================================================
# 3. OUTRAS FUNÇÕES DE DOMÍNIO (Financeiro, Agregações, Alertas)
# ==============================================================================

def compute_energy_cost(prod_df: pd.DataFrame, preco_venda: float, custo_por_tijolo: float) -> Tuple[float, float]:
    """Calcula KPIs financeiros de energia."""
    if prod_df is None or prod_df.empty or "consumo_kwh" not in prod_df.columns:
        return 0.0, 0.0

    CUSTO_KWH = 0.75  # R$/kWh estimado
    
    total_kwh = prod_df["consumo_kwh"].sum()
    custo_total_energia = total_kwh * CUSTO_KWH
    
    total_pecas = prod_df["pecas_produzidas"].sum()
    custo_por_peca = custo_total_energia / total_pecas if total_pecas > 0 else 0.0
    
    return custo_por_peca, custo_total_energia


def aggregate_by_period(df: pd.DataFrame, freq: str = "5T") -> pd.DataFrame:
    """Agrega telemetria para gráficos de linha do tempo."""
    if df is None or df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    
    d = df.copy()
    d["period"] = d["timestamp"].dt.floor(freq)
    
    # Agregação segura
    agg_rules = {
        "pecas_produzidas": "sum",
        "flag_defeito": "sum",
        "pressao_mpa": "mean",
        "temp_matriz_c": "mean"
    }
    # Remove regras para colunas que não existem
    agg_rules = {k: v for k, v in agg_rules.items() if k in d.columns}
    
    agg = d.groupby(["maquina_id", "period"]).agg(agg_rules).reset_index()
    return agg


def check_alerts(tele_agg: pd.DataFrame) -> List[str]:
    """Gera lista de alertas textuais."""
    alerts = []
    if tele_agg is None or tele_agg.empty:
        return alerts
    
    # Regra: Pressão < 12 MPa
    crit = tele_agg[tele_agg["pressao_mpa"] < 12]
    
    for _, r in crit.tail(5).iterrows():
        alerts.append(f"Máquina {r['maquina_id']}: Pressão Baixa ({r['pressao_mpa']:.1f} MPa) às {r['period'].strftime('%H:%M')}")
    return alerts


def compute_refugo_by_turno(prod_df: pd.DataFrame) -> pd.DataFrame:
    """KPI de qualidade por turno."""
    if prod_df is None or prod_df.empty or "turno" not in prod_df.columns:
        return pd.DataFrame()
    
    grp = prod_df.groupby("turno")[["pecas_produzidas", "pecas_refugadas"]].sum().reset_index()
    grp["pct_refugo"] = (grp["pecas_refugadas"] / grp["pecas_produzidas"].replace(0, np.nan) * 100).fillna(0).round(1)
    
    return grp.sort_values("pct_refugo", ascending=False)


def pareto_paradas(evt_df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """Gera dados para gráfico de Pareto de paradas."""
    if evt_df is None or evt_df.empty:
        return pd.DataFrame(columns=["motivo", "count"])
    
    # Filtra eventos críticos (Média/Alta severidade)
    df_f = evt_df.copy()
    if "sev_codigo" in df_f.columns:
        df_f = df_f[df_f["sev_codigo"] >= 2] # 2=Média, 3=Alta
    elif "severidade" in df_f.columns:
        df_f = df_f[df_f["severidade"].str.lower().isin(["média", "alta", "media", "high"])]
        
    vc = df_f["evento"].value_counts().reset_index()
    vc.columns = ["motivo", "count"]
    return vc.head(top_n)


def build_pressure_humidity_scatter(tele_df: pd.DataFrame) -> pd.DataFrame:
    """Prepara dados para o gráfico de dispersão (Mapa Operacional)."""
    if tele_df is None or tele_df.empty:
        return pd.DataFrame()
    
    # Pega amostra recente para não pesar o gráfico
    df = tele_df.tail(2000).copy()
    
    # Cria coluna de status legível
    if "flag_defeito" in df.columns:
        df["status"] = df["flag_defeito"].apply(lambda x: "Defeito" if x == 1 else "OK")
    else:
        df["status"] = "OK"
        
    return df


def aggregate_events(evt_df: pd.DataFrame) -> pd.DataFrame:
    """Retorna lista de eventos críticos recentes."""
    if evt_df is None or evt_df.empty:
        return pd.DataFrame()
    
    # Filtra Alta Severidade
    criticos = evt_df[evt_df["sev_codigo"] == 3] if "sev_codigo" in evt_df.columns else evt_df
    return criticos.sort_values("timestamp", ascending=False).head(10)


def latest_status(tele_agg: pd.DataFrame) -> pd.DataFrame:
    """Retorna o status mais recente de cada máquina."""
    if tele_agg is None or tele_agg.empty:
        return pd.DataFrame()
    return tele_agg.sort_values("period").groupby("maquina_id").tail(1).set_index("maquina_id")