"""
Módulo de KPIs e Agregações - EcoData Monitor (versão corrigida)
Contém funções puras e protegidas contra dados sujos/ausentes.
Autor: Revisão automatizada — adaptado ao projeto PROJETOSENAI
"""

from typing import Tuple, Optional
import pandas as pd
import numpy as np
import os

# ---------------------------
# Utilitários internos
# ---------------------------
def _to_datetime_safe(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Converte coluna para datetime de forma segura (in-place)."""
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def _to_numeric_safe(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Converte coluna para numérico (in-place), coercing errors to NaN."""
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# ---------------------------
# 1. Período inteligente
# ---------------------------
def selecionar_periodo_inteligente(producao_df: pd.DataFrame, modo: str = "auto") -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp], str]:
    """
    Seleciona período de análise com heurística para evitar mostrar dias sem produção.
    Retorna (inicio, fim, descricao)
    """
    if producao_df is None or producao_df.empty or "timestamp" not in producao_df.columns:
        return None, None, "Sem Dados"

    df = producao_df.copy()
    _to_datetime_safe(df, "timestamp")

    agora = pd.Timestamp.now()

    modo = (modo or "auto").lower()
    if modo == "24h":
        fim = df["timestamp"].max()
        inicio = fim - pd.Timedelta(hours=24)
        descricao = "Últimas 24 Horas"
        return inicio, fim, descricao

    if modo == "ontem":
        ontem = (agora - pd.Timedelta(days=1)).date()
        inicio = pd.Timestamp(ontem)
        fim = inicio + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        descricao = f"Ontem ({ontem.strftime('%d/%m')})"
        return inicio, fim, descricao

    # Modo auto
    prod_valida = df[df.get("pecas_produzidas", 0) > 0]
    if prod_valida.empty:
        # mostra todo o histórico disponível
        inicio = df["timestamp"].min()
        fim = df["timestamp"].max()
        return inicio, fim, "Histórico Total"

    ultimo_timestamp = prod_valida["timestamp"].max()
    ultimo_dia = ultimo_timestamp.date()

    prod_ultimo_dia = prod_valida[prod_valida["timestamp"].dt.date == ultimo_dia]
    total_ultimo = int(prod_ultimo_dia["pecas_produzidas"].sum()) if not prod_ultimo_dia.empty else 0

    # heurística simples: se muito pouco produzido no último dia ou domingo, retrocede
    if total_ultimo < 500 or ultimo_dia.weekday() == 6:
        # procura dia anterior com produção (até 7 dias)
        dia_ref = ultimo_dia - pd.Timedelta(days=1)
        tentativas = 0
        found = False
        while tentativas < 7:
            prod_ref = df[df["timestamp"].dt.date == dia_ref]
            if not prod_ref.empty and prod_ref["pecas_produzidas"].sum() > 0:
                found = True
                break
            dia_ref -= pd.Timedelta(days=1)
            tentativas += 1
        if found:
            inicio = pd.Timestamp(dia_ref)
            fim = inicio + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            descricao = f"Fechamento Anterior ({dia_ref.strftime('%d/%m')})"
            return inicio, fim, descricao
        # fallback para histórico
        inicio = df["timestamp"].min()
        fim = df["timestamp"].max()
        return inicio, fim, "Histórico (fallback)"

    # caso normal: usar o próprio último dia disponível
    inicio = pd.Timestamp(ultimo_dia)
    fim = inicio + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    descricao = f"Hoje ({ultimo_dia.strftime('%d/%m')})"
    return inicio, fim, descricao

# ---------------------------
# 2. KPIs principais
# ---------------------------
def compute_kpis(producao_df: pd.DataFrame, telemetria_df: pd.DataFrame, modo_periodo: str = "auto") -> Tuple[int, int, int, float, str]:
    """
    Calcula KPIs: (pecas, refugo, defeitos, temp_media, descricao_periodo)
    """
    if producao_df is None or producao_df.empty:
        return 0, 0, 0, np.nan, "N/D"

    inicio, fim, descricao = selecionar_periodo_inteligente(producao_df, modo_periodo)
    if inicio is None:
        return 0, 0, 0, np.nan, descricao

    mask_prod = (producao_df["timestamp"] >= inicio) & (producao_df["timestamp"] <= fim)
    prod_periodo = producao_df.loc[mask_prod]

    pecas = int(prod_periodo.get("pecas_produzidas", pd.Series(dtype=float)).sum()) if not prod_periodo.empty else 0
    refugo = int(prod_periodo.get("pecas_refugadas", pd.Series(dtype=float)).sum()) if not prod_periodo.empty else 0

    defeitos = 0
    temp_media = np.nan
    if telemetria_df is not None and not telemetria_df.empty:
        tele = telemetria_df.copy()
        _to_datetime_safe(tele, "timestamp")
        mask_tel = (tele["timestamp"] >= inicio) & (tele["timestamp"] <= fim)
        tele_periodo = tele.loc[mask_tel]
        if not tele_periodo.empty:
            if "flag_defeito" in tele_periodo.columns:
                # garantir numeric
                tele_periodo["flag_defeito"] = pd.to_numeric(tele_periodo["flag_defeito"], errors="coerce").fillna(0).astype(int)
                defeitos = int(tele_periodo["flag_defeito"].sum())
            if "temp_matriz_c" in tele_periodo.columns:
                tele_periodo["temp_matriz_c"] = pd.to_numeric(tele_periodo["temp_matriz_c"], errors="coerce")
                temp_media = float(tele_periodo["temp_matriz_c"].mean()) if not tele_periodo["temp_matriz_c"].dropna().empty else np.nan

    return pecas, refugo, defeitos, temp_media, descricao

# ---------------------------
# 3. OEE
# ---------------------------
def compute_oee_kpis(producao_df: pd.DataFrame, telemetria_df: pd.DataFrame, capacidade_nominal_hora: int = 1000, modo_periodo: str = "auto") -> Tuple[float, float, float, float]:
    """
    Calcula Disponibilidade, Performance, Qualidade e OEE.
    """
    if producao_df is None or producao_df.empty:
        return 0.0, 0.0, 0.0, 0.0

    inicio, fim, _ = selecionar_periodo_inteligente(producao_df, modo_periodo)
    if inicio is None:
        return 0.0, 0.0, 0.0, 0.0

    prod_periodo = producao_df[(producao_df["timestamp"] >= inicio) & (producao_df["timestamp"] <= fim)]
    if prod_periodo.empty:
        return 0.0, 0.0, 0.0, 0.0

    total_produzido = prod_periodo["pecas_produzidas"].sum()
    total_refugado = prod_periodo["pecas_refugadas"].sum()
    pecas_boas = total_produzido - total_refugado
    qualidade = pecas_boas / total_produzido if total_produzido > 0 else 0.0

    tempo_programado_horas = prod_periodo.shape[0]  # assumido unidade por registro
    tempo_operando_horas = (prod_periodo["pecas_produzidas"] > 0).sum()
    disponibilidade = tempo_operando_horas / tempo_programado_horas if tempo_programado_horas > 0 else 0.0

    capacidade_teorica_total = tempo_programado_horas * capacidade_nominal_hora
    performance = total_produzido / capacidade_teorica_total if capacidade_teorica_total > 0 else 0.0

    oee = disponibilidade * performance * qualidade
    return disponibilidade, performance, qualidade, oee

# ---------------------------
# 4. Energia / Financeiro
# ---------------------------
def compute_energy_cost(prod_df: pd.DataFrame, preco_venda: float, custo_por_tijolo: float) -> Tuple[float, float]:
    """
    Retorna (custo_por_peça_estimado, custo_total_energia)
    """
    if prod_df is None or prod_df.empty or "consumo_kwh" not in prod_df.columns:
        return 0.0, 0.0

    CUSTO_KWH = 0.75
    total_kwh = prod_df["consumo_kwh"].sum()
    custo_total_energia = total_kwh * CUSTO_KWH
    total_pecas = prod_df["pecas_produzidas"].sum()
    custo_por_peca = custo_total_energia / total_pecas if total_pecas > 0 else 0.0
    return custo_por_peca, custo_total_energia

# ---------------------------
# 5. Agregações de telemetria
# ---------------------------
def aggregate_by_period(df: pd.DataFrame, freq: str = "5T") -> pd.DataFrame:
    """
    Agrega telemetria por período.
    Output: DataFrame com colunas [isa95_equipamento, period, <metrics>]
    """
    if df is None or df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()

    d = df.copy()
    _to_datetime_safe(d, "timestamp")
    # fallback for equipment id
    if "isa95_equipamento" not in d.columns and "maquina_id" in d.columns:
        d = d.rename(columns={"maquina_id": "isa95_equipamento"})

    # floor period
    d["period"] = d["timestamp"].dt.floor(freq)

    # garantir colunas numéricas (coerce)
    for col in ["pressao_mpa", "umidade_pct", "temp_matriz_c", "pecas_produzidas", "flag_defeito"]:
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")

    # construir regras dinamicamente
    agg_rules = {}
    if "pressao_mpa" in d.columns:
        agg_rules["pressao_mpa"] = "mean"
    if "umidade_pct" in d.columns:
        agg_rules["umidade_pct"] = "mean"
    if "temp_matriz_c" in d.columns:
        agg_rules["temp_matriz_c"] = "mean"
    if "pecas_produzidas" in d.columns:
        agg_rules["pecas_produzidas"] = "sum"
    if "flag_defeito" in d.columns:
        agg_rules["flag_defeito"] = "sum"

    group_key = "isa95_equipamento" if "isa95_equipamento" in d.columns else ("maquina_id" if "maquina_id" in d.columns else None)
    if group_key is None:
        # não há chave de máquina, agrega apenas por period
        agg = d.groupby("period").agg(agg_rules).reset_index()
        return agg

    # executar groupby de forma robusta
    agg = d.groupby([group_key, "period"]).agg(agg_rules).reset_index()
    # garantir nomes padronizados
    agg = agg.rename(columns={group_key: "isa95_equipamento"}).reset_index(drop=True)
    return agg

# ---------------------------
# 6. Alertas simples
# ---------------------------
def check_alerts(tele_agg: pd.DataFrame) -> list:
    """
    Retorna lista de alertas curtos (strings) baseados em regras simples.
    Ex.: pressão média por equipamento < 12 MPa
    """
    alerts = []
    if tele_agg is None or tele_agg.empty:
        return alerts

    # normalizar nomes
    df = tele_agg.copy()
    if "isa95_equipamento" not in df.columns and "maquina_id" in df.columns:
        df = df.rename(columns={"maquina_id": "isa95_equipamento"})

    if "pressao_mpa" not in df.columns:
        return alerts

    df["pressao_mpa"] = pd.to_numeric(df["pressao_mpa"], errors="coerce")
    crit = df[df["pressao_mpa"] < 12]
    for _, r in crit.tail(10).iterrows():
        equipamento = r.get("isa95_equipamento", r.get("maquina_id", "Desconhecido"))
        period = r.get("period")
        try:
            hora = pd.to_datetime(period).strftime("%Y-%m-%d %H:%M")
        except Exception:
            hora = str(period)
        alerts.append(f"Equipamento {equipamento}: Pressão baixa ({r['pressao_mpa']:.1f} MPa) em {hora}")
    return alerts

# ---------------------------
# 7. Refugo por turno
# ---------------------------
def compute_refugo_by_turno(prod_df: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna DataFrame com colunas [turno, pct_refugo] ordenado por pct_refugo desc.
    """
    if prod_df is None or prod_df.empty or "turno" not in prod_df.columns:
        return pd.DataFrame(columns=["turno", "pct_refugo"])

    df = prod_df.copy()
    # garantir numericidade
    df["pecas_produzidas"] = pd.to_numeric(df.get("pecas_produzidas", pd.Series([])), errors="coerce").fillna(0)
    df["pecas_refugadas"] = pd.to_numeric(df.get("pecas_refugadas", pd.Series([])), errors="coerce").fillna(0)

    grp = df.groupby("turno")[["pecas_produzidas", "pecas_refugadas"]].sum().reset_index()
    grp["pct_refugo"] = (grp["pecas_refugadas"] / grp["pecas_produzidas"].replace(0, np.nan) * 100).fillna(0).round(1)
    return grp.sort_values("pct_refugo", ascending=False).reset_index(drop=True)

# ---------------------------
# 8. MTTR / MTBF
# ---------------------------
def calculate_mttr_mtbf(evt_df: pd.DataFrame) -> Tuple[float, float]:
    """
    Calcula MTTR e MTBF em minutos. Requer coluna 'duracao_min' no evt_df.
    """
    if evt_df is None or evt_df.empty:
        return 0.0, 0.0

    df = evt_df.copy()
    _to_datetime_safe(df, "timestamp")
    # garantir duracao_min numeric
    if "duracao_min" not in df.columns:
        df["duracao_min"] = 0.0
    df["duracao_min"] = pd.to_numeric(df["duracao_min"], errors="coerce").fillna(0.0)

    falhas = df[df["duracao_min"] > 0]
    if falhas.empty:
        return 0.0, 0.0

    total_down_time = falhas["duracao_min"].sum()
    num_falhas = len(falhas)
    mttr = total_down_time / num_falhas if num_falhas > 0 else 0.0

    # total period time in minutes
    min_ts = df["timestamp"].min()
    max_ts = df["timestamp"].max()
    if pd.isna(min_ts) or pd.isna(max_ts) or min_ts == max_ts:
        return mttr, 0.0

    tempo_total_min = (max_ts - min_ts).total_seconds() / 60.0
    tempo_operacao = max(0.0, tempo_total_min - total_down_time)
    mtbf = tempo_operacao / num_falhas if num_falhas > 0 else 0.0

    return mttr, mtbf

# ---------------------------
# 9. Pareto de paradas
# ---------------------------
def pareto_paradas(df: pd.DataFrame):
    """Gera o Pareto de paradas baseado no Silver."""

    # Ajustar nome da coluna dependendo da origem
    if "evento" in df.columns:
        col_evento = "evento"
    elif "descricao" in df.columns:
        col_evento = "descricao"
    else:
        raise KeyError("Nenhuma coluna de evento encontrada (esperado: evento ou descricao)")

    # Verifica duração — se não existir, cria zerado
    if "duracao_min" not in df.columns:
        df["duracao_min"] = 0

    # Agrupamento corrigido
    grouped = (
        df.groupby(col_evento)
        .agg(
            count=(col_evento, "size"),
            tempo_total_min=("duracao_min", "sum")
        )
        .reset_index()
    )

    # Ordenar pelo maior impacto
    grouped = grouped.sort_values(by="count", ascending=False)

    return grouped

# ---------------------------
# 10. Build Scatter (pressão x umidade)
# ---------------------------
def build_pressure_humidity_scatter(tele_df: pd.DataFrame, sample: int = 2000) -> pd.DataFrame:
    """Prepara DataFrame para scatter visual (adiciona status)."""
    if tele_df is None or tele_df.empty:
        return pd.DataFrame()

    df = tele_df.copy()
    # garantir tipos
    _to_numeric_safe(df, "pressao_mpa")
    _to_numeric_safe(df, "umidade_pct")
    df["flag_defeito"] = pd.to_numeric(df.get("flag_defeito", 0), errors="coerce").fillna(0).astype(int)

    # amostragem por performance
    if df.shape[0] > sample:
        df = df.tail(sample).copy()

    df["status"] = df["flag_defeito"].apply(lambda x: "Defeito" if int(x) == 1 else "OK")
    return df

# ---------------------------
# 11. Aggregate events (recentes/críticos)
# ---------------------------
def aggregate_events(evt_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Retorna eventos críticos recentes ordenados por timestamp decrescente."""
    if evt_df is None or evt_df.empty:
        return pd.DataFrame()

    df = evt_df.copy()
    _to_datetime_safe(df, "timestamp")

    # se existir coluna de código, filtrar >=2
    if "sev_codigo" in df.columns:
        df = df[df["sev_codigo"] >= 2]
    elif "severidade" in df.columns:
        sev_low = df["severidade"].astype(str).str.lower()
        df = df[sev_low.isin(["média", "media", "alta", "high"])]

    df = df.sort_values("timestamp", ascending=False)
    return df.head(top_n).reset_index(drop=True)

# ---------------------------
# 12. Load gold kpis
# ---------------------------
def load_gold_kpis() -> pd.DataFrame:
    """Carrega KPIs diários Gold se existir (caminho relativo ao package root)."""
    # caminho relativo: projeto_root/data/gold/kpis_daily_gold.csv
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    gold_path = os.path.join(base, "data", "gold", "kpis_daily_gold.csv")
    try:
        df = pd.read_csv(gold_path)
        if "data" in df.columns:
            df["data"] = pd.to_datetime(df["data"], errors="coerce")
        return df
    except FileNotFoundError:
        # aviso leve — não interrompe pipeline/app
        print(f"AVISO: Gold KPIs não encontrado em {gold_path}. Execute pipeline_etl se necessário.")
        return pd.DataFrame()

# ---------------------------
# 13. Latest status
# ---------------------------
def latest_status(tele_agg: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna última leitura agregada por equipamento (isa95_equipamento ou maquina_id).
    """
    if tele_agg is None or tele_agg.empty:
        return pd.DataFrame()

    df = tele_agg.copy()
    key = "isa95_equipamento" if "isa95_equipamento" in df.columns else ("maquina_id" if "maquina_id" in df.columns else None)
    if key is None:
        return pd.DataFrame()
    last = df.sort_values("period").groupby(key, as_index=False).tail(1)
    return last.set_index(key)

# ---------------------------
# 14. Contextualização ISA-95
# ---------------------------
def map_isa95(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona colunas isa95_planta, isa95_area, isa95_linha, e renomeia maquina_id -> isa95_equipamento.
    Não altera o id original (maquina_id).
    """
    if df is None or df.empty or "maquina_id" not in df.columns:
        return df

    d = df.copy()
    # salvar coluna original
    d["maquina_id"] = d["maquina_id"].astype(str)

    # renomeia
    d = d.rename(columns={"maquina_id": "isa95_equipamento"})

    # valores fixos (pode ser parametrizado futuramente)
    d["isa95_planta"] = "Tijolos_PE"
    d["isa95_area"] = "Extrusao"

    def _map_linha(e):
        # comportamento robusto: aceita formatos M1, 1, etc.
        s = str(e).upper()
        if "1" in s or "M1" in s:
            return "Linha_A"
        if "2" in s or "M2" in s:
            return "Linha_A"
        if "3" in s or "M3" in s:
            return "Linha_B"
        if "4" in s or "M4" in s:
            return "Linha_B"
        return "Linha_Outras"

    d["isa95_linha"] = d["isa95_equipamento"].apply(_map_linha)
    return d