import pandas as pd
from app.config import settings
from typing import Optional

def aggregate_by_period(tele_df: pd.DataFrame, freq: str = "1T") -> pd.DataFrame:
    """
    Agrega telemetria por periodo.
    Output: columns [maquina_id, period, pecas_produzidas, flag_defeito, pressao_mpa, temp_matriz_c]
    """
    if tele_df is None or tele_df.empty:
        return pd.DataFrame()
    t = tele_df.copy()
    if "timestamp" not in t.columns:
        raise ValueError("tele_df precisa conter coluna 'timestamp'")
    t["period"] = t["timestamp"].dt.floor(freq)
    agg = (
        t.groupby(["maquina_id", "period"], dropna=False)
         .agg(
             pecas_produzidas=("pecas_produzidas", "sum"),
             flag_defeito=("flag_defeito", "sum"),
             pressao_mpa=("pressao_mpa", "mean"),
             temp_matriz_c=("temp_matriz_c", "mean"),
         )
         .reset_index()
    )
    return agg

def latest_status(tele_agg: pd.DataFrame) -> pd.DataFrame:
    """Retorna última linha por máquina (último período)"""
    if tele_agg is None or tele_agg.empty:
        return pd.DataFrame()
    last = tele_agg.sort_values("period").groupby("maquina_id", as_index=False).tail(1)
    return last.set_index("maquina_id")

def compute_daily_kpis(prod_df: pd.DataFrame, tele_df: pd.DataFrame, date: Optional[pd.Timestamp]=None) -> dict:
    """
    Calcula KPIs do dia (ou da date passada). Retorna dict com peças, refugos, defeitos, temp_media.
    """
    if date is None:
        date = pd.Timestamp.now().normalize()
    kpis = {"total_pecas": 0, "total_refugo": 0, "total_defeitos": 0, "avg_temp": float("nan")}
    if prod_df is not None and "timestamp" in prod_df.columns:
        prod_today = prod_df[prod_df["timestamp"] >= date]
        if not prod_today.empty and "pecas_produzidas" in prod_today.columns:
            kpis["total_pecas"] = int(prod_today["pecas_produzidas"].sum())
        if not prod_today.empty and "pecas_refugadas" in prod_today.columns:
            kpis["total_refugo"] = int(prod_today["pecas_refugadas"].sum())
    if tele_df is not None and "timestamp" in tele_df.columns and "temp_matriz_c" in tele_df.columns:
        tele_today = tele_df[tele_df["timestamp"] >= date]
        if not tele_today.empty:
            kpis["total_defeitos"] = int(tele_today["flag_defeito"].sum()) if "flag_defeito" in tele_today.columns else 0
            kpis["avg_temp"] = float(tele_today["temp_matriz_c"].mean())
    return kpis

def compute_defect_rates(tele_agg: pd.DataFrame) -> pd.DataFrame:
    """Retorna taxa de defeito por máquina (fração)."""
    if tele_agg is None or tele_agg.empty:
        return pd.DataFrame(columns=["maquina_id","taxa_defeito"])
    sums = tele_agg.groupby("maquina_id").agg(total_defeitos=("flag_defeito","sum"), total_pecas=("pecas_produzidas","sum")).reset_index()
    sums["taxa_defeito"] = sums.apply(lambda r: (r["total_defeitos"]/r["total_pecas"]) if r["total_pecas"]>0 else 0.0, axis=1)
    return sums[["maquina_id","taxa_defeito"]]