import pandas as pd
import numpy as np
from app.config import settings

def compute_alerts(tele_agg: pd.DataFrame, window_minutes: int = None, std_factor: float = None, persistence: int = None):
    """
    Retorna lista de alertas simples baseados em pressao_mpa por máquina.
    Regras:
      - calcula média móvel e std em janela (window_minutes)
      - um ponto fora de m ± k*std e persistente por N amostras gera alerta
    """
    if tele_agg is None or tele_agg.empty:
        return []

    if window_minutes is None:
        window_minutes = settings.ALERT_WINDOW_MINUTES
    if std_factor is None:
        std_factor = settings.ALERT_STD_FACTOR
    if persistence is None:
        persistence = settings.ALERT_PERSISTENCE

    alerts = []
    # trabalhar por máquina
    for mid, g in tele_agg.dropna(subset=["pressao_mpa"]).groupby("maquina_id"):
        df = g.sort_values("period").copy()
        # calcular rolling mean/std por número de linhas equivalente (aprox)
        # assumimos período regular; usar window por quantidade de pontos:
        window = max(1, int(window_minutes))  # se period em minutos, aqui tratamos como n pontos
        df["rolling_mean"] = df["pressao_mpa"].rolling(window=window, min_periods=1, center=False).mean()
        df["rolling_std"] = df["pressao_mpa"].rolling(window=window, min_periods=1, center=False).std(ddof=0).fillna(0.0)

        df["zscore"] = (df["pressao_mpa"] - df["rolling_mean"]) / df["rolling_std"].replace(0, np.nan)
        df["out_of_range"] = False
        # ponto é out_of_range quando abs(zscore) > std_factor OR absolute beyond safe bounds
        for i, row in df.iterrows():
            p = row["pressao_mpa"]
            mm = row["rolling_mean"]
            ss = row["rolling_std"]
            is_out = False
            if ss > 0:
                if abs(p - mm) > std_factor * ss:
                    is_out = True
            # checagem por limite absoluto
            if p > settings.PRESSURE_MAX_SAFE or p < settings.PRESSURE_MIN_SAFE:
                is_out = True
            df.at[i, "out_of_range"] = is_out

        # persistência: procurar sequências de True de comprimento >= persistence
        out_series = df["out_of_range"].astype(int)
        if out_series.sum() == 0:
            continue
        # compute runs
        runs = (out_series.groupby((out_series != out_series.shift()).cumsum()).agg(run_val=('out_of_range','first'), run_len=('out_of_range','sum')))
        # runs index are groups, but easier: scan sequentially
        consec = 0
        last_idx = None
        for idx, val in out_series.items():
            if val:
                consec += 1
                last_idx = idx
            else:
                if consec >= persistence:
                    # create alert from last consec block
                    alert_row = df.loc[last_idx]
                    alerts.append({
                        "maquina_id": mid,
                        "timestamp": alert_row["period"],
                        "metric": "pressao_mpa",
                        "value": float(alert_row["pressao_mpa"]),
                        "rolling_mean": float(alert_row["rolling_mean"]),
                        "rolling_std": float(alert_row["rolling_std"]),
                        "rule": f"{std_factor}*std + persistência {persistence}"
                    })
                consec = 0
                last_idx = None
        # check tail
        if consec >= persistence and last_idx is not None:
            alert_row = df.loc[last_idx]
            alerts.append({
                "maquina_id": mid,
                "timestamp": alert_row["period"],
                "metric": "pressao_mpa",
                "value": float(alert_row["pressao_mpa"]),
                "rolling_mean": float(alert_row["rolling_mean"]),
                "rolling_std": float(alert_row["rolling_std"]),
                "rule": f"{std_factor}*std + persistência {persistence}"
            })
    return alerts