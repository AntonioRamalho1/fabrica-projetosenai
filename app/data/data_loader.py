import pandas as pd
from typing import Tuple
from app.config import paths
from app.config import settings
import logging

logger = logging.getLogger(__name__)

REQUIRED_FILES = {
    "telemetry": paths.TELEMETRY_SILVER,
    "production": paths.PRODUCTION_SILVER,
    "events": paths.EVENTS_SILVER,
}

def _standardize_cols(df: pd.DataFrame) -> pd.DataFrame:
    # lower, trim and replace spaces/special chars with underscore
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(r"\s+", "_", regex=True)
                  .str.normalize("NFKD")
                  .str.encode("ascii", errors="ignore")
                  .str.decode("ascii")
                  .str.replace(r"[^\w_]", "", regex=True)
    )
    return df

def _safe_read_csv(path) -> pd.DataFrame:
    # tenta leituras comuns e retorna df (ou raise FileNotFoundError)
    path = paths.Path(path) if not isinstance(path, (str,)) else paths.Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    # tentar ler com engine padrão
    try:
        df = pd.read_csv(path)
    except Exception as e:
        # tentar latin-1 como fallback
        try:
            df = pd.read_csv(path, encoding="latin1")
        except Exception as e2:
            raise RuntimeError(f"Falha ao ler {path}: {e} / {e2}")
    return df

def _ensure_timestamp(df: pd.DataFrame, col_name="timestamp") -> pd.DataFrame:
    if col_name in df.columns:
        df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
    return df

def load_silver_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Lê e padroniza os CSVs da camada processed (silver).
    Retorna: telemetry_df, production_df, events_df
    """
    # leitura
    telemetry = _safe_read_csv(REQUIRED_FILES["telemetry"])
    production = _safe_read_csv(REQUIRED_FILES["production"])
    events = _safe_read_csv(REQUIRED_FILES["events"])

    # padroniza colunas
    telemetry = _standardize_cols(telemetry)
    production = _standardize_cols(production)
    events = _standardize_cols(events)

    # garantir timestamp como datetime
    telemetry = _ensure_timestamp(telemetry, "timestamp")
    production = _ensure_timestamp(production, "timestamp")
    events = _ensure_timestamp(events, "timestamp")

    # logging básico de sanity-check
    logger.info("telemetry: rows=%s cols=%s", telemetry.shape[0], telemetry.shape[1])
    logger.info("production: rows=%s cols=%s", production.shape[0], production.shape[1])
    logger.info("events: rows=%s cols=%s", events.shape[0], events.shape[1])

    return telemetry, production, events