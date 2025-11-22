from pathlib import Path

# caminho base do repositório (assume que você roda streamlit a partir da raiz do repo)
ROOT = Path(__file__).resolve().parents[2]

# diretórios de dados
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# arquivos processados (silver)
TELEMETRY_SILVER = PROCESSED_DIR / "telemetria_silver.csv"
PRODUCTION_SILVER = PROCESSED_DIR / "producao_silver.csv"
EVENTS_SILVER = PROCESSED_DIR / "eventos_silver.csv"