import pandas as pd

def safe_to_plotly(df: pd.DataFrame) -> pd.DataFrame:
    """Converte dtypes problemáticos para string para Plotly/Streamlit."""
    if df is None or df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_categorical_dtype(out[c]) or pd.api.types.is_interval_dtype(out[c]):
            out[c] = out[c].astype(str)
    return out