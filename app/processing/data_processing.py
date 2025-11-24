import pandas as pd

def safe_to_plotly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte dtypes problemáticos (categorias, intervalos) para string 
    para que o Plotly/Streamlit consiga renderizar sem erros.
    """
    if df is None or df.empty:
        return df
        
    out = df.copy()
    for c in out.columns:
        # Verifica se é categoria ou intervalo e converte para texto
        if pd.api.types.is_categorical_dtype(out[c]) or pd.api.types.is_interval_dtype(out[c]):
            out[c] = out[c].astype(str)
            
    return out