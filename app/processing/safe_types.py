import re
import pandas as pd

def parse_number_like(x):
    """Remove caracteres não-numéricos úteis e converte para float se possível."""
    if pd.isna(x):
        return None
    s = str(x).strip()
    # substituir vírgula decimal por ponto
    s = s.replace(",", ".")
    # extrair padrão numérico (com optional sinal e ponto)
    m = re.search(r"[-+]?\d*\.?\d+", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None