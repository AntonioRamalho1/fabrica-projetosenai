def human_number(x):
    """Formata número com separador de milhares no estilo BR (ponto)."""
    try:
        return f"{int(x):,}".replace(",", ".")
    except Exception:
        return str(x)

def to_percentage(x, ndigits=1):
    try:
        return f"{round(x*100, ndigits)}%"
    except Exception:
        return str(x)