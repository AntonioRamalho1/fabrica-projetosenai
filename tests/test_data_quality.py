import pandas as pd
import pytest
import os
from datetime import datetime

# --- Configuração de Caminhos ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_DIR = os.path.join(BASE_DIR, "app", "data", "silver")

# --- Fixtures para carregar dados ---

@pytest.fixture(scope="session")
def telemetria_silver():
    path = os.path.join(SILVER_DIR, "telemetria_silver.csv")
    if not os.path.exists(path):
        pytest.skip(f"Arquivo de telemetria não encontrado em: {path}")
    return pd.read_csv(path)

@pytest.fixture(scope="session")
def producao_silver():
    path = os.path.join(SILVER_DIR, "producao_silver.csv")
    if not os.path.exists(path):
        pytest.skip(f"Arquivo de produção não encontrado em: {path}")
    return pd.read_csv(path)

@pytest.fixture(scope="session")
def eventos_silver():
    path = os.path.join(SILVER_DIR, "eventos_silver.csv")
    if not os.path.exists(path):
        pytest.skip(f"Arquivo de eventos não encontrado em: {path}")
    return pd.read_csv(path)

# ==============================================================================
# TESTES DE QUALIDADE DE DADOS (DATAOPS)
# ==============================================================================

# --- Testes de Schema e Tipos ---

def test_telemetria_has_required_columns(telemetria_silver):
    """Verifica se as colunas essenciais da telemetria estão presentes."""
    required_cols = ["timestamp", "isa95_equipamento", "pressao_mpa", "umidade_pct", "temp_matriz_c"]
    assert all(col in telemetria_silver.columns for col in required_cols)

def test_producao_has_required_columns(producao_silver):
    """Verifica se as colunas essenciais da produção estão presentes."""
    required_cols = ["timestamp", "isa95_equipamento", "pecas_produzidas", "pecas_refugadas", "turno"]
    assert all(col in producao_silver.columns for col in required_cols)

def test_eventos_has_required_columns(eventos_silver):
    """Verifica se as colunas essenciais de eventos estão presentes."""
    required_cols = ["timestamp", "isa95_equipamento", "evento", "sev_codigo", "duracao_min"]
    assert all(col in eventos_silver.columns for col in required_cols)

# --- Testes de Integridade e Validade ---

def test_telemetria_timestamp_freshness(telemetria_silver):
    """Verifica se os dados de telemetria são recentes (freshness)."""
    telemetria_silver["timestamp"] = pd.to_datetime(telemetria_silver["timestamp"], errors="coerce")
    max_date = telemetria_silver["timestamp"].max()
    # Assume que os dados não devem ser mais antigos que 1 ano (para simulação)
    assert (datetime.now() - max_date).days < 365

def test_producao_pecas_range(producao_silver):
    """Verifica se a produção e refugo estão dentro de um range razoável (não negativos)."""
    assert (producao_silver["pecas_produzidas"] >= 0).all()
    assert (producao_silver["pecas_refugadas"] >= 0).all()

def test_telemetria_pressao_range(telemetria_silver):
    """Verifica se a pressão está dentro do range operacional (10 a 20 MPa)."""
    assert (telemetria_silver["pressao_mpa"].between(10, 20)).all()

def test_eventos_duracao_min_not_null(eventos_silver):
    """Verifica se a duração da parada não é nula (completeness)."""
    assert eventos_silver["duracao_min"].notnull().all()

def test_eventos_severidade_valid_codes(eventos_silver):
    """Verifica se os códigos de severidade são válidos (1, 2 ou 3)."""
    assert eventos_silver["sev_codigo"].isin([1, 2, 3]).all()

# --- Testes de Consistência (ISA-95) ---

def test_isa95_hierarquia_exists(telemetria_silver):
    """Verifica se a hierarquia ISA-95 foi aplicada corretamente."""
    required_isa95 = ["isa95_planta", "isa95_area", "isa95_linha"]
    assert all(col in telemetria_silver.columns for col in required_isa95)
    # Verifica se os valores não são vazios
    assert telemetria_silver["isa95_planta"].iloc[0] == "Tijolos_PE"
    assert telemetria_silver["isa95_area"].iloc[0] == "Extrusao"