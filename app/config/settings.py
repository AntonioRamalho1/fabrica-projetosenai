import os
import yaml
from pathlib import Path

# =============================================
# 1. CONFIGURAÇÃO DE CAMINHOS (CORRIGIDO)
# =============================================

# Pega o diretório onde este arquivo (settings.py) está: .../app/config/
CURRENT_DIR = Path(__file__).resolve().parent

# Como o config.yaml está na MESMA pasta, o caminho é direto:
CONFIG_PATH = CURRENT_DIR / "config.yaml"

def load_config():
    """Carrega as configurações do arquivo YAML."""
    if not CONFIG_PATH.exists():
        print(f"⚠️ Aviso Crítico: config.yaml não encontrado em {CONFIG_PATH}")
        return {}
    
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ Erro ao ler YAML: {e}")
        return {}

# Carrega tudo para a variável CONFIG
CONFIG = load_config()

# =============================================
# 2. EXPORTAR VARIÁVEIS (Mapeamento)
# =============================================

# Configurações de Cache
CACHE_TTL_LOAD = 600
CACHE_TTL_AGG = 120

# Parâmetros de Negócio
_kpis = CONFIG.get("kpis", {}).get("costs", {})
PRECO_VENDA = _kpis.get("sales_price", 1.20)
CUSTO_POR_TIJOLO = _kpis.get("cost_per_piece", 0.45)

# Limites de Segurança
_limits = CONFIG.get("alerts", {}).get("safety_limits", {})
PRESSURE_MIN_SAFE = _limits.get("pressure", {}).get("min", 10.0)
PRESSURE_MAX_SAFE = _limits.get("pressure", {}).get("max", 18.0)
TEMP_MIN_SAFE = _limits.get("temperature", {}).get("min", 50.0)
TEMP_MAX_SAFE = _limits.get("temperature", {}).get("max", 70.0)

# Configuração do Simulador
SIMULATOR_CONFIG = CONFIG.get("simulator", {})