# TTLs para cache (segundos)
CACHE_TTL_LOAD = 600        # 10 minutos para dados brutos
CACHE_TTL_AGG = 120         # 2 minutos para agregações
CACHE_TTL_ALERTS = 60       # 1 minuto para alertas

# parâmetros de alertas
ALERT_WINDOW_MINUTES = 30   # janela para média móvel (minutos)
ALERT_PERSISTENCE = 2       # número de amostras consecutivas fora do limiar
ALERT_STD_FACTOR = 3.0      # k * std para limiar dinâmico

# limites absolutos (exemplos — ajuste conforme equipamento)
PRESSURE_MAX_SAFE = 30.0    # MPa, exemplo
PRESSURE_MIN_SAFE = 0.0
TEMP_MAX_SAFE = 120.0       # °C, exemplo