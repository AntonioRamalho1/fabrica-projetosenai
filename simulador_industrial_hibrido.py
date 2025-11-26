import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import random
import os
import sys

# --- 1. CONFIGURAÇÃO DE IMPORTAÇÃO (Conecta ao settings.py) ---
# Adiciona a pasta atual ao path para conseguir importar app.config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from app.config.settings import SIMULATOR_CONFIG
    print("✅ Configurações carregadas do config.yaml via settings.py")
except ImportError:
    print("⚠️ Erro ao importar settings.py. Usando padrões de fallback.")
    SIMULATOR_CONFIG = {}

# --- 2. DEFINIÇÃO DE PARÂMETROS (Lê do Config ou usa Padrão) ---
# Tenta ler do YAML, se não conseguir, usa o valor padrão (segundo argumento)
PARAMS = SIMULATOR_CONFIG.get('operating_parameters', {})

META_PRESSAO = PARAMS.get('pressure', {}).get('mean', 15.0)
STD_PRESSAO = PARAMS.get('pressure', {}).get('std', 1.2)

META_UMIDADE = PARAMS.get('humidity', {}).get('mean', 12.0)
STD_UMIDADE = PARAMS.get('humidity', {}).get('std', 1.5)

META_TEMP = PARAMS.get('temperature', {}).get('mean', 60.0)
STD_TEMP = PARAMS.get('temperature', {}).get('std', 5.0)

CICLO_MEDIO_S = PARAMS.get('cycle_time', {}).get('mean', 7.0)
PECAS_POR_CICLO = 2
NUM_MAQUINAS = 2

# Configuração de Pastas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "app", "data", "raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"📂 Diretório de saída: {OUTPUT_DIR}")

# Datas
DATA_FIM = datetime.now()
days_tele = SIMULATOR_CONFIG.get('telemetry_days', 30)
years_hist = SIMULATOR_CONFIG.get('production_history_years', 1)

DATA_INICIO_ANO = DATA_FIM - timedelta(days=365 * years_hist)
DATA_INICIO_TELEMETRIA = DATA_FIM - timedelta(days=days_tele)

def turno(h):
    if 6 <= h < 14: return "Manhã"
    if 14 <= h < 22: return "Tarde"
    return "Noite"

def gerar_topico(maquina_id, tipo_msg="DDATA"):
    return f"spBv1.0/EcoTijolos/{tipo_msg}/Prensagem/Prensa_{maquina_id:02d}"

print("\nIniciando simulação industrial AVANÇADA...")
print(f"⚙️ Parâmetros Base: Pressão={META_PRESSAO} MPa | Temp={META_TEMP} °C")

# =============================================
# 1) HISTÓRICO (1 ANO) - COM SAZONALIDADE E STANDBY
# =============================================
print("⏳ Gerando histórico anual...")

historico = []
t = DATA_INICIO_ANO

while t <= DATA_FIM:
    for m in range(1, NUM_MAQUINAS + 1):

        dia_sem = t.weekday()
        hr = t.hour
        mes = t.month

        # --- SAZONALIDADE (Nordeste) ---
        if mes in [9, 10, 11, 12, 1]:  # Alta temporada
            fator_sazonal = 1.15
        elif mes in [5, 6, 7]:         # Baixa temporada
            fator_sazonal = 0.85
        else:
            fator_sazonal = 1.0

        # --- PERSONALIDADE DA MÁQUINA ---
        if m == 1:
            # Máquina 1: Nova e Eficiente
            fator_performance = random.uniform(0.95, 1.05)
            probabilidade_quebra = 0.01 
            energia_base = 2.0 
        else:
            # Máquina 2: Velha e Problemática
            fator_performance = random.uniform(0.75, 0.90)
            probabilidade_quebra = 0.04
            energia_base = 3.5 

        # Máquina parada domingo ou falha
        operando = True
        if dia_sem == 6 or (random.random() < probabilidade_quebra):
            operando = False

        if operando:
            fator_turno = 1.0 if 6 <= hr < 22 else 0.9
            capacidade_teorica = (3600 / CICLO_MEDIO_S) * PECAS_POR_CICLO

            # Produção com Sazonalidade
            producao = int(capacidade_teorica * fator_turno * fator_performance * fator_sazonal)
            
            # Consumo = Base + Variável (Produção)
            energia = energia_base + (producao * 0.012 * random.uniform(0.95, 1.05))
            
            taxa_def = random.uniform(0.003, 0.03) if m == 1 else random.uniform(0.02, 0.05)
            refugos = int(producao * taxa_def)

            status = "Operando"
        else:
            producao = 0
            refugos = 0
            energia = energia_base * random.uniform(0.8, 1.0)
            status = "Parada/Manutencao"

        historico.append({
            "timestamp": t,
            "maquina_id": m,
            "topico_uns": gerar_topico(m, "DDATA"),
            "turno": turno(hr),
            "pecas_produzidas": producao,
            "pecas_refugadas": refugos,
            "consumo_kwh": round(energia, 3),
            "status": status
        })

    t += timedelta(hours=1)

df_hist = pd.DataFrame(historico)
path_hist = os.path.join(OUTPUT_DIR, "historico_producao_1ano.csv")
df_hist.to_csv(path_hist, index=False)
print(f"✔ histórico_producao_1ano.csv gerado")

# =============================================
# ANOMALIAS DE TEMPERATURA
# =============================================
# Mantivemos sua lista complexa de erros!
anomalias_temp = ["temp", "temperatura", "C", None, "ERR", "nan", "55C", "60.1 °C", "44,3"]

def aplicar_anomalia_temp(valor):
    if random.random() < 0.03: 
        return random.choice(anomalias_temp)
    return valor

# =============================================
# 2) TELEMETRIA DETALHADA (30 dias)
# =============================================
print("⚡ Gerando telemetria detalhada...")

telemetria = []
t = DATA_INICIO_TELEMETRIA

while t <= DATA_FIM:
    for m in range(1, NUM_MAQUINAS + 1):
        if not (2 <= t.hour < 5): # Pausa na madrugada

            # Máquina 2 oscila mais (Lógica complexa mantida)
            desvio_extra = 2.5 if m == 2 else 0.5
            pressao = np.random.normal(META_PRESSAO, desvio_extra)
            
            if m == 2 and random.random() < 0.1:
                 temp_real = np.random.normal(META_TEMP + 15, 3.0) # Superaquecimento
            else:
                 temp_real = np.random.normal(META_TEMP, STD_TEMP)

            umidade = np.random.normal(META_UMIDADE, STD_UMIDADE)
            
            # Aplica sujeira na temperatura
            temperatura_final = aplicar_anomalia_temp(round(temp_real, 1))

            # Regra de Ouro da Qualidade
            flag = 0
            # Converte para float seguro para validar a regra lógica
            try:
                temp_valida = float(temperatura_final)
            except:
                temp_valida = 999 # Se for erro de texto, considera falha
                
            if pressao < 12.0 or umidade > 14.5 or temp_valida > 68.0:
                flag = 1
            
            if temperatura_final in anomalias_temp:
                flag = 1 

            telemetria.append({
                "timestamp": t,
                "maquina_id": m,
                "topico_uns": gerar_topico(m, "DDATA"),
                "ciclo_tempo_s": round(np.random.normal(CICLO_MEDIO_S, 0.25), 2),
                "pressao_mpa": round(pressao, 2),
                "umidade_pct": round(umidade, 2),
                "temp_matriz_c": temperatura_final,
                "pecas_produzidas": PECAS_POR_CICLO,
                "flag_defeito": flag
            })

    t += timedelta(minutes=5) # Amostragem a cada 5 min (ajuste para performance)

df_tel = pd.DataFrame(telemetria)
path_tel = os.path.join(OUTPUT_DIR, "telemetria_detalhada_30dias.csv")
df_tel.to_csv(path_tel, index=False)
print(f"✔ telemetria_detalhada_30dias.csv gerado")

# =============================================
# 3) EVENTOS INDUSTRIAIS - COM IDs ÚNICOS
# =============================================
print("🚨 Gerando eventos industriais...")

eventos = []
tipos = ["Falha de Pressão", "Sobrecarga Elétrica", "Baixa Umidade", "Falha de Motor", "Parada Programada"]

for i in range(200):
    evento_id = f"EVT-{2025}{i+1:04d}"
    
    eventos.append({
        "evento_id": evento_id,
        "timestamp": DATA_INICIO_ANO + timedelta(hours=random.randint(0, 365*24)),
        "maquina_id": random.randint(1, NUM_MAQUINAS),
        "topico_uns": gerar_topico(random.randint(1, NUM_MAQUINAS), "DALARM"),
        "evento": random.choice(tipos),
        "severidade": random.choice(["Baixa", "Média", "Alta"]),
        "origem": random.choice(["Sensor", "Operador", "SCADA"])
    })

df_evt = pd.DataFrame(eventos)
path_evt = os.path.join(OUTPUT_DIR, "eventos_industriais.csv")
df_evt.to_csv(path_evt, index=False)
print(f"✔ eventos_industriais.csv gerado")

# =============================================
# 4) UNS SIMBÓLICO
# =============================================
uns = {
    "empresa": PARAMS.get('factory_name', "EcoTijolos"),
    "estrutura": "ISA-95",
    "last_update": str(datetime.now())
}
path_uns = os.path.join(OUTPUT_DIR, "uns_tags.json")
with open(path_uns, "w") as f:
    json.dump(uns, f, indent=4)

print(f"✔ Salvo em: {path_uns}")
print("\n🎉 Simulação COMPLETA! Integrada com config.yaml.")