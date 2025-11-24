import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import random
import os

# =============================================
# CONFIGURAÇÕES DE PASTA
# =============================================
# Caminho exato para não quebrar seu projeto
# =============================================
# CONFIGURAÇÕES DE PASTA (AUTOMÁTICA)
# =============================================
import os

# Pega o diretório onde este script está rodando
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Monta o caminho relativo: ./app/data/raw
OUTPUT_DIR = os.path.join(BASE_DIR, "app", "data", "raw")

# Cria a pasta se não existir
os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"📂 Diretório de saída configurado: {OUTPUT_DIR}")

# =============================================
# CONFIGURAÇÕES DA FÁBRICA
# =============================================
NUM_MAQUINAS = 2
CICLO_MEDIO_S = 7.0
PECAS_POR_CICLO = 2
META_PRESSAO = 15.0
META_UMIDADE = 12.0
META_TEMP = 60.0

DATA_FIM = datetime.now()
DATA_INICIO_ANO = DATA_FIM - timedelta(days=365)
DATA_INICIO_TELEMETRIA = DATA_FIM - timedelta(days=30)

def turno(h):
    if 6 <= h < 14: return "Manhã"
    if 14 <= h < 22: return "Tarde"
    return "Noite"

print("\nIniciando simulação industrial AVANÇADA...")
print("Características: Sazonalidade, Standby de Energia, IDs de Eventos e Diferença de Máquinas.\n")

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

        # --- MELHORIA 1: SAZONALIDADE (Nordeste) ---
        if mes in [9, 10, 11, 12, 1]:  # Alta temporada (Verão/Seca)
            fator_sazonal = 1.15
        elif mes in [5, 6, 7]:         # Baixa temporada (Chuvas/São João)
            fator_sazonal = 0.85
        else:
            fator_sazonal = 1.0

        # --- PERSONALIDADE DA MÁQUINA ---
        if m == 1:
            # Máquina 1: Nova e Eficiente
            fator_performance = random.uniform(0.95, 1.05)
            probabilidade_quebra = 0.01 
            energia_base = 2.0 # kWh em standby (mais eficiente)
        else:
            # Máquina 2: Velha e Problemática
            fator_performance = random.uniform(0.75, 0.90)
            probabilidade_quebra = 0.04
            energia_base = 3.5 # kWh em standby (menos eficiente)

        # Máquina parada domingo ou falha
        operando = True
        if dia_sem == 6 or (random.random() < probabilidade_quebra):
            operando = False

        # --- MELHORIA 4: ENERGIA BASE (STANDBY) ---
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
            # Máquina parada ainda gasta um pouco (painel ligado, CLP)
            energia = energia_base * random.uniform(0.8, 1.0)
            status = "Parada/Manutencao"

        historico.append({
            "timestamp": t,
            "maquina_id": m,
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
print(f"✔ histórico_producao_1ano.csv gerado (Sazonalidade aplicada!)")

# =============================================
# ANOMALIAS DE TEMPERATURA
# =============================================
anomalias_temp = ["temp", "temperatura", "C", None, "ERR", "nan", "55C", "60.1 °C", "44,3"]

def aplicar_anomalia_temp(valor):
    if random.random() < 0.03: 
        return random.choice(anomalias_temp)
    return valor

# =============================================
# 2) TELEMETRIA DETALHADA (30 dias) - CENÁRIO REALISTA (INSTABILIDADE)
# =============================================
print("⚡ Gerando telemetria com instabilidade realista...")

telemetria = []
t = DATA_INICIO_TELEMETRIA

# Não existe mais "data de início da falha". O problema é aleatório/crônico.

while t <= DATA_FIM:
    for m in range(1, NUM_MAQUINAS + 1):
        # A fábrica para de madrugada (02h as 05h)
        if not (2 <= t.hour < 5): 

            # --- PERSONALIDADE DA MÁQUINA NA TELEMETRIA ---
            if m == 1:
                # Máquina 1 (Boa): Pressão estável, varia pouco
                # Média 15 MPa, Desvio 0.5 (Bem precisa)
                pressao = np.random.normal(META_PRESSAO, 0.5)
                
                # Temperatura controlada (Média 60, varia 1.5)
                temp_real = np.random.normal(META_TEMP, 1.5)
                
                # Umidade consistente
                umidade = np.random.normal(META_UMIDADE, 0.5)
                
            else:
                # Máquina 2 (Instável): A pressão "samba" muito
                # Média um pouco menor (14.5) e Desvio ENORME (2.5)
                # Isso faz ela cair abaixo de 12 (defeito) várias vezes ao dia, aleatoriamente
                pressao = np.random.normal(14.5, 2.5)
                
                # Superaquecimento aleatório (picos de calor)
                if random.random() < 0.1: # 10% do tempo ela esquenta
                    temp_real = np.random.normal(75.0, 3.0)
                else:
                    temp_real = np.random.normal(62.0, 2.0)
                
                # Umidade varia mais
                umidade = np.random.normal(META_UMIDADE, 1.5)

            # Aplica sujeira na temperatura (sensor falhando as vezes)
            temperatura_final = aplicar_anomalia_temp(round(temp_real, 1))

            # --- REGRA FÍSICA DE DEFEITO ---
            flag = 0
            
            # Se a pressão cair muito (<12) OU esquentar demais (>70) = DEFEITO
            # Como a Máq 2 tem desvio alto (2.5), ela vai cair < 12 com frequência
            if pressao < 12.0 or umidade > 14.5 or temp_real > 70.0:
                flag = 1
            
            # Se o sensor de temperatura falhou (sujeira), também marca alerta
            if temperatura_final in anomalias_temp:
                flag = 1 

            telemetria.append({
                "timestamp": t,
                "maquina_id": m,
                "ciclo_tempo_s": round(np.random.normal(CICLO_MEDIO_S, 0.25), 2),
                "pressao_mpa": round(pressao, 2),
                "umidade_pct": round(umidade, 2),
                "temp_matriz_c": temperatura_final,
                "pecas_produzidas": PECAS_POR_CICLO,
                "flag_defeito": flag
            })

    t += timedelta(seconds=CICLO_MEDIO_S)

df_tel = pd.DataFrame(telemetria)
path_tel = os.path.join(OUTPUT_DIR, "telemetria_detalhada_30dias.csv")
df_tel.to_csv(path_tel, index=False)
print(f"✔ telemetria_detalhada_30dias.csv gerado (Cenário Instável)")

# =============================================
# 3) EVENTOS INDUSTRIAIS - COM IDs ÚNICOS
# =============================================
print("🚨 Gerando eventos industriais...")

eventos = []
tipos = ["Falha de Pressão", "Sobrecarga Elétrica", "Baixa Umidade", "Falha de Motor", "Parada Programada"]

for i in range(200):
    # --- MELHORIA 3: ID ÚNICO DE EVENTO ---
    evento_id = f"EVT-{2025}{i+1:04d}"
    
    eventos.append({
        "evento_id": evento_id,
        "timestamp": DATA_INICIO_ANO + timedelta(hours=random.randint(0, 365*24)),
        "maquina_id": random.randint(1, NUM_MAQUINAS),
        "evento": random.choice(tipos),
        "severidade": random.choice(["Baixa", "Média", "Alta"]),
        "origem": random.choice(["Sensor", "Operador", "SCADA"])
    })

df_evt = pd.DataFrame(eventos)
path_evt = os.path.join(OUTPUT_DIR, "eventos_industriais.csv")
df_evt.to_csv(path_evt, index=False)
print(f"✔ eventos_industriais.csv gerado (IDs adicionados)")

# =============================================
# 4) UNS SIMBÓLICO
# =============================================
uns = {
    "empresa": "Fabrica_Tijolos_Eco",
    "planta": "PE_Recife",
    "estrutura": "ISA-95",
    "last_update": str(datetime.now())
}

path_uns = os.path.join(OUTPUT_DIR, "uns_tags.json")
with open(path_uns, "w") as f:
    json.dump(uns, f, indent=4)

print(f"✔ Salvo em: {path_uns}")
print("\n🎉 Simulação COMPLETA! Dados salvos com sucesso.")