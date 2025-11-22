import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import random

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

print("\nIniciando simulação industrial híbrida...")
print("Gerando datasets realistas com falhas, anomalias e sazonalidade.\n")

# =============================================
# 1) HISTÓRICO (1 ANO, HORA A HORA)
# =============================================
print("⏳ Gerando histórico anual...")

historico = []
t = DATA_INICIO_ANO

while t <= DATA_FIM:
    for m in range(1, NUM_MAQUINAS + 1):

        dia_sem = t.weekday()
        hr = t.hour

        # Máquina parada domingo ou falha
        operando = True
        if dia_sem == 6 or (random.random() < 0.02):
            operando = False

        if operando:
            fator_turno = 1.0 if 6 <= hr < 22 else 0.9
            capacidade = (3600 / CICLO_MEDIO_S) * PECAS_POR_CICLO

            producao = int(capacidade * fator_turno * random.uniform(0.8, 1.02))
            energia = producao * 0.01 * random.uniform(0.9, 1.1)
            taxa_def = random.uniform(0.003, 0.03)
            refugos = int(producao * taxa_def)

            historico.append({
                "timestamp": t,
                "maquina_id": m,
                "turno": turno(hr),
                "pecas_produzidas": producao,
                "pecas_refugadas": refugos,
                "consumo_kwh": round(energia, 3),
                "status": "Operando"
            })
        else:
            historico.append({
                "timestamp": t,
                "maquina_id": m,
                "turno": turno(hr),
                "pecas_produzidas": 0,
                "pecas_refugadas": 0,
                "consumo_kwh": 0.0,
                "status": "Parada/Manutencao"
            })

    t += timedelta(hours=1)

df_hist = pd.DataFrame(historico)
df_hist.to_csv("historico_producao_1ano.csv", index=False)
print(f"✔ histórico_producao_1ano.csv gerado: {len(df_hist)} linhas\n")

# =============================================
# FUNÇÃO DE ANOMALIAS DE TEMPERATURA
# =============================================
anomalias_temp = [
    "temp", "temperatura", "C", None, "ERR", "nan",
    "55C", "60.1 °C", "44,3"
]

def aplicar_anomalia_temp(valor):
    if random.random() < 0.03:  # 3% de anomalias
        return random.choice(anomalias_temp)
    return valor

# =============================================
# 2) TELEMETRIA DETALHADA (30 dias, ciclo-a-ciclo)
# =============================================
print("⚡ Gerando telemetria detalhada...")

telemetria = []
t = DATA_INICIO_TELEMETRIA

inicio_falha = DATA_INICIO_TELEMETRIA + timedelta(days=12)

while t <= DATA_FIM:
    for m in range(1, NUM_MAQUINAS + 1):

        if not (2 <= t.hour < 5):

            fator_falha = 0
            if t > inicio_falha:
                horas_falha = (t - inicio_falha).total_seconds() / 3600
                fator_falha = min(4.0, horas_falha * 0.08)

            pressao = np.random.normal(META_PRESSAO - fator_falha, 0.5)
            umidade = np.random.normal(META_UMIDADE, 0.8)
            temp_real = round(np.random.normal(META_TEMP, 1.7), 1)

            temperatura_final = aplicar_anomalia_temp(temp_real)

            flag = 0
            if isinstance(temperatura_final, (int, float)):
                if pressao < 11.5 or umidade > 14.5:
                    flag = 1
            else:
                flag = 1  # anomalia = considerado defeito

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
df_tel.to_csv("telemetria_detalhada_30dias.csv", index=False)
print(f"✔ telemetria_detalhada_30dias.csv gerado: {len(df_tel)} linhas\n")

# =============================================
# 3) EVENTOS INDUSTRIAIS
# =============================================
print("🚨 Gerando eventos industriais...")

eventos = []
tipos = ["Falha de Pressão", "Sobrecarga Elétrica", "Baixa Umidade", "Falha de Motor", "Parada Programada"]

for i in range(200):
    eventos.append({
        "timestamp": DATA_INICIO_ANO + timedelta(hours=random.randint(0, 365*24)),
        "maquina_id": random.randint(1, NUM_MAQUINAS),
        "evento": random.choice(tipos),
        "severidade": random.choice(["Baixa", "Média", "Alta"]),
        "origem": random.choice(["Sensor", "Operador", "SCADA"])
    })

df_evt = pd.DataFrame(eventos)
df_evt.to_csv("eventos_industriais.csv", index=False)
print(f"✔ eventos_industriais.csv gerado: {len(df_evt)} eventos\n")

# =============================================
# 4) UNS SIMBÓLICO (ISA-95 → JSON)
# =============================================
print("📡 Criando UNS (Unified Namespace)...")

uns = {
    "empresa": "Fabrica_Tijolos_Eco",
    "planta": {
        "PE_Recife": {
            "linha_1": {
                f"maquina_{m}": {
                    "sensores": {
                        "pressao": f"Fabrica/PE_Recife/Linha1/Maquina{m}/Sensores/Pressao",
                        "umidade": f"Fabrica/PE_Recife/Linha1/Maquina{m}/Sensores/Umidade",
                        "temp": f"Fabrica/PE_Recife/Linha1/Maquina{m}/Sensores/Temperatura"
                    },
                    "eventos": f"Fabrica/PE_Recife/Linha1/Maquina{m}/Eventos"
                }
                for m in range(1, NUM_MAQUINAS + 1)
            }
        }
    }
}

with open("uns_tags.json", "w") as f:
    json.dump(uns, f, indent=4)

print("✔ uns_tags.json gerado\n")
print("🎉 Simulação COMPLETA! Todos os arquivos estão prontos.")
