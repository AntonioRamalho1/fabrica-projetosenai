"""
Script de Treinamento do Modelo de Predição de Defeitos
Treina um Random Forest com os dados de telemetria_silver.csv
"""

import pandas as pd
import numpy as np
import joblib
import os
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

print("=" * 60)
print("🧠 TREINAMENTO DO MODELO DE PREDIÇÃO DE DEFEITOS")
print("=" * 60)

# =============================================
# 1. CONFIGURAÇÃO DE CAMINHOS
# =============================================
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "app" / "data" / "processed"
MODEL_DIR = BASE_DIR / "app" / "models"

# Cria pasta de modelos se não existir
MODEL_DIR.mkdir(parents=True, exist_ok=True)

TELEMETRIA_PATH = DATA_DIR / "telemetria_silver.csv"
MODEL_PATH = MODEL_DIR / "rf_defeito.joblib"

print(f"\n📂 Caminhos configurados:")
print(f"   Dados: {TELEMETRIA_PATH}")
print(f"   Modelo: {MODEL_PATH}")

# =============================================
# 2. VALIDAÇÃO DE ARQUIVOS
# =============================================
if not TELEMETRIA_PATH.exists():
    print(f"\n❌ ERRO: Arquivo não encontrado!")
    print(f"   Esperado em: {TELEMETRIA_PATH}")
    print(f"\n🔧 Solução:")
    print(f"   1. Execute o simulador: python simulador_industrial_hibrido.py")
    print(f"   2. Execute o ETL: python processamento_etl.py")
    exit(1)

# =============================================
# 3. CARREGAMENTO DOS DADOS
# =============================================
print("\n📥 Carregando dados de telemetria...")

try:
    df = pd.read_csv(TELEMETRIA_PATH)
    print(f"   ✅ {len(df):,} registros carregados")
except Exception as e:
    print(f"   ❌ Erro ao carregar: {e}")
    exit(1)

# =============================================
# 4. PREPARAÇÃO DOS DADOS
# =============================================
print("\n⚙️  Preparando dados para treinamento...")

# Features e Target
FEATURES = ["pressao_mpa", "umidade_pct", "temp_matriz_c", "ciclo_tempo_s"]
TARGET = "flag_defeito"

# Validação de colunas
missing_cols = [col for col in FEATURES + [TARGET] if col not in df.columns]
if missing_cols:
    print(f"   ❌ Colunas faltando: {missing_cols}")
    exit(1)

# Remove NaNs
df_clean = df[FEATURES + [TARGET]].dropna()
print(f"   ✅ {len(df_clean):,} registros válidos (sem NaN)")

# Verifica se há dados suficientes
if len(df_clean) < 100:
    print(f"   ❌ Dados insuficientes para treino (mínimo: 100)")
    exit(1)

# Separa X e y
X = df_clean[FEATURES]
y = df_clean[TARGET]

# Verifica balanceamento
defeitos = y.sum()
normais = len(y) - defeitos
pct_defeitos = (defeitos / len(y)) * 100

print(f"\n📊 Distribuição dos dados:")
print(f"   OK (0):      {normais:,} ({100-pct_defeitos:.1f}%)")
print(f"   Defeito (1): {defeitos:,} ({pct_defeitos:.1f}%)")

if pct_defeitos < 1 or pct_defeitos > 99:
    print(f"   ⚠️  AVISO: Dados desbalanceados!")

# =============================================
# 5. DIVISÃO TREINO/TESTE
# =============================================
print("\n🔀 Dividindo dados em treino e teste...")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, 
    test_size=0.2, 
    random_state=42, 
    stratify=y  # Mantém proporção de classes
)

print(f"   Treino: {len(X_train):,} registros")
print(f"   Teste:  {len(X_test):,} registros")

# =============================================
# 6. TREINAMENTO DO MODELO
# =============================================
print("\n🤖 Treinando Random Forest...")

model = RandomForestClassifier(
    n_estimators=100,      # Número de árvores
    max_depth=10,          # Profundidade máxima
    min_samples_split=20,  # Mínimo de amostras para dividir
    random_state=42,
    n_jobs=-1,             # Usa todos os cores
    class_weight='balanced' # Compensa desbalanceamento
)

model.fit(X_train, y_train)
print("   ✅ Modelo treinado!")

# =============================================
# 7. AVALIAÇÃO DO MODELO
# =============================================
print("\n📈 Avaliando performance...")

# Predições
y_pred_train = model.predict(X_train)
y_pred_test = model.predict(X_test)

# Acurácia
acc_train = accuracy_score(y_train, y_pred_train)
acc_test = accuracy_score(y_test, y_pred_test)

print(f"\n   Acurácia Treino: {acc_train:.2%}")
print(f"   Acurácia Teste:  {acc_test:.2%}")

# Matriz de Confusão
print(f"\n   Matriz de Confusão (Teste):")
cm = confusion_matrix(y_test, y_pred_test)
print(f"      [[VP={cm[0,0]:,}  FP={cm[0,1]:,}]")
print(f"       [FN={cm[1,0]:,}  VP={cm[1,1]:,}]]")

# Importância das Features
print(f"\n   📊 Importância das Features:")
importances = sorted(
    zip(FEATURES, model.feature_importances_), 
    key=lambda x: x[1], 
    reverse=True
)
for feat, imp in importances:
    bar = "█" * int(imp * 50)
    print(f"      {feat:20s} {bar} {imp:.3f}")

# =============================================
# 8. SALVAMENTO DO MODELO
# =============================================
print(f"\n💾 Salvando modelo...")

try:
    joblib.dump(model, MODEL_PATH)
    print(f"   ✅ Modelo salvo em: {MODEL_PATH}")
    print(f"   Tamanho: {MODEL_PATH.stat().st_size / 1024:.1f} KB")
except Exception as e:
    print(f"   ❌ Erro ao salvar: {e}")
    exit(1)

# =============================================
# 9. TESTE RÁPIDO
# =============================================
print(f"\n🧪 Teste de predição:")

cenarios = [
    (15.0, 12.0, 60.0, 7.0, "Normal (Parâmetros Ideais)"),
    (11.0, 15.0, 68.0, 7.0, "Alto Risco (Pressão Baixa + Temp Alta)"),
    (16.0, 10.0, 55.0, 7.0, "Ótimo (Todos os Parâmetros OK)"),
]

for p, u, t, c, desc in cenarios:
    input_data = pd.DataFrame([[p, u, t, c]], columns=FEATURES)
    prob = model.predict_proba(input_data)[0][1]
    
    if prob < 0.05:
        status = "✅ Baixo Risco"
    elif prob < 0.15:
        status = "⚠️  Risco Moderado"
    else:
        status = "🚨 Alto Risco"
    
    print(f"\n   {desc}")
    print(f"      P={p} U={u} T={t}")
    print(f"      → Probabilidade: {prob*100:.2f}% {status}")

# =============================================
# 10. FINALIZAÇÃO
# =============================================
print("\n" + "=" * 60)
print("✅ TREINAMENTO CONCLUÍDO COM SUCESSO!")
print("=" * 60)
print(f"\n📋 Próximos passos:")
print(f"   1. Rode o dashboard: streamlit run app/app.py")
print(f"   2. Navegue até 'Simulador de Qualidade'")
print(f"   3. Teste diferentes combinações de parâmetros")
print("\n" + "=" * 60)