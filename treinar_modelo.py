"""
Script de Treinamento do Modelo de Predição de Defeitos
Treina um Random Forest com os dados de telemetria_silver.csv

MELHORIAS IMPLEMENTADAS:
- ✅ Métricas avançadas (Precision, Recall, F1, ROC-AUC)
- ✅ Matriz de confusão visual
- ✅ Feature Importance detalhado
- ✅ Validação temporal (train/test por data)
- ✅ Cross-validation estratificado
- ✅ Logging estruturado
- ✅ Relatório completo em JSON
"""

import pandas as pd
import numpy as np
import joblib
import json
import logging
from pathlib import Path
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.metrics import (
    accuracy_score, 
    classification_report, 
    confusion_matrix,
    precision_recall_fscore_support,
    roc_auc_score,
    roc_curve
)
import matplotlib.pyplot as plt
import seaborn as sns

# =============================================
# CONFIGURAÇÃO DE LOGGING
# =============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('training.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =============================================
# 1. CONFIGURAÇÃO DE CAMINHOS
# =============================================
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "app" / "data" / "silver" 
MODEL_DIR = BASE_DIR / "app" / "models"
REPORTS_DIR = BASE_DIR / "reports"

# Cria pastas necessárias
for directory in [MODEL_DIR, REPORTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

TELEMETRIA_PATH = DATA_DIR / "telemetria_silver.csv"
MODEL_PATH = MODEL_DIR / "rf_defeito.joblib"
REPORT_PATH = REPORTS_DIR / f"training_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

logger.info("="*70)
logger.info("🧠 TREINAMENTO DO MODELO DE PREDIÇÃO DE DEFEITOS - VERSÃO ROBUSTA")
logger.info("="*70)
logger.info(f"Dados: {TELEMETRIA_PATH}")
logger.info(f"Modelo: {MODEL_PATH}")
logger.info(f"Relatório: {REPORT_PATH}")

# =============================================
# 2. VALIDAÇÃO DE ARQUIVOS
# =============================================
if not TELEMETRIA_PATH.exists():
    logger.error(f"Arquivo não encontrado: {TELEMETRIA_PATH}")
    logger.info("Execute: python simulador_industrial_hibrido.py && python pipeline_etl.py")
    exit(1)

# =============================================
# 3. CARREGAMENTO DOS DADOS
# =============================================
logger.info("📥 Carregando dados de telemetria...")

try:
    df = pd.read_csv(TELEMETRIA_PATH)
    logger.info(f"✅ {len(df):,} registros carregados")
except Exception as e:
    logger.error(f"Erro ao carregar dados: {e}")
    exit(1)

# =============================================
# 4. PREPARAÇÃO DOS DADOS
# =============================================
logger.info("⚙️  Preparando dados para treinamento...")

FEATURES = ["pressao_mpa", "umidade_pct", "temp_matriz_c", "ciclo_tempo_s"]
TARGET = "flag_defeito"

# Validação de colunas
missing_cols = [col for col in FEATURES + [TARGET] if col not in df.columns]
if missing_cols:
    logger.error(f"Colunas faltando: {missing_cols}")
    exit(1)

# Remove NaNs
df_clean = df[FEATURES + [TARGET] + ["timestamp"]].dropna()
logger.info(f"✅ {len(df_clean):,} registros válidos (sem NaN)")

if len(df_clean) < 100:
    logger.error(f"Dados insuficientes para treino (mínimo: 100)")
    exit(1)

# Verifica balanceamento
defeitos = df_clean[TARGET].sum()
normais = len(df_clean) - defeitos
pct_defeitos = (defeitos / len(df_clean)) * 100

logger.info(f"📊 Distribuição dos dados:")
logger.info(f"   OK (0):      {normais:,} ({100-pct_defeitos:.1f}%)")
logger.info(f"   Defeito (1): {defeitos:,} ({pct_defeitos:.1f}%)")

if pct_defeitos < 1 or pct_defeitos > 99:
    logger.warning("⚠️  Dados desbalanceados detectados!")

# =============================================
# 5. DIVISÃO TREINO/TESTE COM VALIDAÇÃO TEMPORAL
# =============================================
logger.info("🔀 Dividindo dados com validação temporal...")

# Converte timestamp para datetime
df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], errors='coerce')
df_clean = df_clean.dropna(subset=['timestamp']).sort_values('timestamp')

# Split temporal: 80% primeiros dados para treino, 20% últimos para teste
split_idx = int(len(df_clean) * 0.8)
train_df = df_clean.iloc[:split_idx]
test_df = df_clean.iloc[split_idx:]

X_train = train_df[FEATURES]
y_train = train_df[TARGET]
X_test = test_df[FEATURES]
y_test = test_df[TARGET]

logger.info(f"   Treino: {len(X_train):,} registros ({train_df['timestamp'].min()} a {train_df['timestamp'].max()})")
logger.info(f"   Teste:  {len(X_test):,} registros ({test_df['timestamp'].min()} a {test_df['timestamp'].max()})")

# =============================================
# 6. TREINAMENTO DO MODELO
# =============================================
logger.info("🤖 Treinando Random Forest com hiperparâmetros otimizados...")

model = RandomForestClassifier(
    n_estimators=200,       # Mais árvores para melhor generalização
    max_depth=12,           # Profundidade controlada
    min_samples_split=20,
    min_samples_leaf=5,
    random_state=42,
    n_jobs=-1,
    class_weight='balanced',
    max_features='sqrt'     # Diversidade entre árvores
)

model.fit(X_train, y_train)
logger.info("✅ Modelo treinado!")

# =============================================
# 7. CROSS-VALIDATION ESTRATIFICADO
# =============================================
logger.info("🔄 Executando Cross-Validation estratificado (5-fold)...")

cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
cv_scores = cross_val_score(model, X_train, y_train, cv=cv, scoring='f1')

logger.info(f"   F1-Score por fold: {[f'{s:.3f}' for s in cv_scores]}")
logger.info(f"   F1-Score médio: {cv_scores.mean():.3f} (±{cv_scores.std():.3f})")

# =============================================
# 8. AVALIAÇÃO COMPLETA DO MODELO
# =============================================
logger.info("📈 Avaliando performance com métricas avançadas...")

# Predições
y_pred_train = model.predict(X_train)
y_pred_test = model.predict(X_test)
y_proba_test = model.predict_proba(X_test)[:, 1]

# Métricas básicas
acc_train = accuracy_score(y_train, y_pred_train)
acc_test = accuracy_score(y_test, y_pred_test)

# Métricas avançadas
precision, recall, f1, support = precision_recall_fscore_support(
    y_test, y_pred_test, average='binary', zero_division=0
)

# ROC-AUC (se houver ambas as classes)
try:
    roc_auc = roc_auc_score(y_test, y_proba_test)
except ValueError:
    roc_auc = None
    logger.warning("⚠️  ROC-AUC não calculado (apenas uma classe presente)")

logger.info(f"\n📊 MÉTRICAS DE PERFORMANCE:")
logger.info(f"   Acurácia Treino:  {acc_train:.4f}")
logger.info(f"   Acurácia Teste:   {acc_test:.4f}")
logger.info(f"   Precision:        {precision:.4f}")
logger.info(f"   Recall:           {recall:.4f}")
logger.info(f"   F1-Score:         {f1:.4f}")
if roc_auc:
    logger.info(f"   ROC-AUC:          {roc_auc:.4f}")

# Matriz de Confusão
cm = confusion_matrix(y_test, y_pred_test)
logger.info(f"\n📋 MATRIZ DE CONFUSÃO:")
logger.info(f"                Predito")
logger.info(f"              OK  Defeito")
logger.info(f"   Real OK    {cm[0,0]:4d}  {cm[0,1]:4d}")
logger.info(f"   Defeito    {cm[1,0]:4d}  {cm[1,1]:4d}")

# Cálculo de métricas derivadas da matriz
tn, fp, fn, tp = cm.ravel() if cm.size == 4 else (0, 0, 0, 0)
specificity = tn / (tn + fp) if (tn + fp) > 0 else 0
npv = tn / (tn + fn) if (tn + fn) > 0 else 0

logger.info(f"\n📐 MÉTRICAS DERIVADAS:")
logger.info(f"   Sensibilidade (Recall): {recall:.4f}")
logger.info(f"   Especificidade:         {specificity:.4f}")
logger.info(f"   VPP (Precision):        {precision:.4f}")
logger.info(f"   VPN:                    {npv:.4f}")

# =============================================
# 9. FEATURE IMPORTANCE DETALHADO
# =============================================
logger.info(f"\n📊 FEATURE IMPORTANCE (Importância das Variáveis):")

importances = pd.DataFrame({
    'feature': FEATURES,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

for _, row in importances.iterrows():
    bar = "█" * int(row['importance'] * 60)
    logger.info(f"   {row['feature']:20s} {bar} {row['importance']:.4f}")

# =============================================
# 10. VISUALIZAÇÕES
# =============================================
logger.info("📊 Gerando visualizações...")

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Análise do Modelo de Predição de Defeitos', fontsize=16, fontweight='bold')

# 1. Matriz de Confusão
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=axes[0, 0], 
            xticklabels=['OK', 'Defeito'], yticklabels=['OK', 'Defeito'])
axes[0, 0].set_title('Matriz de Confusão')
axes[0, 0].set_ylabel('Valor Real')
axes[0, 0].set_xlabel('Valor Predito')

# 2. Feature Importance
axes[0, 1].barh(importances['feature'], importances['importance'], color='steelblue')
axes[0, 1].set_xlabel('Importância')
axes[0, 1].set_title('Importância das Features')
axes[0, 1].invert_yaxis()

# 3. ROC Curve (se disponível)
if roc_auc:
    fpr, tpr, thresholds = roc_curve(y_test, y_proba_test)
    axes[1, 0].plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC (AUC = {roc_auc:.3f})')
    axes[1, 0].plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--', label='Baseline')
    axes[1, 0].set_xlim([0.0, 1.0])
    axes[1, 0].set_ylim([0.0, 1.05])
    axes[1, 0].set_xlabel('Taxa de Falso Positivo')
    axes[1, 0].set_ylabel('Taxa de Verdadeiro Positivo')
    axes[1, 0].set_title('Curva ROC')
    axes[1, 0].legend(loc="lower right")
else:
    axes[1, 0].text(0.5, 0.5, 'ROC não disponível\n(apenas uma classe)', 
                    ha='center', va='center', fontsize=12)
    axes[1, 0].set_title('Curva ROC')

# 4. Distribuição de Probabilidades
axes[1, 1].hist(y_proba_test[y_test == 0], bins=30, alpha=0.6, label='OK', color='green')
axes[1, 1].hist(y_proba_test[y_test == 1], bins=30, alpha=0.6, label='Defeito', color='red')
axes[1, 1].set_xlabel('Probabilidade de Defeito')
axes[1, 1].set_ylabel('Frequência')
axes[1, 1].set_title('Distribuição de Probabilidades')
axes[1, 1].legend()

plt.tight_layout()
plot_path = REPORTS_DIR / f"model_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
plt.savefig(plot_path, dpi=150, bbox_inches='tight')
logger.info(f"✅ Visualizações salvas em: {plot_path}")

# =============================================
# 11. SALVAMENTO DO MODELO
# =============================================
logger.info("💾 Salvando modelo...")

try:
    joblib.dump(model, MODEL_PATH)
    logger.info(f"✅ Modelo salvo em: {MODEL_PATH}")
    logger.info(f"   Tamanho: {MODEL_PATH.stat().st_size / 1024:.1f} KB")
except Exception as e:
    logger.error(f"Erro ao salvar modelo: {e}")
    exit(1)

# =============================================
# 12. RELATÓRIO COMPLETO EM JSON
# =============================================
logger.info("📄 Gerando relatório completo...")

report = {
    "timestamp": datetime.now().isoformat(),
    "data": {
        "total_records": len(df_clean),
        "train_records": len(X_train),
        "test_records": len(X_test),
        "train_period": [str(train_df['timestamp'].min()), str(train_df['timestamp'].max())],
        "test_period": [str(test_df['timestamp'].min()), str(test_df['timestamp'].max())],
        "class_distribution": {
            "ok": int(normais),
            "defeito": int(defeitos),
            "defeito_pct": float(pct_defeitos)
        }
    },
    "model": {
        "algorithm": "RandomForestClassifier",
        "n_estimators": 200,
        "max_depth": 12,
        "class_weight": "balanced",
        "features": FEATURES
    },
    "performance": {
        "train_accuracy": float(acc_train),
        "test_accuracy": float(acc_test),
        "precision": float(precision),
        "recall": float(recall),
        "f1_score": float(f1),
        "roc_auc": float(roc_auc) if roc_auc else None,
        "specificity": float(specificity),
        "npv": float(npv),
        "cv_f1_mean": float(cv_scores.mean()),
        "cv_f1_std": float(cv_scores.std())
    },
    "confusion_matrix": {
        "true_negative": int(tn),
        "false_positive": int(fp),
        "false_negative": int(fn),
        "true_positive": int(tp)
    },
    "feature_importance": {
        feat: float(imp) for feat, imp in zip(FEATURES, model.feature_importances_)
    }
}

with open(REPORT_PATH, 'w') as f:
    json.dump(report, f, indent=2)

logger.info(f"✅ Relatório salvo em: {REPORT_PATH}")

# =============================================
# 13. TESTE DE CENÁRIOS
# =============================================
logger.info("\n🧪 Teste de Cenários de Produção:")

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
    
    logger.info(f"\n   {desc}")
    logger.info(f"      Pressão={p} Umidade={u} Temp={t}")
    logger.info(f"      → Probabilidade: {prob*100:.2f}% {status}")

# =============================================
# 14. FINALIZAÇÃO
# =============================================
logger.info("\n" + "="*70)
logger.info("✅ TREINAMENTO CONCLUÍDO COM SUCESSO!")
logger.info("="*70)
logger.info("\n📋 Artefatos gerados:")
logger.info(f"   1. Modelo treinado: {MODEL_PATH}")
logger.info(f"   2. Relatório JSON: {REPORT_PATH}")
logger.info(f"   3. Visualizações: {plot_path}")
logger.info(f"   4. Log completo: training.log")
logger.info("\n📋 Próximos passos:")
logger.info("   1. streamlit run app/app.py")
logger.info("   2. Navegue até 'Simulador de Qualidade'")
logger.info("   3. Teste diferentes combinações de parâmetros")
logger.info("="*70)