import joblib
import os
import numpy as np
import pandas as pd
from typing import Union

# Caminho para o modelo (assumindo que está na raiz do projeto)
MODEL_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models", "rf_defeito.joblib")

# Variáveis de controle para o modelo
_model = None
_model_loaded = False

# Colunas de features esperadas pelo modelo (baseado na análise do simulador)
# O simulador usa: pressao_mpa, umidade_pct, temp_matriz_c, ciclo_tempo_s
# Vamos assumir que o modelo foi treinado com estas 4 features.
FEATURE_COLS = ["pressao_mpa", "umidade_pct", "temp_matriz_c", "ciclo_tempo_s"]


def load_model():
    """Carrega o modelo Random Forest treinado."""
    global _model, _model_loaded
    if not _model_loaded:
        try:
            _model = joblib.load(MODEL_PATH)
            _model_loaded = True
            print(f"Modelo carregado com sucesso de: {MODEL_PATH}")
        except FileNotFoundError:
            print(f"ERRO: Arquivo do modelo não encontrado em: {MODEL_PATH}")
            _model = None
            _model_loaded = False
        except Exception as e:
            print(f"ERRO ao carregar o modelo: {e}")
            _model = None
            _model_loaded = False
    return _model


def predict_defeito_prob(pressao: float, umidade: float, temperatura: float, ciclo_tempo: float = 7.0) -> Union[float, None]:
    """
    Prevê a probabilidade de defeito com base nos parâmetros de telemetria.
    Retorna a probabilidade (0.0 a 1.0) ou None em caso de erro.
    """
    model = load_model()
    if model is None:
        return None

    # Cria o DataFrame de entrada com as features na ordem correta
    data = {
        "pressao_mpa": [pressao],
        "umidade_pct": [umidade],
        "temp_matriz_c": [temperatura],
        "ciclo_tempo_s": [ciclo_tempo]
    }
    
    # Garante que o DataFrame tem as colunas na ordem esperada pelo modelo
    X = pd.DataFrame(data, columns=FEATURE_COLS)

    try:
        # A previsão de probabilidade retorna um array [[prob_classe_0, prob_classe_1]]
        # Queremos a probabilidade da classe 1 (defeito)
        prob_defeito = model.predict_proba(X)[0][1]
        return float(prob_defeito)
    except Exception as e:
        print(f"Erro durante a previsão: {e}")
        return None

if __name__ == '__main__':
    # Teste rápido
    load_model()
    # Cenário 1: Parâmetros normais (deve dar baixa probabilidade)
    prob1 = predict_defeito_prob(pressao=15.0, umidade=12.0, temperatura=60.0)
    print(f"Probabilidade de defeito (Normal): {prob1:.4f}")
    
    # Cenário 2: Parâmetros anormais (pressão baixa, umidade alta - deve dar alta probabilidade)
    prob2 = predict_defeito_prob(pressao=11.0, umidade=15.0, temperatura=55.0)
    print(f"Probabilidade de defeito (Anormal): {prob2:.4f}")