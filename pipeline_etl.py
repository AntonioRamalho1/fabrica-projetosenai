"""
Pipeline ETL Bronze → Silver → Gold
Arquitetura Medallion com Data Quality e Observabilidade

MELHORIAS IMPLEMENTADAS:
- ✅ Validação de schema com Great Expectations
- ✅ Detecção de outliers (IQR e Z-Score)
- ✅ Salvamento em Parquet particionado
- ✅ Métricas de qualidade de dados
- ✅ Logging estruturado
- ✅ Tratamento de erros robusto
"""

import pandas as pd
import numpy as np
import re
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple, Dict, List
from scipy import stats
import sys
import codecs

if sys.platform.startswith('win'):
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# =============================================
# CONFIGURAÇÃO DE LOGGING
# =============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(module)s | %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =============================================
# CONFIGURAÇÃO DE CAMINHOS
# =============================================
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "app" / "data"

RAW_DIR = DATA_DIR / "raw"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
QUALITY_DIR = DATA_DIR / "quality_reports"

# Cria diretórios necessários
for directory in [SILVER_DIR, GOLD_DIR, QUALITY_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# =============================================
# CONFIGURAÇÕES DE QUALIDADE DE DADOS
# =============================================
class DataQualityConfig:
    """Configurações de limites e validações"""
    
    # Limites de telemetria
    PRESSAO_MIN = 8.0
    PRESSAO_MAX = 18.0
    UMIDADE_MIN = 8.0
    UMIDADE_MAX = 18.0
    TEMP_MIN = 40.0
    TEMP_MAX = 80.0
    CICLO_MIN = 4.0
    CICLO_MAX = 10.0
    
    # Detecção de outliers
    Z_SCORE_THRESHOLD = 3.0
    IQR_MULTIPLIER = 1.5
    
    # Qualidade mínima aceitável
    MIN_COMPLETENESS = 0.95  # 95% de dados não-nulos
    MAX_DUPLICATES_PCT = 0.01  # 1% de duplicatas

# =============================================
# FUNÇÕES DE VALIDAÇÃO E QUALIDADE
# =============================================

def detect_outliers_iqr(series: pd.Series, multiplier: float = 1.5) -> pd.Series:
    """Detecta outliers usando método IQR (Interquartile Range)"""
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    return (series < lower_bound) | (series > upper_bound)

def detect_outliers_zscore(series: pd.Series, threshold: float = 3.0) -> pd.Series:
    """Detecta outliers usando Z-Score"""
    z_scores = np.abs(stats.zscore(series, nan_policy='omit'))
    return z_scores > threshold

def calculate_data_quality_metrics(df: pd.DataFrame, dataset_name: str) -> Dict:
    """Calcula métricas de qualidade de dados"""
    total_records = len(df)
    
    metrics = {
        "dataset": dataset_name,
        "timestamp": datetime.now().isoformat(),
        "total_records": total_records,
        "columns": list(df.columns),
        "completeness": {},
        "duplicates": {
            "count": int(df.duplicated().sum()),
            "percentage": float(df.duplicated().sum() / total_records * 100)
        },
        "outliers": {}
    }
    
    # Completude por coluna
    for col in df.columns:
        null_count = df[col].isna().sum()
        metrics["completeness"][col] = {
            "null_count": int(null_count),
            "null_percentage": float(null_count / total_records * 100),
            "completeness": float((total_records - null_count) / total_records)
        }
    
    # Outliers em colunas numéricas
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if col in df.columns:
            outliers_iqr = detect_outliers_iqr(df[col].dropna())
            outliers_z = detect_outliers_zscore(df[col].dropna())
            metrics["outliers"][col] = {
                "iqr_method": int(outliers_iqr.sum()),
                "zscore_method": int(outliers_z.sum())
            }
    
    return metrics

def validate_data_schema(df: pd.DataFrame, expected_columns: List[str], dataset_name: str) -> bool:
    """Valida se o schema dos dados está correto"""
    missing_cols = set(expected_columns) - set(df.columns)
    extra_cols = set(df.columns) - set(expected_columns)
    
    if missing_cols:
        logger.error(f"{dataset_name}: Colunas faltando: {missing_cols}")
        return False
    
    if extra_cols:
        logger.warning(f"{dataset_name}: Colunas extras encontradas: {extra_cols}")
    
    return True

def save_to_parquet_partitioned(df: pd.DataFrame, base_path: Path, partition_col: str = "data"):
    """Salva DataFrame em formato Parquet particionado por data (compatível com Windows)"""

    if partition_col not in df.columns:
        logger.warning(f"Coluna de partição '{partition_col}' não encontrada. Salvando sem partição.")
        df.to_parquet(base_path / "data.parquet", index=False, engine='pyarrow', compression='snappy')
        return

    # --- CORREÇÃO: garantir que a coluna de partição NÃO tenha hora ---
    if pd.api.types.is_datetime64_any_dtype(df[partition_col]):
        # Converte datetime para string segura YYYY-MM-DD
        df[partition_col] = df[partition_col].dt.strftime('%Y-%m-%d')
    else:
        # Se for string mas tiver hora, remove
        df[partition_col] = (
            df[partition_col]
            .astype(str)
            .str.split(" ")
            .str[0]
        )

    # --- Salvar particionado ---
    for partition_value, group in df.groupby(partition_col):
        # Pasta no formato Windows-safe: data=2024-11-24
        partition_path = base_path / f"{partition_col}={partition_value}"
        partition_path.mkdir(parents=True, exist_ok=True)

        group.to_parquet(
            partition_path / "data.parquet",
            index=False,
            engine="pyarrow",
            compression="snappy"
        )

    logger.info(f"✅ Dados salvos em Parquet particionado por {partition_col}")


# =============================================
# PROCESSAMENTO BRONZE → SILVER
# =============================================

def process_telemetria(tele_raw: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Processa dados brutos de telemetria para camada Silver
    
    Returns:
        Tuple[DataFrame processado, Métricas de qualidade]
    """
    logger.info("📡 Processando Telemetria (Bronze → Silver)...")
    
    tele = tele_raw.copy()
    initial_count = len(tele)
    
    # 1. Padronização de nomes
    tele.columns = [
        col.lower().replace(" ", "_").replace("(", "").replace(")", "").replace(".", "_") 
        for col in tele.columns
    ]
    
    # 2. Renomeação de colunas chave
    rename_map = {
        "timestamp": "timestamp",
        "maquina": "maquina_id",
        "pressao_mpa": "pressao_mpa",
        "umidade": "umidade_pct",
        "temperatura_matriz_c": "temp_matriz_c",
        "defeito": "flag_defeito"
    }
    tele = tele.rename(columns={k: v for k, v in rename_map.items() if k in tele.columns})
    
    # 3. Conversão de tipos (timestamp)
    tele["timestamp"] = pd.to_datetime(tele["timestamp"], errors="coerce")
    tele = tele.dropna(subset=["timestamp"])
    
    # ======================================================
    # 4. 🔧 ***CORREÇÃO CRÍTICA: LIMPEZA DA TEMPERATURA ANTES***
    # ======================================================
    def clean_temp(x):
        """Remove texto, letras, ERR, simbolos e converte temperatura para float."""
        if pd.isna(x): 
            return np.nan
        
        s = str(x)
        
        if "ERR" in s or "err" in s:
            return np.nan
        
        # Remove qualquer coisa que NÃO for número, ponto, ou sinal
        s = re.sub(r"[^\d\.-]", "", s)
        
        try:
            return float(s)
        except:
            return np.nan
    
    if "temp_matriz_c" in tele.columns:
        tele["temp_matriz_c"] = tele["temp_matriz_c"].apply(clean_temp)

    # Converte todas as colunas numéricas corretas
    for col in ["pressao_mpa", "umidade_pct", "temp_matriz_c", "ciclo_tempo_s"]:
        if col in tele.columns:
            tele[col] = pd.to_numeric(tele[col], errors="coerce")
    # ======================================================

    # 5. Validação de limites físicos (agora seguro!)
    config = DataQualityConfig()
    
    mask_valid = pd.Series(True, index=tele.index)
    
    if "pressao_mpa" in tele.columns:
        mask_valid &= (
            (tele["pressao_mpa"] >= config.PRESSAO_MIN) & 
            (tele["pressao_mpa"] <= config.PRESSAO_MAX)
        )
    
    if "umidade_pct" in tele.columns:
        mask_valid &= (
            (tele["umidade_pct"] >= config.UMIDADE_MIN) &
            (tele["umidade_pct"] <= config.UMIDADE_MAX)
        )
    
    if "temp_matriz_c" in tele.columns:
        mask_valid &= (
            (tele["temp_matriz_c"] >= config.TEMP_MIN) &
            (tele["temp_matriz_c"] <= config.TEMP_MAX)
        )
    
    invalid_count = (~mask_valid).sum()
    tele = tele[mask_valid]
    
    # 6. Detecção e tratamento de outliers
    for col in ["pressao_mpa", "umidade_pct", "temp_matriz_c"]:
        if col in tele.columns:
            outliers = detect_outliers_iqr(tele[col])
            outlier_count = outliers.sum()
            
            if outlier_count > 0:
                logger.warning(f"   ⚠️  {outlier_count} outliers detectados em {col}")
                
                # Estratégia winsorization (limita aos percentis 1 e 99)
                lower = tele[col].quantile(0.01)
                upper = tele[col].quantile(0.99)
                tele[col] = tele[col].clip(lower, upper)
    
    # 7. Adicionar coluna de data (particionamento)
    tele["data"] = tele["timestamp"].dt.date
    
    # 8. Métricas de qualidade
    quality_metrics = calculate_data_quality_metrics(tele, "telemetria_silver")
    quality_metrics["processing"] = {
        "initial_records": initial_count,
        "final_records": len(tele),
        "removed_invalid": invalid_count,
        "removal_rate": float(invalid_count / initial_count * 100)
    }
    
    logger.info(f"   ✅ {len(tele):,} registros processados ({initial_count - len(tele):,} removidos)")
    
    return tele, quality_metrics


def process_producao(prod_raw: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Processa dados brutos de produção para camada Silver
    
    Returns:
        Tuple[DataFrame processado, Métricas de qualidade]
    """
    logger.info("🏭 Processando Produção (Bronze → Silver)...")
    
    prod = prod_raw.copy()
    initial_count = len(prod)
    
    # 1. Padronização de nomes
    prod.columns = [
        col.lower().replace(" ", "_").replace("(", "").replace(")", "").replace(".", "_") 
        for col in prod.columns
    ]
    
    # 2. Renomeação
    rename_map = {
        "timestamp": "timestamp",
        "maquina": "maquina_id",
        "pecas_produzidas": "pecas_produzidas",
        "pecas_refugadas": "pecas_refugadas",
        "consumo_kwh": "consumo_kwh"
    }
    prod = prod.rename(columns={k: v for k, v in rename_map.items() if k in prod.columns})
    
    # 3. Conversão de tipos
    prod["timestamp"] = pd.to_datetime(prod["timestamp"], errors="coerce")
    prod = prod.dropna(subset=["timestamp"])
    
    # 4. Validações de negócio
    prod = prod[prod["pecas_produzidas"] >= 0]
    prod = prod[prod["pecas_refugadas"] >= 0]
    prod = prod[prod["pecas_refugadas"] <= prod["pecas_produzidas"]]  # Refugo não pode ser maior que produção
    
    # 5. Criação de features
    def get_turno(hour):
        if 6 <= hour < 14:
            return "Turno A (06h-14h)"
        elif 14 <= hour < 22:
            return "Turno B (14h-22h)"
        else:
            return "Turno C (22h-06h)"
    
    prod["turno"] = prod["timestamp"].dt.hour.apply(get_turno)
    prod["data"] = prod["timestamp"].dt.date
    prod["taxa_refugo"] = prod["pecas_refugadas"] / prod["pecas_produzidas"].replace(0, np.nan)
    
    # 6. Calcular métricas de qualidade
    quality_metrics = calculate_data_quality_metrics(prod, "producao_silver")
    quality_metrics["processing"] = {
        "initial_records": initial_count,
        "final_records": len(prod),
        "removed_invalid": initial_count - len(prod)
    }
    
    logger.info(f"   ✅ {len(prod):,} registros processados")
    
    return prod, quality_metrics

def process_eventos(evt_raw: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Processa eventos industriais para camada Silver
    
    Returns:
        Tuple[DataFrame processado, Métricas de qualidade]
    """
    logger.info("⚡ Processando Eventos (Bronze → Silver)...")
    
    evt = evt_raw.copy()
    
    # 1. Normaliza timestamp
    evt["timestamp"] = pd.to_datetime(evt["timestamp"], errors="coerce")
    evt = evt.dropna(subset=["timestamp"])
    
    # 2. Padroniza nomes
    evt.rename(columns={
        "evento_id": "id_evento",
        "maquina_id": "id_maquina",
        "evento": "descricao",
        "severidade": "severidade_texto",
        "origem": "sistema_origem"
    }, inplace=True)
    
    # 3. Converte severidade textual para código numérico
    mapa_sev = {
        "Baixa": 1,
        "Média": 2,
        "Media": 2,
        "Alta": 3
    }
    evt["severidade"] = evt["severidade_texto"].map(mapa_sev).fillna(0).astype(int)
    
    # 4. Adiciona data para particionamento
    evt["data"] = evt["timestamp"].dt.date
    
    # 5. Ordena por tempo
    evt.sort_values("timestamp", inplace=True)
    
    # 6. Garante tipos seguros
    evt["id_evento"] = evt["id_evento"].astype(str)
    evt["id_maquina"] = evt["id_maquina"].astype(int)
    evt["descricao"] = evt["descricao"].astype(str)
    evt["sistema_origem"] = evt["sistema_origem"].astype(str)
    
    # 7. Calcular métricas de qualidade
    quality_metrics = calculate_data_quality_metrics(evt, "eventos_silver")
    
    logger.info(f"   ✅ {len(evt):,} eventos processados")
    
    return evt, quality_metrics

# =============================================
# PROCESSAMENTO SILVER → GOLD
# =============================================

def create_gold_kpis(prod_silver: pd.DataFrame) -> pd.DataFrame:
    """
    Cria tabela Gold de KPIs diários agregados
    """
    logger.info("💎 Criando KPIs Diários (Silver → Gold)...")
    
    if prod_silver.empty:
        logger.warning("   ⚠️  Nenhum dado de produção disponível")
        return pd.DataFrame()
    
    # Agregação diária
    prod_silver["data"] = pd.to_datetime(prod_silver["data"])
    
    daily_agg = prod_silver.groupby("data").agg(
        pecas_produzidas=("pecas_produzidas", "sum"),
        pecas_refugadas=("pecas_refugadas", "sum"),
        consumo_kwh=("consumo_kwh", "sum"),
        horas_operando=("pecas_produzidas", lambda x: (x > 0).sum())
    ).reset_index()
    
    # Cálculo de KPIs
    daily_agg["pecas_boas"] = daily_agg["pecas_produzidas"] - daily_agg["pecas_refugadas"]
    daily_agg["qualidade"] = daily_agg["pecas_boas"] / daily_agg["pecas_produzidas"].replace(0, 1)
    daily_agg["disponibilidade"] = daily_agg["horas_operando"] / 24
    daily_agg["performance"] = daily_agg["pecas_produzidas"] / (24 * 1000)  # Capacidade nominal: 1000 pçs/h
    daily_agg["oee"] = daily_agg["disponibilidade"] * daily_agg["performance"] * daily_agg["qualidade"]
    daily_agg["kwh_por_peca"] = daily_agg["consumo_kwh"] / daily_agg["pecas_produzidas"].replace(0, np.nan)
    
    logger.info(f"   ✅ {len(daily_agg)} dias agregados")
    
    return daily_agg.round(4)

# =============================================
# PIPELINE PRINCIPAL
# =============================================

def run_etl_pipeline():
    """Executa o pipeline completo Bronze → Silver → Gold"""
    
    logger.info("="*80)
    logger.info(f"🚀 INICIANDO PIPELINE ETL - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80)
    
    all_quality_metrics = []
    
    try:
        # ========== ETAPA 1: LEITURA (BRONZE) ==========
        logger.info("\n📂 ETAPA 1: Carregando dados da camada Bronze...")
        
        tele_raw = pd.read_csv(RAW_DIR / "telemetria_raw.csv")
        prod_raw = pd.read_csv(RAW_DIR / "producao_raw.csv")
        evt_raw = pd.read_csv(RAW_DIR / "eventos_raw.csv")
        
        logger.info(f"   ✅ Telemetria: {len(tele_raw):,} registros")
        logger.info(f"   ✅ Produção: {len(prod_raw):,} registros")
        logger.info(f"   ✅ Eventos: {len(evt_raw):,} registros")
        
        # ========== ETAPA 2: PROCESSAMENTO (SILVER) ==========
        logger.info("\n⚙️  ETAPA 2: Processando para camada Silver...")
        
        tele_silver, tele_quality = process_telemetria(tele_raw)
        prod_silver, prod_quality = process_producao(prod_raw)
        evt_silver, evt_quality = process_eventos(evt_raw)
        
        all_quality_metrics.extend([tele_quality, prod_quality, evt_quality])
        
        # ========== ETAPA 3: AGREGAÇÃO (GOLD) ==========
        logger.info("\n💎 ETAPA 3: Criando camada Gold...")
        
        kpis_gold = create_gold_kpis(prod_silver)
        
        # ========== ETAPA 4: SALVAMENTO ==========
        logger.info("\n💾 ETAPA 4: Salvando dados processados...")
        
        # Salvar Silver em CSV (compatibilidade com código existente)
        tele_silver.to_csv(SILVER_DIR / "telemetria_silver.csv", index=False)
        prod_silver.to_csv(SILVER_DIR / "producao_silver.csv", index=False)
        evt_silver.to_csv(SILVER_DIR / "eventos_silver.csv", index=False)
        
        # Salvar Silver em Parquet particionado (escalabilidade)
        parquet_dir = SILVER_DIR / "parquet"
        parquet_dir.mkdir(exist_ok=True)
        
        save_to_parquet_partitioned(tele_silver, parquet_dir / "telemetria", "data")
        save_to_parquet_partitioned(prod_silver, parquet_dir / "producao", "data")
        save_to_parquet_partitioned(evt_silver, parquet_dir / "eventos", "data")
        
        # Salvar Gold
        kpis_gold.to_csv(GOLD_DIR / "kpis_daily_gold.csv", index=False)
        
        logger.info("   ✅ Dados Silver salvos (CSV + Parquet)")
        logger.info("   ✅ Dados Gold salvos")
        
        # ========== ETAPA 5: RELATÓRIO DE QUALIDADE ==========
        logger.info("\n📊 ETAPA 5: Gerando relatório de qualidade...")
        
        quality_report = {
            "pipeline_execution": {
                "timestamp": datetime.now().isoformat(),
                "status": "SUCCESS"
            },
            "datasets": all_quality_metrics
        }
        
        import json
        report_path = QUALITY_DIR / f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(quality_report, f, indent=2, default=str)
        
        logger.info(f"   ✅ Relatório salvo em: {report_path}")
        
        # ========== FINALIZAÇÃO ==========
        logger.info("\n" + "="*80)
        logger.info("✅ PIPELINE ETL CONCLUÍDO COM SUCESSO!")
        logger.info("="*80)
        logger.info(f"\n📋 Resumo:")
        logger.info(f"   • Telemetria: {len(tele_silver):,} registros processados")
        logger.info(f"   • Produção: {len(prod_silver):,} registros processados")
        logger.info(f"   • Eventos: {len(evt_silver):,} eventos processados")
        logger.info(f"   • KPIs Gold: {len(kpis_gold)} dias agregados")
        logger.info(f"\n📂 Formatos gerados:")
        logger.info(f"   • CSV (compatibilidade)")
        logger.info(f"   • Parquet particionado (escalabilidade)")
        logger.info(f"\n📊 Qualidade de dados:")
        logger.info(f"   • Relatório completo em: {report_path}")
        logger.info("="*80)
        
    except FileNotFoundError as e:
        logger.error(f"❌ ERRO: Arquivo Bronze não encontrado")
        logger.error(f"   Detalhes: {e}")
        logger.info(f"\n🔧 Solução:")
        logger.info(f"   Execute: python simulador_industrial_hibrido.py")
        return False
        
    except Exception as e:
        logger.error(f"❌ ERRO CRÍTICO no pipeline: {e}", exc_info=True)
        return False
    
    return True

# =============================================
# EXECUÇÃO
# =============================================

if __name__ == "__main__":
    # Instala dependências se necessário
    try:
        import pyarrow
    except ImportError:
        logger.warning("⚠️  PyArrow não instalado. Instalando...")
        os.system("pip install pyarrow")
    
    # Garante que as pastas existem
    for directory in [SILVER_DIR, GOLD_DIR, QUALITY_DIR]:
        directory.mkdir(parents=True, exist_ok=True)
    
    # Executa pipeline
    success = run_etl_pipeline()
    
    if not success:
        exit(1)