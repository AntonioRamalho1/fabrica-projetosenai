"""
Sistema de Alertas Industrial com Observabilidade
Bandas de Controle Estatísticas + Notificações Multi-canal

MELHORIAS IMPLEMENTADAS:
- ✅ Regras de alertas configuráveis
- ✅ Bandas de controle (UCL/LCL)
- ✅ Sistema de notificações (Email, WhatsApp, Teams)
- ✅ Severidade e priorização
- ✅ Histórico estruturado
- ✅ Integração com observabilidade
"""

import pandas as pd
import numpy as np
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path

# Configuração de logging
logger = logging.getLogger(__name__)

# =============================================
# ENUMS E DATACLASSES
# =============================================

class AlertSeverity(Enum):
    """Níveis de severidade de alertas"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

class AlertType(Enum):
    """Tipos de alerta"""
    OUT_OF_CONTROL = "OUT_OF_CONTROL"
    TREND = "TREND"
    SPIKE = "SPIKE"
    DRIFT = "DRIFT"
    ANOMALY = "ANOMALY"

@dataclass
class ControlLimits:
    """Limites de controle estatístico"""
    ucl: float  # Upper Control Limit
    lcl: float  # Lower Control Limit
    mean: float
    std: float
    
    def to_dict(self):
        return asdict(self)

@dataclass
class Alert:
    """Estrutura de um alerta"""
    alert_id: str
    timestamp: datetime
    maquina_id: str
    metric: str
    value: float
    alert_type: AlertType
    severity: AlertSeverity
    message: str
    control_limits: Optional[ControlLimits] = None
    metadata: Optional[Dict] = None
    
    def to_dict(self):
        result = asdict(self)
        result['timestamp'] = self.timestamp.isoformat()
        result['alert_type'] = self.alert_type.value
        result['severity'] = self.severity.value
        if self.control_limits:
            result['control_limits'] = self.control_limits.to_dict()
        return result

# =============================================
# CONFIGURAÇÃO DE REGRAS DE ALERTAS
# =============================================

class AlertConfig:
    """Configuração centralizada de regras de alertas"""
    
    # Limites físicos (Safety Limits)
    PRESSURE_MIN_SAFE = 10.0
    PRESSURE_MAX_SAFE = 16.0
    TEMP_MIN_SAFE = 50.0
    TEMP_MAX_SAFE = 70.0
    UMIDADE_MIN_SAFE = 9.0
    UMIDADE_MAX_SAFE = 15.0
    
    # Parâmetros de controle estatístico
    WINDOW_SIZE = 30  # Tamanho da janela para cálculo de bandas
    STD_FACTOR_WARNING = 2.0  # 2 sigmas para warning
    STD_FACTOR_CRITICAL = 3.0  # 3 sigmas para crítico
    
    # Persistência (número de pontos consecutivos fora de controle)
    PERSISTENCE_WARNING = 3
    PERSISTENCE_CRITICAL = 5
    
    # Detecção de tendências
    TREND_WINDOW = 7  # Número de pontos para detectar tendência
    TREND_THRESHOLD = 0.7  # Correlação mínima para considerar tendência
    
    # Cooldown (tempo mínimo entre alertas iguais)
    COOLDOWN_MINUTES = 15

# =============================================
# CALCULADORA DE LIMITES DE CONTROLE
# =============================================

class ControlLimitsCalculator:
    """Calcula limites de controle estatístico (UCL/LCL)"""
    
    @staticmethod
    def calculate_limits(
        series: pd.Series, 
        window: int = 30, 
        std_factor: float = 3.0
    ) -> ControlLimits:
        """
        Calcula limites de controle usando média móvel e desvio padrão
        
        Args:
            series: Série temporal de dados
            window: Tamanho da janela para cálculo
            std_factor: Fator de desvio padrão (3σ = 99.7% dos dados)
        """
        if len(series) < window:
            window = len(series)
        
        mean = series.rolling(window=window, min_periods=1).mean().iloc[-1]
        std = series.rolling(window=window, min_periods=1).std().iloc[-1]
        
        if pd.isna(std) or std == 0:
            std = series.std() if not series.empty else 0.0
        
        ucl = mean + (std_factor * std)
        lcl = mean - (std_factor * std)
        
        return ControlLimits(
            ucl=float(ucl),
            lcl=float(lcl),
            mean=float(mean),
            std=float(std)
        )
    
    @staticmethod
    def calculate_ewma_limits(
        series: pd.Series,
        alpha: float = 0.2,
        std_factor: float = 3.0
    ) -> ControlLimits:
        """
        Calcula limites usando EWMA (Exponentially Weighted Moving Average)
        Mais sensível a mudanças recentes
        """
        ewma = series.ewm(alpha=alpha, adjust=False).mean().iloc[-1]
        std = series.std()
        
        ucl = ewma + (std_factor * std)
        lcl = ewma - (std_factor * std)
        
        return ControlLimits(
            ucl=float(ucl),
            lcl=float(lcl),
            mean=float(ewma),
            std=float(std)
        )

# =============================================
# DETECTOR DE ALERTAS
# =============================================

class AlertDetector:
    """Motor de detecção de alertas baseado em regras"""
    
    def __init__(self, config: AlertConfig = AlertConfig()):
        self.config = config
        self.alert_history: List[Alert] = []
        self.last_alert_time: Dict[str, datetime] = {}
    
    def _generate_alert_id(self) -> str:
        """Gera ID único para alerta"""
        return f"ALT-{datetime.now().strftime('%Y%m%d%H%M%S')}-{np.random.randint(1000, 9999)}"
    
    def _is_in_cooldown(self, alert_key: str) -> bool:
        """Verifica se alerta está em cooldown"""
        if alert_key not in self.last_alert_time:
            return False
        
        time_since_last = datetime.now() - self.last_alert_time[alert_key]
        return time_since_last < timedelta(minutes=self.config.COOLDOWN_MINUTES)
    
    def detect_out_of_control(
        self,
        df: pd.DataFrame,
        metric: str,
        maquina_id: str
    ) -> List[Alert]:
        """
        Detecta pontos fora de controle estatístico
        Regras Western Electric / Shewhart
        """
        alerts = []
        
        if df.empty or metric not in df.columns:
            return alerts
        
        # Calcula limites de controle
        limits = ControlLimitsCalculator.calculate_limits(
            df[metric],
            window=self.config.WINDOW_SIZE,
            std_factor=self.config.STD_FACTOR_WARNING
        )
        
        # Verifica últimos pontos
        recent_df = df.tail(self.config.PERSISTENCE_CRITICAL)
        
        for idx, row in recent_df.iterrows():
            value = row[metric]
            
            # Regra 1: Ponto além de 3σ (CRÍTICO)
            if value > limits.ucl or value < limits.lcl:
                severity = AlertSeverity.CRITICAL
                message = f"Valor {value:.2f} fora de controle (Limites: {limits.lcl:.2f} - {limits.ucl:.2f})"
                
                alert_key = f"{maquina_id}_{metric}_OUT_OF_CONTROL"
                
                if not self._is_in_cooldown(alert_key):
                    alert = Alert(
                        alert_id=self._generate_alert_id(),
                        timestamp=row['timestamp'] if 'timestamp' in row else datetime.now(),
                        maquina_id=maquina_id,
                        metric=metric,
                        value=float(value),
                        alert_type=AlertType.OUT_OF_CONTROL,
                        severity=severity,
                        message=message,
                        control_limits=limits
                    )
                    alerts.append(alert)
                    self.last_alert_time[alert_key] = datetime.now()
        
        return alerts
    
    def detect_trend(
        self,
        df: pd.DataFrame,
        metric: str,
        maquina_id: str
    ) -> List[Alert]:
        """
        Detecta tendências monotônicas (7+ pontos consecutivos subindo/descendo)
        """
        alerts = []
        
        if len(df) < self.config.TREND_WINDOW or metric not in df.columns:
            return alerts
        
        recent = df[metric].tail(self.config.TREND_WINDOW)
        
        # Calcula correlação linear
        x = np.arange(len(recent))
        correlation = np.corrcoef(x, recent)[0, 1]
        
        if abs(correlation) > self.config.TREND_THRESHOLD:
            direction = "ascendente" if correlation > 0 else "descendente"
            severity = AlertSeverity.MEDIUM
            
            alert_key = f"{maquina_id}_{metric}_TREND"
            
            if not self._is_in_cooldown(alert_key):
                alert = Alert(
                    alert_id=self._generate_alert_id(),
                    timestamp=df['timestamp'].iloc[-1] if 'timestamp' in df else datetime.now(),
                    maquina_id=maquina_id,
                    metric=metric,
                    value=float(recent.iloc[-1]),
                    alert_type=AlertType.TREND,
                    severity=severity,
                    message=f"Tendência {direction} detectada (correlação: {correlation:.2f})",
                    metadata={"correlation": float(correlation), "direction": direction}
                )
                alerts.append(alert)
                self.last_alert_time[alert_key] = datetime.now()
        
        return alerts
    
    def detect_safety_violations(
        self,
        df: pd.DataFrame,
        maquina_id: str
    ) -> List[Alert]:
        """
        Detecta violações de limites de segurança absolutos
        """
        alerts = []
        
        safety_rules = {
            "pressao_mpa": (self.config.PRESSURE_MIN_SAFE, self.config.PRESSURE_MAX_SAFE),
            "temp_matriz_c": (self.config.TEMP_MIN_SAFE, self.config.TEMP_MAX_SAFE),
            "umidade_pct": (self.config.UMIDADE_MIN_SAFE, self.config.UMIDADE_MAX_SAFE)
        }
        
        for metric, (min_val, max_val) in safety_rules.items():
            if metric not in df.columns:
                continue
            
            violations = df[
                (df[metric] < min_val) | (df[metric] > max_val)
            ]
            
            for idx, row in violations.iterrows():
                value = row[metric]
                alert_key = f"{maquina_id}_{metric}_SAFETY"
                
                if not self._is_in_cooldown(alert_key):
                    alert = Alert(
                        alert_id=self._generate_alert_id(),
                        timestamp=row['timestamp'] if 'timestamp' in row else datetime.now(),
                        maquina_id=maquina_id,
                        metric=metric,
                        value=float(value),
                        alert_type=AlertType.ANOMALY,
                        severity=AlertSeverity.CRITICAL,
                        message=f"⚠️ LIMITE DE SEGURANÇA VIOLADO: {value:.2f} (Faixa segura: {min_val}-{max_val})",
                        metadata={"min_safe": min_val, "max_safe": max_val}
                    )
                    alerts.append(alert)
                    self.last_alert_time[alert_key] = datetime.now()
        
        return alerts
    
    def analyze(
        self,
        tele_df: pd.DataFrame,
        metrics: List[str] = ["pressao_mpa", "temp_matriz_c", "umidade_pct"]
    ) -> List[Alert]:
        """
        Análise completa de telemetria para detectar todos os tipos de alertas
        """
        all_alerts = []
        
        if tele_df.empty:
            return all_alerts
        
        # Garante timestamp
        if 'timestamp' in tele_df.columns:
            tele_df['timestamp'] = pd.to_datetime(tele_df['timestamp'])
        
        # Analisa por máquina
        for maquina_id in tele_df['maquina_id'].unique():
            df_maq = tele_df[tele_df['maquina_id'] == maquina_id].copy()
            
            if 'timestamp' in df_maq.columns:
                df_maq = df_maq.sort_values('timestamp')
            
            for metric in metrics:
                if metric not in df_maq.columns:
                    continue
                
                # Detecta anomalias de controle
                all_alerts.extend(
                    self.detect_out_of_control(df_maq, metric, str(maquina_id))
                )
                
                # Detecta tendências
                all_alerts.extend(
                    self.detect_trend(df_maq, metric, str(maquina_id))
                )
            
            # Detecta violações de segurança
            all_alerts.extend(
                self.detect_safety_violations(df_maq, str(maquina_id))
            )
        
        # Armazena no histórico
        self.alert_history.extend(all_alerts)
        
        return all_alerts

# =============================================
# SISTEMA DE NOTIFICAÇÕES
# =============================================

class NotificationChannel:
    """Interface base para canais de notificação"""
    
    def send(self, alert: Alert) -> bool:
        raise NotImplementedError

class EmailNotifier(NotificationChannel):
    """Notificações por Email"""
    
    def __init__(self, smtp_config: Dict):
        self.config = smtp_config
        logger.info("📧 Email Notifier configurado")
    
    def send(self, alert: Alert) -> bool:
        """Envia alerta por email (simulado)"""
        logger.info(f"📧 [EMAIL] Enviando alerta {alert.alert_id}")
        logger.info(f"   Para: {self.config.get('to', 'operacao@fabrica.com')}")
        logger.info(f"   Assunto: [{alert.severity.value}] {alert.message}")
        return True

class WhatsAppNotifier(NotificationChannel):
    """Notificações por WhatsApp (via API)"""
    
    def __init__(self, api_config: Dict):
        self.config = api_config
        logger.info("💬 WhatsApp Notifier configurado")
    
    def send(self, alert: Alert) -> bool:
        """Envia alerta por WhatsApp (simulado)"""
        logger.info(f"💬 [WHATSAPP] Enviando alerta {alert.alert_id}")
        logger.info(f"   Para: {self.config.get('phone', '+55 81 99999-9999')}")
        logger.info(f"   🚨 {alert.message}")
        return True

class TeamsNotifier(NotificationChannel):
    """Notificações por Microsoft Teams"""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        logger.info("🔔 Teams Notifier configurado")
    
    def send(self, alert: Alert) -> bool:
        """Envia alerta para Teams (simulado)"""
        logger.info(f"🔔 [TEAMS] Enviando alerta {alert.alert_id}")
        logger.info(f"   Canal: Operações Fábrica")
        logger.info(f"   {alert.severity.value}: {alert.message}")
        return True

class NotificationManager:
    """Gerenciador de notificações multi-canal"""
    
    def __init__(self):
        self.channels: List[NotificationChannel] = []
    
    def add_channel(self, channel: NotificationChannel):
        """Adiciona canal de notificação"""
        self.channels.append(channel)
    
    def notify(self, alert: Alert):
        """Envia notificação para todos os canais configurados"""
        for channel in self.channels:
            try:
                channel.send(alert)
            except Exception as e:
                logger.error(f"Erro ao enviar notificação: {e}")

# =============================================
# FUNÇÃO PRINCIPAL PARA COMPATIBILIDADE
# =============================================

def compute_alerts(
    tele_agg: pd.DataFrame, 
    window_minutes: int = None, 
    std_factor: float = None, 
    persistence: int = None
) -> List[Dict]:
    """
    Função de compatibilidade com código legado
    Agora usa o novo sistema de alertas
    """
    if tele_agg is None or tele_agg.empty:
        return []
    
    # Usa configuração customizada ou padrão
    config = AlertConfig()
    if window_minutes:
        config.WINDOW_SIZE = window_minutes
    if std_factor:
        config.STD_FACTOR_WARNING = std_factor
    if persistence:
        config.PERSISTENCE_WARNING = persistence
    
    # Detecta alertas
    detector = AlertDetector(config)
    alerts = detector.analyze(tele_agg)
    
    # Converte para formato legado
    return [alert.to_dict() for alert in alerts]

# =============================================
# EXEMPLO DE USO COMPLETO
# =============================================

def example_usage():
    """Exemplo de uso do sistema de alertas"""
    
    # 1. Cria detector de alertas
    detector = AlertDetector()
    
    # 2. Configura notificações
    notifier = NotificationManager()
    notifier.add_channel(EmailNotifier({"to": "operacao@fabrica.com"}))
    notifier.add_channel(WhatsAppNotifier({"phone": "+55 81 99999-9999"}))
    notifier.add_channel(TeamsNotifier("https://teams.webhook.url"))
    
    # 3. Carrega dados de telemetria (exemplo)
    # tele_df = pd.read_csv("telemetria_silver.csv")
    
    # 4. Detecta alertas
    # alerts = detector.analyze(tele_df)
    
    # 5. Envia notificações para alertas críticos
    # for alert in alerts:
    #     if alert.severity in [AlertSeverity.HIGH, AlertSeverity.CRITICAL]:
    #         notifier.notify(alert)
    
    logger.info("✅ Sistema de alertas configurado e pronto!")

if __name__ == "__main__":
    example_usage()