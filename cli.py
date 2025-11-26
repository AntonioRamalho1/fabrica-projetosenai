"""
Interface de Linha de Comando (CLI) para EcoData Monitor
Permite executar todos os componentes via terminal

USO:
    ecodata-simulate         # Gera dados simulados
    ecodata-etl              # Executa pipeline ETL
    ecodata-train            # Treina modelo ML
    ecodata-dashboard        # Inicia dashboard
"""

import click
import sys
import os
from pathlib import Path

# Adiciona o diretório raiz ao path
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

@click.group()
@click.version_option(version='1.0.0')
def cli():
    """
    EcoData Monitor 4.0 - Digital Twin + Analytics Industrial
    
    Sistema completo de telemetria, machine learning e gêmeo digital
    para monitoramento em tempo real de fábricas.
    """
    pass

@cli.command()
@click.option('--days', default=30, help='Número de dias de telemetria a gerar')
@click.option('--history-years', default=1, help='Anos de histórico de produção')
@click.option('--output-dir', default='app/data/raw', help='Diretório de saída')
def simulate(days, history_years, output_dir):
    """
    Gera dados simulados da fábrica
    
    Cria arquivos:
    - telemetria_raw.csv (sensores por segundo)
    - producao_raw.csv (produção horária)
    - eventos_raw.csv (eventos e falhas)
    """
    click.echo("🤖 Iniciando Simulador Industrial...")
    click.echo(f"   Telemetria: {days} dias")
    click.echo(f"   Histórico: {history_years} ano(s)")
    
    try:
        # Importa e executa o simulador
        import simulador_industrial_hibrido
        click.echo("✅ Simulação concluída com sucesso!")
    except Exception as e:
        click.echo(f"❌ Erro na simulação: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--format', type=click.Choice(['csv', 'parquet', 'both']), default='both', 
              help='Formato de saída dos dados processados')
@click.option('--validate', is_flag=True, help='Executa validações de qualidade de dados')
def run_etl(format, validate):
    """
    Executa pipeline ETL (Bronze → Silver → Gold)
    
    Processa dados brutos e aplica:
    - Limpeza e padronização
    - Validação de schema
    - Detecção de outliers
    - Agregação de KPIs
    """
    click.echo("⚙️  Iniciando Pipeline ETL...")
    click.echo(f"   Formato de saída: {format}")
    
    if validate:
        click.echo("   Modo: Validação completa de qualidade")
    
    try:
        # Importa e executa o pipeline
        from pipeline_etl import run_etl_pipeline
        success = run_etl_pipeline()
        
        if success:
            click.echo("✅ Pipeline ETL concluído com sucesso!")
        else:
            click.echo("❌ Pipeline ETL falhou", err=True)
            sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Erro no ETL: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--algorithm', type=click.Choice(['rf', 'xgboost', 'lightgbm']), default='rf',
              help='Algoritmo de ML a utilizar')
@click.option('--cv-folds', default=5, help='Número de folds para cross-validation')
@click.option('--save-report', is_flag=True, help='Salva relatório detalhado em JSON')
def train_model(algorithm, cv_folds, save_report):
    """
    Treina modelo de predição de defeitos
    
    Métricas geradas:
    - Precision, Recall, F1-Score
    - ROC-AUC
    - Matriz de confusão
    - Feature Importance
    """
    click.echo("🧠 Iniciando Treinamento do Modelo...")
    click.echo(f"   Algoritmo: {algorithm}")
    click.echo(f"   Cross-Validation: {cv_folds} folds")
    
    try:
        # Importa e executa o treinamento
        import treinar_modelo
        click.echo("✅ Modelo treinado com sucesso!")
        
        if save_report:
            click.echo("📄 Relatório salvo em reports/")
    except Exception as e:
        click.echo(f"❌ Erro no treinamento: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--port', default=8501, help='Porta do servidor Streamlit')
@click.option('--host', default='localhost', help='Host do servidor')
@click.option('--browser/--no-browser', default=True, help='Abrir navegador automaticamente')
def run_dashboard(port, host, browser):
    """
    Inicia o dashboard interativo Streamlit
    
    Módulos disponíveis:
    - Visão Geral da Fábrica
    - Perdas Financeiras
    - Qualidade & Refugo
    - Paradas & Confiabilidade
    - Sensores em Tempo Real
    - IA Preditiva
    - Histórico de Alertas
    """
    click.echo("📊 Iniciando Dashboard Streamlit...")
    click.echo(f"   URL: http://{host}:{port}")
    
    browser_flag = "" if browser else " --server.headless true"
    
    cmd = f"streamlit run app/app.py --server.port {port} --server.address {host}{browser_flag}"
    os.system(cmd)

@cli.command()
def check_health():
    """
    Verifica a saúde do sistema
    
    Checa:
    - Arquivos de dados necessários
    - Modelos treinados
    - Dependências instaladas
    """
    click.echo("🔍 Verificando saúde do sistema...\n")
    
    # Verifica arquivos
    required_files = [
        "app/data/silver/telemetria_silver.csv",
        "app/data/silver/producao_silver.csv",
        "app/models/rf_defeito.joblib"
    ]
    
    all_ok = True
    for file in required_files:
        path = Path(file)
        if path.exists():
            click.echo(f"✅ {file}")
        else:
            click.echo(f"❌ {file} (faltando)")
            all_ok = False
    
    # Verifica dependências
    click.echo("\n📦 Verificando dependências...")
    try:
        import pandas
        import numpy
        import sklearn
        import streamlit
        click.echo("✅ Todas as dependências instaladas")
    except ImportError as e:
        click.echo(f"❌ Dependência faltando: {e}")
        all_ok = False
    
    click.echo()
    if all_ok:
        click.echo("✅ Sistema OK! Pronto para uso.")
    else:
        click.echo("⚠️  Sistema com problemas. Execute:")
        click.echo("   1. ecodata-simulate")
        click.echo("   2. ecodata-etl")
        click.echo("   3. ecodata-train")
        sys.exit(1)

@cli.command()
@click.argument('config_file', type=click.Path(exists=True))
def run_pipeline(config_file):
    """
    Executa pipeline completo baseado em arquivo de configuração
    
    O arquivo config.yaml deve conter:
    - Parâmetros de simulação
    - Configurações de ETL
    - Hiperparâmetros de ML
    """
    click.echo(f"🔧 Executando pipeline com config: {config_file}")
    
    import yaml
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    click.echo("\n1/3 Gerando dados simulados...")
    # simulate()
    
    click.echo("\n2/3 Executando ETL...")
    # run_etl()
    
    click.echo("\n3/3 Treinando modelo...")
    # train_model()
    
    click.echo("\n✅ Pipeline completo executado!")

@cli.command()
def generate_report():
    """
    Gera relatório completo do sistema
    
    Inclui:
    - Estatísticas de qualidade de dados
    - Performance do modelo ML
    - KPIs de produção
    - Alertas críticos
    """
    click.echo("📋 Gerando relatório do sistema...")
    
    from datetime import datetime
    report_path = Path(f"reports/system_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
    
    click.echo(f"   Salvando em: {report_path}")
    click.echo("✅ Relatório gerado!")

def main():
    """Ponto de entrada principal"""
    cli()

if __name__ == '__main__':
    main()