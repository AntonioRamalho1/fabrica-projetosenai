import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import os

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    layout="wide", 
    page_title="EcoData Monitor - Versão Final",
    page_icon="🏭"
)

# CSS para visual profissional
st.markdown("""
<style>
    div[data-testid="stMetric"] {
        background-color: #F0F2F6;
        border: 1px solid #D6D6D6;
        padding: 15px;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. FUNÇÕES DE CARREGAMENTO (Caminho Corrigido: data/processed) ---

@st.cache_data(ttl=600)
def load_data():
    """Carrega os dados Silver da pasta data/processed."""
    try:
        # Pega o diretório onde o app.py está
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Caminho exato baseado no que você mandou: app/data/processed/
        data_dir = os.path.join(base_dir, 'data', 'processed')
        
        # Debug: Mostra onde ele está procurando (caso dê erro)
        print(f"Procurando arquivos em: {data_dir}")

        path_tele = os.path.join(data_dir, "telemetria_silver.csv")
        path_prod = os.path.join(data_dir, "producao_silver.csv")
        path_evt = os.path.join(data_dir, "eventos_silver.csv")

        # Verificação de segurança
        if not os.path.exists(path_tele):
            st.error(f"❌ Arquivo não encontrado: {path_tele}")
            return None, None, None

        tele = pd.read_csv(path_tele)
        prod = pd.read_csv(path_prod)
        evt = pd.read_csv(path_evt)
        
        for df in [tele, prod, evt]:
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                
        return tele, prod, evt

    except Exception as e:
        st.error(f"Erro ao carregar arquivos CSV: {e}")
        return None, None, None

def aggregate_by_period(df, freq="5T"):
    """Agrega telemetria por período."""
    if df is None or df.empty: return pd.DataFrame()
    df = df.copy()
    df["period"] = df["timestamp"].dt.floor(freq)
    agg = df.groupby(["maquina_id", "period"]).agg({
        "pecas_produzidas": "sum",
        "flag_defeito": "sum",
        "pressao_mpa": "mean",
        "temp_matriz_c": "mean"
    }).reset_index()
    return agg

def compute_kpis(prod_df, tele_df):
    """Calcula KPIs do dia."""
    if prod_df is None or prod_df.empty:
        return 0, 0, 0, 0
    
    last_date = prod_df["timestamp"].max().date()
    prod_today = prod_df[prod_df["timestamp"].dt.date == last_date]
    tele_today = tele_df[tele_df["timestamp"].dt.date == last_date]
    
    total_pecas = prod_today["pecas_produzidas"].sum()
    total_refugo = prod_today["pecas_refugadas"].sum()
    total_defeitos = tele_today["flag_defeito"].sum()
    avg_temp = tele_today["temp_matriz_c"].mean()
    
    return int(total_pecas), int(total_refugo), int(total_defeitos), avg_temp

def check_alerts(tele_agg):
    """Gera alertas simples."""
    alerts = []
    if tele_agg.empty: return alerts
    criticos = tele_agg[tele_agg['pressao_mpa'] < 12]
    for _, row in criticos.tail(5).iterrows():
        alerts.append(f"Máquina {row['maquina_id']}: Pressão Baixa ({row['pressao_mpa']:.1f} MPa) às {row['period'].strftime('%H:%M')}")
    return alerts

# --- 3. EXECUÇÃO DO APP ---

st.title("🏭 EcoData Monitor — Painel de Controle")
st.markdown("Sistema de Monitoramento de Produção de Tijolos Ecológicos")

with st.spinner("Carregando base de dados..."):
    tele_df, prod_df, evt_df = load_data()

if tele_df is not None:
    
    tele_agg = aggregate_by_period(tele_df)
    pecas, refugo, defeitos, temp = compute_kpis(prod_df, tele_df)
    
    tab1, tab2, tab3, tab4 = st.tabs(["Visão Geral", "Telemetria", "Produção", "Eventos"])
    
    with tab1:
        st.subheader("KPIs de Hoje")
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Peças Produzidas", f"{pecas:,}".replace(",", "."), "Meta: 5.000")
        k2.metric("Refugos", f"{refugo}", delta="-2 un", delta_color="inverse")
        k3.metric("Defeitos Detectados", f"{defeitos}", delta_color="inverse")
        k4.metric("Temp. Média", f"{temp:.1f} °C")
        
        st.divider()
        st.subheader("Tendência de Produção")
        st.plotly_chart(px.line(tele_agg, x="period", y="pecas_produzidas", color="maquina_id", markers=True), use_container_width=True)

    with tab2:
        st.subheader("Monitoramento de Sensores")
        maquina = st.selectbox("Selecione a Máquina:", sorted(tele_agg["maquina_id"].unique()))
        tele_m = tele_agg[tele_agg["maquina_id"] == maquina]
        
        c1, c2 = st.columns(2)
        with c1:
            fig_p = px.line(tele_m, x="period", y="pressao_mpa", title="Pressão (MPa)", color_discrete_sequence=["#1f77b4"])
            fig_p.add_hline(y=12, line_dash="dash", line_color="red")
            st.plotly_chart(fig_p, use_container_width=True)
        with c2:
            st.plotly_chart(px.line(tele_m, x="period", y="temp_matriz_c", title="Temperatura (°C)", color_discrete_sequence=["#ff7f0e"]), use_container_width=True)

    with tab3:
        st.subheader("Análise de Produção")
        if "turno" in prod_df.columns:
            st.plotly_chart(px.bar(prod_df.groupby("turno")["pecas_produzidas"].sum().reset_index(), x="turno", y="pecas_produzidas", title="Total por Turno", text_auto=True), use_container_width=True)
        st.dataframe(prod_df.tail(10), use_container_width=True)

    with tab4:
        st.subheader("Alertas do Sistema")
        alerts = check_alerts(tele_agg)
        if alerts:
            for a in alerts: st.warning(a)
        else:
            st.success("Sistema operando normalmente.")
        st.dataframe(evt_df.sort_values("timestamp", ascending=False).head(20), use_container_width=True)

else:
    st.warning("⚠️ Arquivos não encontrados na pasta 'app/data/processed'. Verifique se eles estão lá.")