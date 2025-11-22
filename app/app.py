import sys
from pathlib import Path

# --- CORREÇÃO DO PATH ---
# O arquivo está em PROJETOSENAI/app/app.py
# .parent refere-se a PROJETOSENAI/app/
# Adicionamos essa pasta ao sys.path para importar 'data', 'domain', etc. diretamente.
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import streamlit as st
import pandas as pd # Adicionado para garantir que manipulações de tempo funcionem caso precise

# --- IMPORTS CORRIGIDOS (Sem "app.") ---
from data.data_loader import load_silver_data
from domain.aggregates import aggregate_by_period, latest_status, compute_daily_kpis, compute_defect_rates
from domain.alerts import compute_alerts
from viz.plotting import plot_line, plot_bar
from viz.ui_components import kpi_row
from processing.data_processing import safe_to_plotly
from config import settings

st.set_page_config(layout="wide", page_title="EcoData Monitor - Simples")

st.title("EcoData Monitor — Versão Organizada")
st.markdown("Painel simples para controlar produção, temperatura e alertas.")

with st.spinner("Carregando dados..."):
    try:
        tele_df, prod_df, evt_df = load_silver_data()
    except FileNotFoundError as e:
        st.error(str(e))
        st.stop()
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        st.stop()

# Agregação (cache simples usando st.cache_data)
@st.cache_data(ttl=settings.CACHE_TTL_AGG)
def get_aggregations(tele_df):
    tele_agg = aggregate_by_period(tele_df, freq="1T")
    return tele_agg

tele_agg = get_aggregations(tele_df)

# KPIs
kpis = compute_daily_kpis(prod_df, tele_df)

# tabs
tabs = st.tabs(["Visão Geral","Telemetria por Máquina","Produção","Eventos & Alertas","Previsões (opcional)"])

with tabs[0]:
    st.header("Visão Geral")
    kpi_row(kpis["total_pecas"], kpis["total_refugo"], kpis["total_defeitos"], kpis["avg_temp"])

    st.markdown("**Tendência de produção (últimas 6 horas)**")
    if "timestamp" in tele_df.columns and "pecas_produzidas" in tele_df.columns:
        # Correção preventiva: Usar pd.Timestamp se st.Timestamp falhar, ou manter lógica original se funcionar no seu ambiente
        agora = st.session_state.get("now", None) or (pd.Timestamp.now() - pd.Timedelta(hours=6))
        
        tele_6h = tele_df[tele_df["timestamp"] >= agora]
        if tele_6h.empty:
            st.info("Sem dados das últimas 6 horas.")
        else:
            tele_6h = tele_6h.copy()
            tele_6h["period"] = tele_6h["timestamp"].dt.floor("5T")
            trend = tele_6h.groupby("period")["pecas_produzidas"].sum().reset_index()
            st.plotly_chart(plot_line(trend, x="period", y="pecas_produzidas", title="Peças por periodo (5min)"), use_container_width=True)

    st.markdown("**Resumo por máquina (última leitura)**")
    last = latest_status(tele_agg)
    if last.empty:
        st.info("Sem leituras agregadas.")
    else:
        display = last.reset_index()[["maquina_id","pecas_produzidas","flag_defeito","pressao_mpa","temp_matriz_c"]]
        st.dataframe(display)

with tabs[1]:
    st.header("Telemetria por Máquina")
    machines = sorted(tele_agg["maquina_id"].unique()) if "maquina_id" in tele_agg.columns else []
    if not machines:
        st.info("Sem dados agregados por máquina.")
    else:
        machine = st.selectbox("Máquina", machines, index=0)
        tele_m = tele_agg[tele_agg["maquina_id"]==machine].sort_values("period")
        if tele_m.empty:
            st.info("Sem dados para essa máquina.")
        else:
            tele_m_plot = safe_to_plotly(tele_m.copy())
            st.subheader("Pressão")
            st.plotly_chart(plot_line(tele_m_plot, x="period", y="pressao_mpa", title=f"Pressão (máquina {machine})"), use_container_width=True)
            st.subheader("Temperatura da matriz")
            st.plotly_chart(plot_line(tele_m_plot, x="period", y="temp_matriz_c", title=f"Temperatura (máquina {machine})"), use_container_width=True)
            st.subheader("Produção por período")
            st.plotly_chart(plot_bar(tele_m_plot, x="period", y="pecas_produzidas", title=f"Produção (máquina {machine})"), use_container_width=True)

with tabs[2]:
    st.header("Produção")
    if "turno" in prod_df.columns:
        prod_by_turno = prod_df.groupby("turno", dropna=True)["pecas_produzidas"].sum().reset_index()
        st.plotly_chart(plot_bar(prod_by_turno, x="turno", y="pecas_produzidas", title="Produção por turno"), use_container_width=True)
    if "maquina_id" in prod_df.columns:
        prod_by_machine = prod_df.groupby("maquina_id")["pecas_produzidas"].sum().reset_index()
        st.plotly_chart(plot_bar(prod_by_machine, x="maquina_id", y="pecas_produzidas", title="Produção total por máquina"), use_container_width=True)
    st.dataframe(prod_df.sort_values("timestamp", ascending=False).head(20))

with tabs[3]:
    st.header("Eventos & Alertas")
    if "severidade" in evt_df.columns:
        sev_choice = st.selectbox("Severidade", ["Todas","Alta","Média","Baixa"])
        evt_show = evt_df if sev_choice=="Todas" else evt_df[evt_df["severidade"].str.capitalize()==sev_choice]
    else:
        evt_show = evt_df
    st.dataframe(evt_show.sort_values("timestamp", ascending=False).head(50))

    st.markdown("Alertas automáticos (simples)")
    alerts = compute_alerts(tele_agg)
    if not alerts:
        st.success("Nenhum alerta crítico no momento.")
    else:
        for a in alerts:
            st.warning(f"Máquina {a['maquina_id']}: {a['metric']}={a['value']} — regra {a['rule']} (t={a['timestamp']})")

with tabs[4]:
    st.header("Previsões / Taxa de defeito")
    rates = compute_defect_rates(tele_agg)
    if not rates.empty:
        st.plotly_chart(plot_bar(rates, x="maquina_id", y="taxa_defeito", title="Taxa histórica de defeito"), use_container_width=True)
    else:
        st.info("Dados insuficientes para taxa de defeito.")