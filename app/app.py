import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import os
from textwrap import dedent
from domain.kpis import (
    aggregate_by_period, compute_kpis, check_alerts, 
    compute_refugo_by_turno, pareto_paradas, 
    build_pressure_humidity_scatter, aggregate_events,
    compute_oee_kpis, compute_energy_cost
)
from ml.predictor import predict_defeito_prob

# ---------------------------
# 0. CONFIGURAÇÕES GLOBAIS
# ---------------------------
st.set_page_config(layout="wide", page_title="EcoData Monitor - Painel Profissional", page_icon="🏭")
PRECO_VENDA = 1.20       # R$ por tijolo (ajustável)
CUSTO_POR_TIJOLO = 0.45  # estimativa (opcional)

# Estilo simples para métricas
st.markdown("""
<style>
    div[data-testid="stMetric"] { background-color: #F7F9FB; border-radius: 8px; padding: 10px; }
    section[data-testid="stSidebar"] { background-color: #f6f8fb; }
    .big-title { font-size: 22px; font-weight:700; }
</style>
""", unsafe_allow_html=True)

# ---------------------------
# 1. FUNÇÃO DE CARREGAMENTO (data/processed a partir do _file_)
# ---------------------------
@st.cache_data(ttl=600)
def load_data():
    """
    Carrega os arquivos telemetria_silver.csv, producao_silver.csv, eventos_silver.csv
    dentro da pasta data/processed (relativa ao local do app.py).
    """
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(base_dir, "data", "processed")

        tele_path = os.path.join(data_dir, "telemetria_silver.csv")
        prod_path = os.path.join(data_dir, "producao_silver.csv")
        evt_path  = os.path.join(data_dir, "eventos_silver.csv")

        missing = [p for p in [tele_path, prod_path, evt_path] if not os.path.exists(p)]
        if missing:
            raise FileNotFoundError(
                f"Arquivos faltando: {', '.join([os.path.basename(m) for m in missing])}. Coloque-os em: {data_dir}"
            )

        tele = pd.read_csv(tele_path)
        prod = pd.read_csv(prod_path)
        evt  = pd.read_csv(evt_path)

        # parse timestamp quando existir
        for df in (tele, prod, evt):
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        return tele, prod, evt

    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return None, None, None

# ---------------------------
# 2. CARREGAR DADOS
# ---------------------------
with st.spinner("Carregando dados..."):
    tele_df, prod_df, evt_df = load_data()

if tele_df is None or prod_df is None or evt_df is None:
    st.stop()

# ---------------------------
# 3. SIDEBAR / NAVEGAÇÃO (MOVIDO PARA CIMA PARA DEFINIR VARIAVEIS)
# ---------------------------
st.sidebar.title("📌 Navegação")
pagina = st.sidebar.radio("", ["Resumo (Lucro)", "Onde Está Meu Lucro", "Qualidade", "Manutenção", "Telemetria (Mapa)", "Simulador de Qualidade", "Eventos"])

st.sidebar.markdown("---")
st.sidebar.subheader("⚙️ Período de Análise")
modo_periodo = st.sidebar.radio(
    "Escolha o período dos KPIs:",
    ["Automático (Inteligente)", "Ontem (Fechamento)", "Últimas 24 Horas"],
    index=0,  # Automático como padrão
    help="""
    • **Automático**: Escolhe o melhor período com produção
    • **Ontem**: Sempre mostra o dia anterior completo  
    • **Últimas 24h**: Janela móvel de 24 horas
    """
)

# Converte para código interno
modo_map = {
    "Automático (Inteligente)": "auto",
    "Ontem (Fechamento)": "ontem",
    "Últimas 24 Horas": "24h"
}
modo_codigo = modo_map[modo_periodo]

st.sidebar.markdown("---")
st.sidebar.write("EcoData Monitor — SENAI")
st.sidebar.caption(f"Preço venda: R$ {PRECO_VENDA:.2f} | Custo estimado: R$ {CUSTO_POR_TIJOLO:.2f}")

# ---------------------------
# 4. PROCESSAMENTO E CÁLCULOS (AGORA COM MODO_CODIGO DEFINIDO)
# ---------------------------

# pré-process
tele_agg = aggregate_by_period(tele_df)

# CÁLCULOS PRINCIPAIS (Agora modo_codigo existe!)
pecas, refugo, defeitos, temp, periodo_desc = compute_kpis(prod_df, tele_df, modo_codigo)
refugo_turno = compute_refugo_by_turno(prod_df)
pareto = pareto_paradas(evt_df)
scatter_df = build_pressure_humidity_scatter(tele_df)
evt_criticos = aggregate_events(evt_df)
alerts = check_alerts(tele_agg)

# Novos KPIs
DISP, PERF, QUAL, OEE = compute_oee_kpis(prod_df, tele_df, modo_periodo=modo_codigo)
custo_energetico_peca, custo_total_energia = compute_energy_cost(prod_df, PRECO_VENDA, CUSTO_POR_TIJOLO)


# ---------------------------
# 5. PÁGINAS
# ---------------------------

# ---------- RESUMO (LUCRO) ----------
if pagina == "Resumo (Lucro)":
    st.title("🏭 Visão Geral — Resumo Rápido")
    st.subheader(f"KPIs de Operação – {periodo_desc}")
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Peças Produzidas", f"{pecas:,}".replace(",", "."))
    c2.metric("Refugo Total (un)", f"{refugo:,}".replace(",", "."))
    c3.metric("Defeitos Detectados (telemetria)", f"{defeitos:,}")
    c4.metric("Temp. Média Matriz", f"{temp:.1f} °C" if not pd.isna(temp) else "N/D")

    st.markdown("---")
    st.subheader("Eficiência Global (OEE)")
    
    o1, o2, o3, o4 = st.columns(4)
    o1.metric("Disponibilidade (D)", f"{DISP*100:.1f}%", help="Tempo Operando / Tempo Programado")
    o2.metric("Performance (P)", f"{PERF*100:.1f}%", help="Produção Real / Capacidade Nominal")
    o3.metric("Qualidade (Q)", f"{QUAL*100:.1f}%", help="Peças Boas / Total Produzido")
    o4.metric("OEE Global (D x P x Q)", f"{OEE*100:.1f}%", help="Eficiência Total da Fábrica")

    st.markdown("---")
    st.subheader("Custo Operacional")
    c_e1, c_e2 = st.columns(2)
    c_e1.metric("Custo Energético por Peça", f"R$ {custo_energetico_peca:.3f}")
    c_e2.metric("Custo Total de Energia (Período)", f"R$ {custo_total_energia:,.2f}".replace(",", "."))

    st.markdown("---")
    st.subheader("Tendência de Produção (acumulado)")
    if prod_df is not None and not prod_df.empty and "pecas_produzidas" in prod_df.columns:
        df_acc = prod_df.sort_values("timestamp").copy()
        # Acumula a produção hora a hora para cada máquina
        df_acc["acumulado"] = df_acc.groupby("maquina_id")["pecas_produzidas"].cumsum()
        
        fig = px.line(df_acc, x="timestamp", y="acumulado", color="maquina_id", 
                      title="Produção Acumulada por Máquina (Histórico Real)",
                      labels={"timestamp": "Data/Hora", "acumulado": "Peças Produzidas", "maquina_id": "Máquina"})
        fig.update_layout(template="plotly_white", hovermode="x unified", yaxis_title="Peças acumuladas")
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown(dedent("""
            *O que isso significa (direto):* - Mostra a corrida de produção entre as máquinas ao longo do tempo.  
            - A diferença entre as linhas mostra claramente qual máquina é mais eficiente.
        """))
        with st.expander("Detalhes técnicos (engenharia)"):
            st.write(dedent("""
                - Fonte de dados: Histórico de Produção (producao_silver.csv).  
                - Agregação: Horária.  
                - Diferença de inclinação = Diferença de OEE.
            """))
    else:
        st.info("Dados de produção insuficientes para a linha do tempo.")


# ---------- ONDE ESTÁ MEU LUCRO ----------
elif pagina == "Onde Está Meu Lucro":
    st.title("💰 Onde Está Meu Lucro?")
    st.write("Comparação entre faturamento real e potencial máximo (hora a hora).")

    tijolos_bons = int(prod_df["pecas_produzidas"].sum()) if "pecas_produzidas" in prod_df.columns else 0
    tijolos_refugados = int(prod_df["pecas_refugadas"].sum()) if "pecas_refugadas" in prod_df.columns else 0
    faturamento_real = tijolos_bons * PRECO_VENDA
    dinheiro_refugo = tijolos_refugados * PRECO_VENDA

    if prod_df is not None and "timestamp" in prod_df.columns and "pecas_produzidas" in prod_df.columns:
        prod_df["hour"] = prod_df["timestamp"].dt.floor("H")
        hourly = prod_df.groupby("hour")["pecas_produzidas"].sum().reset_index()
        potencial_max_hora = int(hourly["pecas_produzidas"].max()) if not hourly.empty else 0
        horas_totais = prod_df["hour"].nunique()
        lucro_potencial = potencial_max_hora * horas_totais * PRECO_VENDA
        dinheiro_evaporado = lucro_potencial - faturamento_real
    else:
        potencial_max_hora = 0
        horas_totais = 0
        lucro_potencial = 0.0
        dinheiro_evaporado = 0.0

    k1, k2, k3 = st.columns(3)
    k1.metric("📦 Faturamento Real", f"R$ {faturamento_real:,.2f}".replace(",", "."))
    k2.metric("🔥 Dinheiro Jogando Fora (Refugo)", f"R$ {dinheiro_refugo:,.2f}".replace(",", "."))
    k3.metric("💨 Dinheiro que Evaporou (Ineficiência)", f"R$ {dinheiro_evaporado:,.2f}".replace(",", "."))

    st.markdown("---")
    st.subheader("Análise do Potencial")
    st.write(dedent(f"""
        - Pico observado (melhor hora): *{potencial_max_hora} tijolos/hora*.  
        - Horas observadas no histórico: *{horas_totais} horas*.  
        - Faturamento potencial (se tivesse produzido no pico): *R$ {lucro_potencial:,.2f}*.  
        - Diferença (dinheiro evaporado): *R$ {dinheiro_evaporado:,.2f}*.
    """))

    with st.expander("Como interpretar (direto para o Sr. Roberto)"):
        st.write(dedent("""
            - 'Dinheiro evaporado' = perda por paradas e operação abaixo do pico.  
            - Priorize reduzir paradas/baixo ritmo nas horas críticas para recuperar faturamento.
        """))

    st.markdown("---")
    st.subheader("🧮 Calculadora Reversa de Custo (opcional)")
    st.write("Transforme custos mensais em custo unitário e margem.")
    gastos_totais = st.number_input("Quanto gastou no mês (R$)?", min_value=0.0, value=50000.0, step=100.0, format="%.2f")
    if gastos_totais > 0 and tijolos_bons > 0:
        custo_unitario = gastos_totais / tijolos_bons
        margem_unitaria = PRECO_VENDA - custo_unitario
        margem_pct = (margem_unitaria / PRECO_VENDA) * 100
        st.markdown(dedent(f"""
            *Resultado:* - Custo por tijolo: *R$ {custo_unitario:.2f}* - Margem por tijolo: *R$ {margem_unitaria:.2f}* - Margem percentual: *{margem_pct:.1f}%*
        """))
        st.info("Margem saudável: 40%–60%. Abaixo de 30% é sinal de alerta.")
    elif gastos_totais > 0 and tijolos_bons == 0:
        st.warning("Não há produção válida na base para calcular custo unitário.")


# ---------- QUALIDADE ----------
elif pagina == "Qualidade":
    st.title("Qualidade — Onde estamos perdendo material?")
    # Refugo por turno
    if not refugo_turno.empty:
        fig_rt = px.bar(refugo_turno, x="turno", y="pct_refugo", text="pct_refugo", title="Taxa de Refugo por Turno (%)")
        fig_rt.update_layout(template="plotly_white", yaxis_title="% Refugo")
        st.plotly_chart(fig_rt, use_container_width=True)
        st.markdown(dedent("""
            *Explicação direta:* - Mostra qual turno gera mais desperdício (%).  
            - Ação: conversar com o supervisor do turno no topo do gráfico.
        """))
        with st.expander("Detalhes técnicos (engenharia)"):
            st.write(dedent("""
                - % Refugo = (refugos / peças_produzidas) * 100 por turno.
                - Agregação por turno com soma de peças dentro do período disponível.
            """))
    else:
        st.info("Dados de 'turno' ausentes — não é possível calcular refugo por turno.")

    st.markdown("---")
    st.subheader("Linha do tempo da produção (todas as máquinas)")
    if not tele_agg.empty:
        df_total_period = tele_agg.groupby("period")["pecas_produzidas"].sum().reset_index()
        fig_tot = px.area(df_total_period, x="period", y="pecas_produzidas", title="Produção Total por Período (Todas as Máquinas)")
        fig_tot.update_layout(template="plotly_white", yaxis_title="Peças por período")
        st.plotly_chart(fig_tot, use_container_width=True)
        st.markdown(dedent("""
            *Explicação direta:* - Mostra quando a produção sobe e cai ao longo do dia.  
            - Se estamos abaixo da meta em horários críticos, ajustar pessoal/produção.
        """))
    else:
        st.info("Dados de telemetria insuficientes para a linha do tempo consolidada.")


# ---------- MANUTENÇÃO ----------
elif pagina == "Manutenção":
    st.title("🔧 Manutenção Inteligente — Onde focar")
    st.markdown("Análise automática das causas que mais param sua fábrica.")

    st.subheader("📊 Pareto de Paradas (Média/Alta Severidade)")
    pareto = pareto_paradas(evt_df)
    if pareto.empty:
        st.info("Nenhum evento de severidade média/alta encontrado.")
    else:
        fig_pareto = px.bar(pareto, x="motivo", y="count", title="Top causas de parada (Pareto simplificado)", text_auto=True)
        fig_pareto.update_layout(template="plotly_white", xaxis_title="Causa", yaxis_title="Ocorrências")
        st.plotly_chart(fig_pareto, use_container_width=True)

    st.markdown("---")
    st.subheader("🚨 Últimos Eventos Críticos (Alta severidade)")
    # filtro por codigo ou texto de severidade
    if "sev_codigo" in evt_df.columns:
        evt_crit = evt_df[pd.to_numeric(evt_df["sev_codigo"], errors="coerce").fillna(0) >= 3]
    elif "severidade" in evt_df.columns:
        evt_crit = evt_df[evt_df["severidade"].astype(str).str.lower().str.contains("alta")]
    else:
        evt_crit = pd.DataFrame()
    if evt_crit.empty:
        st.success("Nenhum evento crítico detectado.")
    else:
        st.data_editor(evt_crit.sort_values("timestamp", ascending=False).head(20), use_container_width=True, height=420)


# ---------- TELEMETRIA (MAPA) ----------
elif pagina == "Telemetria (Mapa)":
    st.title("Mapa do Tesouro Operacional — Pressão x Umidade")
    
    # Verifica se o dataframe não está vazio
    if not scatter_df.empty:
        
        # CORREÇÃO AQUI: troquei y="umidade" por y="umidade_pct"
        # Também adicionei verificação para garantir que as colunas existem
        col_y = "umidade" if "umidade" in scatter_df.columns else "umidade_pct"
        col_x = "pressao_mpa"
        
        fig_sc = px.scatter(scatter_df, x=col_x, y=col_y, color="status",
                            title="Pressão x Umidade — Verde = OK / Vermelho = Defeito",
                            labels={col_x: "Pressão (MPa)", col_y: "Umidade (%)"},
                            color_discrete_map={"OK": "green", "Defeito": "red"})
        
        fig_sc.update_layout(template="plotly_white")
        st.plotly_chart(fig_sc, use_container_width=True)
        
        st.markdown(dedent("""
            *Explicação direta:* - Pontos vermelhos mostram onde estamos perdendo dinheiro (peças defeituosas).  
            - Ação: se pressão/umidade saírem da "zona verde", intervir.
        """))
        
        with st.expander("Detalhes técnicos (engenharia)"):
            st.write(dedent("""
                - Recomenda-se amostragem física das peças nas zonas vermelhas para validar limites.
                - Gráfico gerado com base na telemetria histórica (amostra).
            """))
    else:
        st.info("Dados de pressão/umidade insuficientes para gerar o mapa operacional.")


# ---------- SIMULADOR DE QUALIDADE (ML) ----------
elif pagina == "Simulador de Qualidade":
    st.title("🧠 Simulador de Qualidade Preditiva")
    st.markdown("Use os parâmetros de telemetria para prever a probabilidade de um defeito ocorrer.")
    
    st.subheader("Ajuste os Parâmetros de Entrada")
    
    # Valores médios/meta para o Sr. Roberto
    PRESSAO_META = 15.0
    UMIDADE_META = 12.0
    TEMP_META = 60.0
    
    col_p, col_u, col_t = st.columns(3)
    
    pressao = col_p.slider("Pressão (MPa)", min_value=10.0, max_value=20.0, value=PRESSAO_META, step=0.1)
    umidade = col_u.slider("Umidade (%)", min_value=5.0, max_value=20.0, value=UMIDADE_META, step=0.1)
    temperatura = col_t.slider("Temperatura (°C)", min_value=50.0, max_value=70.0, value=TEMP_META, step=0.1)
    
    prob_defeito = predict_defeito_prob(pressao, umidade, temperatura)
    
    st.markdown("---")
    st.subheader("Relógio de Risco (Previsão do Modelo)")
    
    if prob_defeito is not None:
        prob_pct = prob_defeito * 100
        
        # Lógica de cores para o "Relógio de Risco"
        if prob_pct < 5:
            cor = "green"
            status = "Baixo Risco"
            emoji = "✅"
        elif prob_pct < 15:
            cor = "orange"
            status = "Risco Moderado"
            emoji = "⚠️"
        else:
            cor = "red"
            status = "Alto Risco"
            emoji = "🚨"
            
        st.markdown(f"""
        <div style="background-color: #F7F9FB; border-radius: 10px; padding: 20px; text-align: center; border: 3px solid {cor};">
            <p style="font-size: 18px; color: #555;">Probabilidade de Defeito:</p>
            <p style="font-size: 48px; font-weight: 900; color: {cor}; margin: 0;">{emoji} {prob_pct:.2f}%</p>
            <p style="font-size: 24px; font-weight: 700; color: {cor}; margin-top: 5px;">{status}</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(dedent(f"""
            *Explicação para o Sr. Roberto:* - O modelo de Inteligência Artificial (IA) prevê que, com **Pressão de {pressao:.1f} MPa**, **Umidade de {umidade:.1f}%** e **Temperatura de {temperatura:.1f} °C**, a chance de produzir uma peça defeituosa é de **{prob_pct:.2f}%**.
            - **Ação:** Mantenha os parâmetros na zona verde (abaixo de 5%) para garantir a qualidade.
        """))
        
    else:
        st.warning("O modelo de Machine Learning não pôde ser carregado. Verifique o arquivo `rf_defeito.joblib`.")

# ---------- EVENTOS ----------
elif pagina == "Eventos":
    st.title("Eventos — Histórico")
    st.subheader("Últimos eventos (completo)")
    if evt_df is not None and not evt_df.empty:
        df_evt_recent = evt_df.sort_values("timestamp", ascending=False).head(200) if "timestamp" in evt_df.columns else evt_df.head(200)
        st.data_editor(df_evt_recent, use_container_width=True, height=520)
    else:
        st.info("Sem registros de eventos.")

# ---------------------------
# RODAPÉ / NOTAS
# ---------------------------
st.markdown("---")
st.caption("Dicas rápidas: 1) Verifique supervisor do turno com maior % de refugo. 2) Priorize ordens de serviço pelas causas do Pareto. 3) Use o mapa de pressão x umidade para instruções visuais aos operadores.")