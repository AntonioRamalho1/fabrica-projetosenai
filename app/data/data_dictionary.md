# 📚 Dicionário de Dados (Data Catalog)

## 🏭 Camada Bronze (Raw)
Dados brutos e imutáveis.
- `telemetria_detalhada_*.csv`: Logs de sensores IoT.
- `eventos_industriais.csv`: Logs de alarmes.

## 🥈 Camada Prata (Silver)
Dados limpos e enriquecidos.
- `telemetria_silver.csv`: Tipagem corrigida, nulos tratados (ffill).
- `eventos_silver.csv`: Padronização de texto e cálculo de `duracao_min` para MTTR.

## 🥇 Camada Ouro (Gold)
Dados agregados para BI.
- `kpis_diarios_gold.csv`: Visão consolidada de OEE por dia.