# Definición de indicadores (KPI)

Definiciones únicas para los dos escenarios (A: pipeline; B: Excel). Se calculan sobre datos **depurados** (sin registros en cuarentena). Fecha de corte = fin del periodo simulado o del periodo filtrado.

| # | KPI | Definición / fórmula | Familia |
|---|---|---|---|
| K1 | Tickets abiertos (periodo) | nº de tickets con fecha_apertura en el periodo | Volumen |
| K2 | Tickets cerrados (periodo) | nº de tickets con fecha_cierre en el periodo | Volumen |
| K3 | Backlog | nº de tickets con fecha_apertura ≤ corte y (fecha_cierre vacía o > corte) | Volumen |
| K4 | MTTR (h) | media de (fecha_cierre − fecha_apertura) en horas, solo tickets con fecha_cierre | Servicio |
| K5 | % cumplimiento SLA | tickets cerrados con horas_resolucion ≤ sla_horas[prioridad] / tickets cerrados × 100 (horas naturales) | Servicio |
| K6 | Equipos por estado | recuento de equipos por estado | Parque |
| K7 | Antigüedad media (años) | media de (corte − fecha_adquisicion) / 365,25 de equipos no dados de baja | Parque |
| K8 | Garantías vencidas | nº de equipos no dados de baja con fecha_fin_garantia < corte | Parque |
| K9 | Tickets por modelo | nº de tickets / nº de equipos del modelo (tasa por equipo) | Relación |
| K10 | Tickets por departamento y ubicación | recuento cruzado por dim_usuario | Relación |

Filtros del dashboard: periodo, ubicación, departamento, categoría y prioridad.

Página adicional de **calidad del dato**: registros procesados, registros en cuarentena por regla y tasa de detección.
