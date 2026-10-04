# Arquitectura de datos para indicadores operativos TI (TFM)

Pipeline automatizado (Airflow + Spark) y dashboard (Power BI) sobre datos simulados de inventario, tickets y usuarios, evaluado frente a un escenario de referencia no automatizado (Excel) y validado con usuarios de TI (SUS).

Autor: Edwin Xavier Jaramillo Sandoval · Máster Universitario en Análisis y Visualización de Datos Masivos (UNIR).

## Requisitos
- Docker Desktop (con al menos 4 GB de RAM asignados).
- Power BI Desktop (solo para el dashboard).

## Puesta en marcha
```bash
docker compose up -d --build
```
Interfaz de Airflow: http://localhost:8080 (por defecto `airflow` / `airflow`; se puede cambiar copiando `.env.example` a `.env`).

Comprobación del entorno (Spark dentro de Airflow):
```bash
docker compose exec airflow-scheduler airflow dags test prueba_entorno
```

## Estructura
- `src/generador/` generación de datos simulados · `src/pipeline/` jobs de Spark · `dags/` DAGs de Airflow · `docker/` imagen
- `data/` capas raw · curated · quarantine · analytics
- `dashboard/` Power BI · `referencia/` escenario B · `resultados/` métricas y capturas · `docs/` diseño y plan · `tests/`

Documentación de diseño: `CONTEXTO.md`, `docs/plan.md`, `docs/esquema_datos.md`, `docs/indicadores.md`, `docs/validacion_sus.md`.
