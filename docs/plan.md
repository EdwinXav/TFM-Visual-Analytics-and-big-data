# Plan de trabajo por iteraciones

Marca cada casilla al terminar y deja evidencia (test o ejecución).
Entregas: **E2 9/11/2026** (generador + pipeline Spark + DAG básico; caps. 3–4 completos, 5 avanzado) · **E3 7/12/2026** (todo terminado) · depósito hasta el 3/02/2027 (solo correcciones).

## Iteración 0 · Preparación (6–12 oct)
- [x] Estructura del repositorio, `requirements.txt`, `.gitignore`.
- [x] Catálogos y parámetros en `src/generador/config.yaml`.
- [x] `docs/esquema_datos.md` y `docs/indicadores.md`.
- [x] `docker-compose.yaml`: Airflow 3 (LocalExecutor) + PySpark en una imagen.
- [x] DAG de humo `prueba_entorno` ejecutado con éxito dentro de Docker (Spark 4.2.0, 2026-10-04).
- [x] Repositorio git propio (identidad de Edwin).
- [ ] Subido a GitHub (lo crea Edwin como único propietario).

## Iteración 1 · Datos simulados (13–26 oct) — objetivo 1
- [x] Generador reproducible (semilla) de usuarios, inventario y tickets con integridad referencial.
- [x] Inyección de las 5 familias de anomalías + `anomalias_inyectadas.csv`.
- [x] Tamaños configurables (10^3 a 10^6; 10^6 en ~9 s) y muestra 10^3 versionada en `data/muestra/`.
- [x] Pruebas (10): reproducibilidad, tamaños, recuento de anomalías, solo violaciones registradas, coherencia, patrones.
- [x] Resumen descriptivo para la memoria: `resultados/datos/resumen_n10000.md`.
- [ ] Escribir `referencia/procedimiento.md` (escenario B) **antes** de ejecutarlo.

## Iteración 2 · Pipeline en Spark (20 oct–9 nov) — objetivo 2
- [ ] Ingesta (CSV → DataFrames con esquema explícito).
- [ ] Validación: las 5 reglas; cuarentena; informe de calidad en `resultados/calidad`.
- [ ] Limpieza e integración → `data/curated` (Parquet).
- [ ] Modelo en estrella + KPI → `data/analytics` (un Parquet por tabla).
- [ ] Registro de métricas de ejecución (duración por tarea, registros).
- [ ] Pruebas con datos pequeños con resultados conocidos.

## Iteración 3 · Orquestación (27 oct–14 nov) — objetivo 2
- [ ] DAG del pipeline con sensor de llegada de ficheros, dependencias y reintentos.
- [ ] Programación periódica y registro de marcas de tiempo (llegada → capa analítica).
- [ ] Documentar cómo lanzar y qué logs revisar.

**E2 (~principios de noviembre):** capítulos 4 y 5 avanzados, resultados en esquema.

## Iteración 4 · Dashboard (10–21 nov) — objetivo 3
- [ ] Documento de diseño en `dashboard/` (modelo, medidas DAX, vistas).
- [ ] Páginas: resumen, servicio (MTTR/SLA), parque, relación, calidad del dato.
- [ ] `.pbix` y capturas en `resultados/capturas`.

## Iteración 5 · Evaluación (17–30 nov) — objetivo 4
- [ ] Escenario A: 5 repeticiones × 4 tamaños → `resultados/tiempos`.
- [ ] Escenario B (Excel): 3 repeticiones × 2 tamaños (10^3, 10^4) → `resultados/tiempos`.
- [ ] Prueba de usabilidad SUS con 5–8 compañeros de TI (`docs/validacion_sus.md`).
- [ ] Script de análisis y gráficos; discusión de validez.

## Cierre (1–7 dic)
- [ ] Capítulos 6, 7 y 8; revisión de la memoria completa → **E3: 7/12/2026**.
