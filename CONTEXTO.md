# Contexto del proyecto

**Título:** Diseño, implementación y evaluación de una arquitectura de datos para indicadores operativos IT
**Autor:** Edwin Xavier Jaramillo Sandoval · **Tutor:** Hugo Alberto Xochicale Rojas · UNIR (MU en Análisis y Visualización de Datos Masivos)
**Tipo:** Tipo 2, desarrollo de software. Entrega final: diciembre de 2026.

## Objetivo general
Diseñar, implementar y evaluar una arquitectura de datos que permita integrar, procesar y visualizar de forma automatizada datos simulados de inventario, tickets de soporte y usuarios, mediante Apache Airflow, Apache Spark y Power BI, con el fin de facilitar el análisis de indicadores operativos en entornos IT.

## Objetivos específicos
1. Diseñar el esquema de datos y generar un conjunto de datos simulados con estructura realista.
2. Implementar un pipeline automatizado (integración, validación, procesamiento) con Airflow (orquestación) y Spark (procesamiento).
3. Desarrollar un dashboard interactivo en Power BI.
4. Evaluar la arquitectura (rendimiento, calidad del dato, disponibilidad de indicadores) frente a un escenario de referencia no automatizado, y validar la usabilidad del dashboard con profesionales de TI (cuestionario SUS).

## Alcance
- Datos simulados, procesamiento **batch** periódico, entorno local en contenedores.
- Fuera de alcance: tiempo real, datos reales, clúster productivo, seguridad corporativa, costes.

## Arquitectura
Fuentes simuladas (CSV) → **Airflow** (DAG) orquesta → **Spark**: 1) ingesta, 2) validación de calidad, 3) limpieza e integración, 4) cálculo de indicadores → **capa analítica** (Parquet, modelo dimensional sencillo) → **Power BI**.
Además el pipeline registra métricas de ejecución y un informe de calidad (fuente de datos de la evaluación).

Capas de datos (carpeta `data/`): `raw` (CSV generados) → `curated` (depurado, Parquet) + `quarantine` (registros rechazados con su motivo) → `analytics` (hechos y dimensiones, Parquet; un fichero por tabla para Power BI).

El DAG arranca con un sensor que detecta la llegada de los CSV a `raw/`; esa marca de tiempo es el inicio de la medida de disponibilidad de indicadores.

Modelo analítico: `fact_tickets`; dimensiones `dim_equipo`, `dim_usuario`, `dim_fecha`.

## Esquema de datos simulados
| Entidad | Atributos |
|---|---|
| inventario | equipo_id, tipo, marca, modelo, fecha_adquisicion, fecha_fin_garantia, estado, ubicacion, usuario_id |
| tickets | ticket_id, fecha_apertura, fecha_cierre, categoria, prioridad, estado, equipo_id, usuario_id, tecnico |
| usuarios | usuario_id, nombre, departamento, ubicacion, rol |

Catálogos cerrados (a fijar en `src/generador/config.yaml`): tipo, estado del equipo, categoría, prioridad, estado del ticket, departamento, ubicación, rol.
Patrones plausibles: la mayoría de tickets con prioridad media; tiempo de resolución dependiente de prioridad y categoría; algunos modelos con tasa de incidencias superior a la media.

## Anomalías inyectadas (5 familias) y reglas de validación
| Familia | Ejemplo | Regla |
|---|---|---|
| Nulos en campos obligatorios | ticket sin categoría | completitud |
| Duplicados | mismo equipo dado de alta dos veces | unicidad de clave |
| Referencias huérfanas | ticket con equipo_id inexistente | integridad referencial |
| Fechas incoherentes | cierre anterior a apertura | consistencia temporal |
| Valores fuera de catálogo | prioridad "Urgentísima" | validez |
El generador escribe un **registro de anomalías inyectadas** (`data/raw/anomalias_inyectadas.csv`) para calcular la tasa de detección = detectadas / inyectadas.

## Indicadores (KPI)
- Volumen y carga: tickets abiertos/cerrados por periodo; backlog.
- Tiempo y servicio: MTTR; % cumplimiento de SLA por prioridad (plazos a definir en config).
- Parque: equipos por estado; antigüedad media; garantías vencidas.
- Relación: tickets por modelo de equipo; por departamento y ubicación.
Filtros del dashboard: periodo, ubicación, departamento, categoría, prioridad.

## Diseño de la evaluación
Mismo conjunto de datos en dos escenarios:
- **A:** arquitectura automatizada (Airflow + Spark + Power BI).
- **B (referencia):** procedimiento no automatizado en **Excel** ejecutado manualmente por el autor con un procedimiento escrito **antes** de ejecutarlo (`referencia/procedimiento.md`). Solo con tamaños pequeños (10^3 y 10^4 tickets): Excel no es viable a mayor volumen (límite de 1.048.576 filas y degradación de rendimiento), lo cual se documenta como resultado.

| Métrica | Medición |
|---|---|
| Tiempo de procesamiento | A: duración registrada por Airflow/Spark, 5 repeticiones × 4 tamaños (10^3–10^6). B: cronometraje manual, 3 repeticiones × 2 tamaños (10^3, 10^4). Media y dispersión. |
| Calidad del dato | % de registros que incumplen reglas tras el proceso; tasa de detección de anomalías; completitud de la salida. |
| Disponibilidad de indicadores | Tiempo desde la llegada de datos hasta que los indicadores son consultables (en A: hasta la capa analítica; la actualización de Power BI Desktop es manual y se mide aparte); proporción de indicadores disponibles frente a los definidos. |
| Usabilidad del dashboard | Cuestionario SUS (Brooke, 1996) a 5–8 compañeros de TI tras realizar tareas guiadas con el dashboard; anónimo y con consentimiento informado. |

Hipótesis (se contrastan, no se dan por buenas): A reduce tiempo y latencia; A detecta al menos tantas anomalías con menor variabilidad. Se reportará con honestidad si en volúmenes pequeños B es más rápido (sobrecarga de arranque de Spark).
Amenazas a la validez: B lo ejecuta el propio autor; datos simulados; entorno local; muestra pequeña y de conveniencia en la prueba SUS.

## Decisiones de diseño ya tomadas
Batch (no streaming); capas raw/curated/quarantine/analytics en formato columnar; Airflow 3 (LocalExecutor) por dependencias, reintentos y monitorización; Spark 4 en modo `local[*]` dentro de la misma imagen que Airflow (sin clúster separado: simplicidad y reproducibilidad); Power BI Desktop por adopción y modelo relacional. Todo se ejecuta en Docker (Airflow no funciona de forma nativa en Windows).

## Calendario
E1 (capítulos 1–3): entregada el 30/09/2026. E2 (~principios de noviembre; capítulos 4 y 5): generador + pipeline Spark avanzados. **E3 (7/12/2026): trabajo completo** (pipeline, Airflow, dashboard, experimento A/B, SUS, capítulos 6–8). Después: correcciones, predepósito y defensa.

## Relación con la memoria
| Parte del repositorio | Capítulo |
|---|---|
| src/generador, docs/esquema | 5 (datos) y 6 |
| src/pipeline, dags | 5 y 6 |
| dashboard | 5 |
| referencia, resultados | 5 (experimento) y 7 |
Guardar en `resultados/` todo lo que haya que citar (tiempos en CSV, informes de calidad, capturas).
