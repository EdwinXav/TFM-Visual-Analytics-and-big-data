# Orquestación del pipeline

El DAG `pipeline_tfm` (`dags/dag_pipeline_tfm.py`) ejecuta el escenario A de principio a fin. La interfaz de Airflow está en http://localhost:8080 (usuario y clave por defecto: `airflow` / `airflow`).

## Tareas
| Tarea | Función |
|---|---|
| `esperar_datos` | Sensor: espera un conjunto completo (`_LISTO`) que no se haya procesado (`_PROCESADO`). Comprueba cada 30 s, sin ocupar un proceso mientras espera. |
| `curar` | Spark: ingesta, validación, cuarentena y capa curated. |
| `analitica` | Spark: modelo en estrella e indicadores K1–K10. |
| `registrar_disponibilidad` | Mide la latencia y los indicadores disponibles. |
| `marcar_procesado` | Registra la llegada ya tratada, para no repetirla. |

Cada tarea tiene un reintento automático, a los 30 s.

## Parámetros
| Parámetro | Por defecto | Uso |
|---|---|---|
| `n` | 10000 | Tamaño del conjunto: lee `data/raw/n<n>/`. |
| `forzar` | `false` | Procesa aunque los datos no sean nuevos (repeticiones del experimento). |

## Ejecución programada
Se ejecuta cada día a las 06:00 UTC con los parámetros por defecto, solo si el DAG está activo y Docker en marcha. El DAG se crea en pausa. Si no hay datos nuevos en 6 horas, la tarea se omite y la ejecución no cuenta como fallo.

```bash
docker compose exec airflow-scheduler airflow dags unpause pipeline_tfm
docker compose exec airflow-scheduler airflow dags pause pipeline_tfm
```

Al activarlo, Airflow crea enseguida la ejecución del último periodo. Solo se permite una ejecución activa a la vez: si esa ejecución espera datos, las que se lancen a mano quedan en cola.

## Ejecución manual
Con el DAG activo:

```bash
docker compose exec airflow-scheduler airflow dags trigger pipeline_tfm --conf '{"n": 100000, "forzar": true}'
docker compose exec airflow-scheduler airflow dags list-runs pipeline_tfm
```

## Qué revisar
| Qué | Dónde |
|---|---|
| Estado de cada ejecución y tarea | Interfaz web: DAG `pipeline_tfm` |
| Log de una tarea | `logs/dag_id=pipeline_tfm/run_id=<run>/task_id=<tarea>/attempt=<n>.log` |
| Duración por paso | `resultados/tiempos/escenario_A_pasos.csv` |
| Latencia y disponibilidad | `resultados/tiempos/escenario_A_disponibilidad.csv` |
| Calidad por tabla y regla | `resultados/calidad/n<n>/calidad_ultima.json` |
| Registros rechazados | `data/quarantine/n<n>/` |

## Problemas habituales
- **La ejecución se queda en `esperar_datos`:** no hay datos nuevos. Genera el conjunto o usa `forzar`.
- **Una ejecución manual no arranca:** el DAG está en pausa o hay otra ejecución activa.
