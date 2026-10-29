"""DAG del pipeline del TFM (escenario A): raw -> curated/quarantine -> analytics.

Versión básica (iteración 2): comprueba la entrada y encadena las etapas de
Spark. En la iteración 3 se añadirán el sensor de llegada de ficheros, la
programación periódica y el registro de latencia de disponibilidad.

Lanzar con otro tamaño:  airflow dags trigger pipeline_tfm --conf '{"n": 100000}'
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task

FICHEROS = ("usuarios.csv", "inventario.csv", "tickets.csv")


def _contexto():
    ctx = get_current_context()
    run_id = ctx["run_id"].replace(":", "").replace("+", "").replace(".", "")
    return int(ctx["params"]["n"]), run_id


@dag(
    dag_id="pipeline_tfm",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"n": Param(10000, type="integer", description="Tamaño del conjunto (nº de tickets)")},
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["tfm", "escenario-A"],
)
def pipeline_tfm():
    @task
    def comprobar_entrada() -> int:
        from src.pipeline.comun import base_dir, rutas

        n, _ = _contexto()
        carpeta = rutas(base_dir(), n)["raw"]
        faltan = [f for f in FICHEROS if not (carpeta / f).is_file()]
        if faltan:
            raise FileNotFoundError(f"Faltan ficheros en {carpeta}: {faltan}")
        return n

    @task
    def curar(n: int) -> dict:
        from src.pipeline.ejecutar import ejecutar

        _, run_id = _contexto()
        informe = ejecutar(n, etapas=["curar"], run_id=run_id)["calidad"]
        return {t: d["rechazados"] for t, d in informe["tablas"].items()}

    @task
    def analitica(n: int, _rechazados: dict) -> dict:
        from src.pipeline.ejecutar import ejecutar

        _, run_id = _contexto()
        return ejecutar(n, etapas=["analitica"], run_id=run_id)["analitica"]

    n = comprobar_entrada()
    analitica(n, curar(n))


pipeline_tfm()
