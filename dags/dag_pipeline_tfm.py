"""DAG del pipeline del TFM (escenario A): raw -> curated/quarantine -> analytics.

El DAG espera a que llegue un conjunto completo de datos (sensor sobre el
marcador ``_LISTO`` que escribe el generador al terminar) y encadena las
etapas de Spark. La marca de tiempo de llegada se transmite a las tareas
siguientes para medir la disponibilidad de los indicadores.

Lanzar con otro tamaño:  airflow dags trigger pipeline_tfm --conf '{"n": 100000}'
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, PokeReturnValue, dag, get_current_context, task

FICHEROS = ("usuarios.csv", "inventario.csv", "tickets.csv")
MARCADOR = "_LISTO"


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
    @task.sensor(poke_interval=30, timeout=6 * 3600, mode="reschedule")
    def esperar_datos() -> PokeReturnValue:
        """Espera al marcador de conjunto completo y registra cuándo llegaron los datos."""
        from src.pipeline.comun import base_dir, rutas

        n, _ = _contexto()
        carpeta = rutas(base_dir(), n)["raw"]
        marcador = carpeta / MARCADOR
        if not marcador.is_file() or not all((carpeta / f).is_file() for f in FICHEROS):
            return PokeReturnValue(is_done=False)
        llegada = datetime.fromtimestamp(marcador.stat().st_mtime, tz=timezone.utc)
        return PokeReturnValue(is_done=True, xcom_value={
            "n": n,
            "llegada_utc": llegada.isoformat(timespec="seconds"),
            "detectado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })

    @task
    def curar(entrada: dict) -> dict:
        from src.pipeline.ejecutar import ejecutar

        _, run_id = _contexto()
        informe = ejecutar(entrada["n"], etapas=["curar"], run_id=run_id)["calidad"]
        return {t: d["rechazados"] for t, d in informe["tablas"].items()}

    @task
    def analitica(entrada: dict, _rechazados: dict) -> dict:
        from src.pipeline.ejecutar import ejecutar

        _, run_id = _contexto()
        return ejecutar(entrada["n"], etapas=["analitica"], run_id=run_id)["analitica"]

    entrada = esperar_datos()
    analitica(entrada, curar(entrada))


pipeline_tfm()
