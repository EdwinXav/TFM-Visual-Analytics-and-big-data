"""DAG del pipeline del TFM (escenario A): raw -> curated/quarantine -> analytics.

Se ejecuta cada día a las 06:00 UTC. El sensor espera a que llegue un conjunto
completo que aún no se haya procesado (marcador ``_LISTO`` del generador); si no
llega ninguno en 6 horas, la ejecución se omite sin contar como fallo. La marca
de tiempo de llegada se transmite a las tareas siguientes para medir la
disponibilidad de los indicadores.

Lanzar a mano:           airflow dags trigger pipeline_tfm --conf '{"n": 100000}'
Repetir sobre los mismos datos (experimento): añadir "forzar": true a --conf.
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, PokeReturnValue, dag, get_current_context, task


def _contexto():
    ctx = get_current_context()
    run_id = ctx["run_id"].replace(":", "").replace("+", "").replace(".", "")
    return int(ctx["params"]["n"]), run_id


@dag(
    dag_id="pipeline_tfm",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "n": Param(10000, type="integer", description="Tamaño del conjunto (nº de tickets)"),
        "forzar": Param(False, type="boolean", description="Reprocesar aunque los datos no sean nuevos"),
    },
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["tfm", "escenario-A"],
)
def pipeline_tfm():
    @task.sensor(poke_interval=30, timeout=6 * 3600, mode="reschedule", soft_fail=True)
    def esperar_datos() -> PokeReturnValue:
        """Espera un conjunto completo sin procesar y registra cuándo llegó."""
        from src.pipeline.comun import base_dir, rutas
        from src.pipeline.entrada import hora_llegada, pendiente_de_procesar

        n, _ = _contexto()
        carpeta = rutas(base_dir(), n)["raw"]
        if not pendiente_de_procesar(carpeta, bool(get_current_context()["params"]["forzar"])):
            return PokeReturnValue(is_done=False)
        return PokeReturnValue(is_done=True, xcom_value={
            "n": n,
            "llegada_utc": hora_llegada(carpeta),
            "detectado_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
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

    @task
    def marcar_procesado(entrada: dict, _recuentos: dict) -> None:
        from src.pipeline.comun import base_dir, rutas
        from src.pipeline import entrada as control

        control.marcar_procesado(rutas(base_dir(), entrada["n"])["raw"], entrada["llegada_utc"])

    entrada = esperar_datos()
    marcar_procesado(entrada, analitica(entrada, curar(entrada)))


pipeline_tfm()
