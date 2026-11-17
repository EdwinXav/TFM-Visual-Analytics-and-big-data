"""Experimento del escenario A: repeticiones del DAG pipeline_tfm a través del scheduler de Airflow.

Uso (dentro del contenedor del scheduler):
    python -m src.evaluacion.escenario_a                       # 5 repeticiones x 4 tamaños
    python -m src.evaluacion.escenario_a --repeticiones 1 --tamanos 1000
    python -m src.evaluacion.escenario_a --solo-resumen        # recalcula el resumen

Cada ejecución se lanza con ``airflow dags trigger`` (``forzar`` = true, mismos datos en cada
repetición) y se espera a que termine antes de lanzar la siguiente. Los tamaños se alternan
dentro de cada repetición (r1: 10^3, 10^4, 10^5, 10^6; r2: ...) para que una deriva del equipo
durante el experimento no afecte a un solo tamaño.

Los run_id son ``<serie>_r<k>_n<N>``; si ya existe uno terminado con éxito, no se repite, así que
el experimento se puede reanudar si se interrumpe.

Salidas (resultados/tiempos/):
    escenario_A_experimento.csv  una fila por ejecución
    escenario_A_resumen.csv      media, desviación típica, mínimo y máximo por tamaño
Los tiempos por paso siguen en escenario_A_pasos.csv y la latencia en escenario_A_disponibilidad.csv.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.pipeline.comun import base_dir, rutas

log = logging.getLogger("escenario_a")
DAG_ID = "pipeline_tfm"
TAMANOS = [1000, 10000, 100000, 1000000]
N_POR_DEFECTO = 10000  # parámetro n de la ejecución programada (dags/dag_pipeline_tfm.py)
TERMINADOS = {"success", "failed"}
ESPERA_S = 15


def airflow(*args: str) -> str:
    r = subprocess.run(["airflow", *args], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"airflow {' '.join(args)} falló:\n{r.stderr[-2000:]}")
    return r.stdout


def _json(salida: str):
    """La CLI puede escribir avisos antes del JSON: se toma desde el primer '[' o '{'."""
    inicio = min(i for i in (salida.find("["), salida.find("{")) if i >= 0)
    return json.loads(salida[inicio:])


def ejecuciones() -> dict[str, dict]:
    return {r["run_id"]: r for r in _json(airflow("dags", "list-runs", DAG_ID, "-o", "json"))}


def en_pausa() -> bool:
    dags = _json(airflow("dags", "list", "-o", "json"))
    return str(next(d for d in dags if d["dag_id"] == DAG_ID)["is_paused"]).lower() == "true"


def esperar_sin_activas(limite_s: int = 900, salvo: str | None = None) -> None:
    """Espera a que no haya ejecuciones en curso (p. ej. la programada que crea Airflow al reactivar)."""
    t0 = time.monotonic()
    while True:
        activas = [r for r, d in ejecuciones().items() if d["state"] not in TERMINADOS and r != salvo]
        if not activas:
            return
        if time.monotonic() - t0 > limite_s:
            raise RuntimeError(f"Siguen activas tras {limite_s} s: {activas}. Revísalas en la interfaz de Airflow.")
        log.info("Esperando a que terminen: %s", activas)
        time.sleep(ESPERA_S)


def esperar(run_id: str, limite_s: int = 1800) -> dict:
    t0 = time.monotonic()
    while time.monotonic() - t0 < limite_s:
        d = ejecuciones().get(run_id)
        if d and d["state"] in TERMINADOS:
            return d
        time.sleep(ESPERA_S)
    raise RuntimeError(f"{run_id} no ha terminado en {limite_s} s")


def preparar(base: Path, tamanos: list[int]) -> bool:
    """Comprueba los datos y activa el DAG. Devuelve si estaba en pausa (para dejarlo igual)."""
    for n in tamanos:
        if not (rutas(base, n)["raw"] / "_LISTO").exists():
            raise SystemExit(f"Falta el conjunto data/raw/n{n} (o su marcador _LISTO): genéralo antes.")
    pausado = en_pausa()
    if pausado:
        # Al reactivarlo, Airflow crea la ejecución programada pendiente (n por defecto). Si ese
        # conjunto ya está procesado, su sensor esperaría hasta 6 h y bloquearía el experimento
        # (max_active_runs = 1). Se le deja procesar el conjunto como una llegada normal.
        marca = rutas(base, N_POR_DEFECTO)["raw"] / "_PROCESADO"
        if marca.exists():
            marca.unlink()
            log.info("Retirado %s para que la ejecución programada no se quede esperando", marca)
        airflow("dags", "unpause", DAG_ID)
        log.info("DAG reactivado; se espera a la ejecución programada que crea Airflow")
        time.sleep(60)
    esperar_sin_activas()
    return pausado


def lanzar(serie: str, repeticiones: int, tamanos: list[int]) -> None:
    previas = ejecuciones()
    for k in range(1, repeticiones + 1):
        for n in tamanos:
            run_id = f"{serie}_r{k}_n{n}"
            if previas.get(run_id, {}).get("state") == "success":
                log.info("%s ya hecho, se salta", run_id)
                continue
            if run_id in previas:
                raise SystemExit(f"{run_id} ya existe sin éxito: bórralo en Airflow o usa otra --serie.")
            log.info("Lanzando %s", run_id)
            airflow("dags", "trigger", DAG_ID, "--run-id", run_id, "--conf", json.dumps({"n": n, "forzar": True}))
            d = esperar(run_id)
            log.info("%s: %s", run_id, d["state"])


def resumir(base: Path, serie: str) -> pd.DataFrame:
    tiempos = rutas(base, TAMANOS[0])["tiempos"]
    runs = pd.DataFrame([d for r, d in ejecuciones().items() if r.startswith(f"{serie}_r")])
    if runs.empty:
        raise SystemExit(f"No hay ejecuciones de la serie {serie}")
    runs["repeticion"] = runs["run_id"].str.extract(r"_r(\d+)_n")[0].astype(int)
    runs["n"] = runs["run_id"].str.extract(r"_n(\d+)$")[0].astype(int)
    ini, fin = pd.to_datetime(runs["start_date"], utc=True), pd.to_datetime(runs["end_date"], utc=True)
    runs["dag_s"] = (fin - ini).dt.total_seconds().round(3)

    pasos = pd.read_csv(tiempos / "escenario_A_pasos.csv")
    tot = pasos[pasos["etapa"] == "total"].pivot_table(index="run_id", columns="paso", values="segundos", aggfunc="sum")
    tot = tot.rename(columns={"sesion_spark": "sesion_spark_s", "curar": "curar_s", "analitica": "analitica_s"})
    tot["spark_s"] = tot.sum(axis=1).round(3)

    disp = pd.read_csv(tiempos / "escenario_A_disponibilidad.csv").drop_duplicates("run_id", keep="last")
    disp = disp.set_index("run_id")[["latencia_s", "indicadores_disponibles", "proporcion_disponible"]]

    exp = (runs.set_index("run_id")[["repeticion", "n", "state", "start_date", "end_date", "dag_s"]]
           .join(tot[["sesion_spark_s", "curar_s", "analitica_s", "spark_s"]]).join(disp)
           .rename(columns={"state": "estado", "start_date": "inicio_utc", "end_date": "fin_utc"})
           .reset_index().sort_values(["n", "repeticion"]))
    exp.insert(1, "serie", serie)
    exp.to_csv(tiempos / "escenario_A_experimento.csv", index=False)

    ok = exp[exp["estado"] == "success"]
    metricas = ["dag_s", "spark_s", "curar_s", "analitica_s", "latencia_s"]
    res = ok.groupby("n")[metricas].agg(["count", "mean", "std", "min", "max"]).round(3)
    res.columns = [f"{m}_{a}" for m, a in res.columns]
    res = res.reset_index()
    res.to_csv(tiempos / "escenario_A_resumen.csv", index=False)
    return res


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Experimento del escenario A (repeticiones del DAG).")
    p.add_argument("--repeticiones", type=int, default=5)
    p.add_argument("--tamanos", type=int, nargs="+", default=TAMANOS)
    p.add_argument("--serie", default="expA", help="prefijo de los run_id")
    p.add_argument("--solo-resumen", action="store_true")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    base = base_dir()
    if not a.solo_resumen:
        log.info("Inicio del experimento: serie %s, %d repeticiones, tamaños %s (%s)",
                 a.serie, a.repeticiones, a.tamanos, datetime.now().isoformat(timespec="seconds"))
        pausado = preparar(base, a.tamanos)
        try:
            lanzar(a.serie, a.repeticiones, a.tamanos)
        finally:
            if pausado:
                airflow("dags", "pause", DAG_ID)
                log.info("DAG vuelto a poner en pausa")
    print(resumir(base, a.serie).to_string(index=False))


if __name__ == "__main__":
    main()
