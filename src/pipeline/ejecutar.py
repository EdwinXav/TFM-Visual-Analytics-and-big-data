"""Punto de entrada del pipeline (escenario A).

Uso:
    python -m src.pipeline.ejecutar --n 10000
    python -m src.pipeline.ejecutar --n 10000 --etapas curar
Las métricas de cada paso se añaden a resultados/tiempos/escenario_A_pasos.csv.
"""

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import datetime, timezone

from src.pipeline.analitica import analitica
from src.pipeline.comun import Metricas, base_dir, cargar_config, crear_sesion
from src.pipeline.curar import curar

log = logging.getLogger("pipeline")
ETAPAS = ("curar", "analitica")


def nuevo_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def ejecutar(n: int, etapas=ETAPAS, run_id: str | None = None, base=None) -> dict:
    base = base or base_dir()
    cfg = cargar_config(base)
    metricas = Metricas(run_id or nuevo_run_id(), n, base)
    resultado = {}
    with metricas.paso("total", "sesion_spark"):
        spark = crear_sesion()
    try:
        if "curar" in etapas:
            with metricas.paso("total", "curar"):
                resultado["calidad"] = curar(spark, cfg, n, base, metricas)
        if "analitica" in etapas:
            with metricas.paso("total", "analitica"):
                resultado["analitica"] = analitica(spark, cfg, n, base, metricas)
    finally:
        metricas.guardar()
    log.info("run %s (n=%d) completado: %s", metricas.run_id, n, resultado)
    return resultado


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Pipeline Spark del TFM (escenario A).")
    parser.add_argument("--n", type=int, required=True, help="tamaño del conjunto (nº de tickets)")
    parser.add_argument("--etapas", nargs="*", default=list(ETAPAS), choices=ETAPAS)
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ejecutar(args.n, args.etapas, args.run_id)


if __name__ == "__main__":
    main()
