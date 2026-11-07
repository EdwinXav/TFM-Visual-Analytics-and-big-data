"""Disponibilidad de indicadores: latencia y proporción de indicadores disponibles.

La latencia va desde que los datos llegan (marcador ``_LISTO``) hasta que los
indicadores se pueden consultar (escritura de ``kpi_resumen.parquet``). Si la
ejecución se fuerza sobre datos ya procesados, la llegada es antigua y se mide
desde la detección; esas filas quedan marcadas con ``forzado``.
La actualización de Power BI Desktop es manual y se mide aparte.
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

INDICADORES = [f"K{i}" for i in range(1, 11)]
COLUMNAS = ["run_id", "n", "forzado", "llegada_utc", "detectado_utc", "disponible_utc",
            "espera_s", "proceso_s", "latencia_s", "indicadores_definidos",
            "indicadores_disponibles", "proporcion_disponible"]


def indicadores_disponibles(fichero_kpi: Path) -> list[str]:
    """Indicadores definidos con al menos un valor calculado en la capa analítica."""
    kpi = pd.read_parquet(fichero_kpi, columns=["kpi", "valor"])
    presentes = set(kpi.loc[kpi["valor"].notna(), "kpi"])
    return [k for k in INDICADORES if k in presentes]


def medir(run_id: str, n: int, llegada_utc: str, detectado_utc: str, fichero_kpi: Path,
          forzado: bool = False) -> dict:
    llegada = datetime.fromisoformat(llegada_utc)
    detectado = datetime.fromisoformat(detectado_utc)
    disponible = datetime.fromtimestamp(fichero_kpi.stat().st_mtime, tz=timezone.utc)
    inicio = detectado if forzado else llegada
    presentes = indicadores_disponibles(fichero_kpi)
    return {
        "run_id": run_id, "n": n, "forzado": forzado,
        "llegada_utc": llegada_utc, "detectado_utc": detectado_utc,
        "disponible_utc": disponible.isoformat(timespec="milliseconds"),
        "espera_s": 0.0 if forzado else round((detectado - llegada).total_seconds(), 3),
        "proceso_s": round((disponible - detectado).total_seconds(), 3),
        "latencia_s": round((disponible - inicio).total_seconds(), 3),
        "indicadores_definidos": len(INDICADORES),
        "indicadores_disponibles": len(presentes),
        "proporcion_disponible": round(len(presentes) / len(INDICADORES), 4),
    }


def guardar(fila: dict, fichero: Path) -> None:
    fichero.parent.mkdir(parents=True, exist_ok=True)
    nuevo = not fichero.exists()
    with open(fichero, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNAS)
        if nuevo:
            w.writeheader()
        w.writerow(fila)
