"""Evaluación del escenario B: compara lo exportado desde Excel con la referencia.

- Detección: ``rechazados.csv`` frente a las anomalías inyectadas.
- Indicadores: ``kpi.csv`` frente a los calculados por el pipeline (escenario A).

Excel guarda los CSV con la configuración regional: separador ``;`` y coma decimal.

Uso:
    python -m src.evaluacion.escenario_b --n 1000 --rechazados <csv> --kpi <csv>
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.evaluacion.deteccion import REGLA_A_FAMILIA, comparar
from src.pipeline.comun import base_dir, rutas

TOLERANCIA = 0.01  # diferencia absoluta admitida por el redondeo entre herramientas
CLAVES = ["kpi", "dimension", "categoria"]


def leer_rechazados(fichero: Path) -> pd.DataFrame:
    df = pd.read_csv(fichero, sep=";", dtype=str, keep_default_na=False, encoding="latin-1")
    return pd.DataFrame({"tabla": df["tabla"], "clave": df["clave"],
                         "familia": df["motivo"].map(REGLA_A_FAMILIA)})


def leer_kpi_excel(fichero: Path) -> pd.DataFrame:
    return pd.read_csv(fichero, sep=";", decimal=",", encoding="latin-1")


def comparar_kpi(excel: pd.DataFrame, pipeline: pd.DataFrame, tolerancia: float = TOLERANCIA) -> dict:
    """Empareja por indicador, dimensión y categoría, y lista las diferencias."""
    m = pipeline[CLAVES + ["valor"]].merge(excel[CLAVES + ["valor"]], on=CLAVES, how="outer",
                                           suffixes=("_a", "_b"), indicator=True)
    # Una categoría sin registros (p. ej. un modelo sin tickets) vale 0 o queda vacía en Excel
    # y no genera fila en Spark: no es una discrepancia.
    vacia = (m["_merge"] == "right_only") & (m["valor_b"].isna() | (m["valor_b"] == 0))
    m = m[~vacia]
    faltan = m[m["_merge"] != "both"]
    ambos = m[m["_merge"] == "both"].copy()
    ambos["diferencia"] = (ambos["valor_a"].astype(float) - ambos["valor_b"].astype(float)).abs()
    distintos = ambos[ambos["diferencia"] > tolerancia]
    return {
        "valores_comparados": len(ambos),
        "coincidentes": int((ambos["diferencia"] <= tolerancia).sum()),
        "diferencia_maxima": round(float(ambos["diferencia"].max()), 6) if len(ambos) else None,
        "sin_emparejar": faltan[CLAVES].to_dict("records"),
        "distintos": distintos[CLAVES + ["valor_a", "valor_b"]].to_dict("records"),
    }


def evaluar(n: int, rechazados: Path, kpi: Path, base: Path | None = None) -> dict:
    r = rutas(base or base_dir(), n)
    iny = pd.read_csv(r["raw"] / "anomalias_inyectadas.csv", dtype=str, keep_default_na=False)
    return {
        "deteccion": comparar(iny[["tabla", "clave", "familia"]], leer_rechazados(rechazados)),
        "indicadores": comparar_kpi(leer_kpi_excel(kpi), pd.read_parquet(r["analytics"] / "kpi_resumen.parquet")),
    }


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Evaluación del escenario B (Excel).")
    parser.add_argument("--n", type=int, required=True)
    parser.add_argument("--rechazados", type=Path, required=True)
    parser.add_argument("--kpi", type=Path, required=True)
    args = parser.parse_args(argv)
    print(json.dumps(evaluar(args.n, args.rechazados, args.kpi), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
