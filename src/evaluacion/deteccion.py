"""Tasa de detección: compara la cuarentena del pipeline con las anomalías inyectadas.

Uso:
    python -m src.evaluacion.deteccion --n 10000
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.pipeline.comun import base_dir, rutas

# Regla del pipeline -> familia de anomalía que detecta
REGLA_A_FAMILIA = {
    "completitud": "nulos",
    "unicidad": "duplicados",
    "integridad_referencial": "huerfanos",
    "consistencia_temporal": "fechas_incoherentes",
    "validez": "fuera_de_catalogo",
}
CLAVE = {"tickets": "ticket_id", "inventario": "equipo_id", "usuarios": "usuario_id"}


def leer_cuarentena(carpeta: Path) -> pd.DataFrame:
    partes = []
    for tabla, clave in CLAVE.items():
        ruta = carpeta / tabla
        if ruta.exists() and any(ruta.glob("*.parquet")):
            df = pd.read_parquet(ruta)
            partes.append(pd.DataFrame({"tabla": tabla, "clave": df[clave].astype(str),
                                        "familia": df["motivo"].map(REGLA_A_FAMILIA)}))
    return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame(columns=["tabla", "clave", "familia"])


def comparar(inyectadas: pd.DataFrame, detectadas: pd.DataFrame) -> dict:
    """Empareja por (tabla, clave, familia) contando repeticiones (duplicados múltiples)."""
    def multiconjunto(df):
        return df.groupby(["tabla", "clave", "familia"]).size()

    iny, det = multiconjunto(inyectadas), multiconjunto(detectadas)
    comun = pd.concat([iny, det], axis=1, keys=["i", "d"]).fillna(0)
    comun["ok"] = comun[["i", "d"]].min(axis=1)
    por_familia = {}
    for fam, g in comun.groupby(level="familia"):
        i, d, ok = int(g["i"].sum()), int(g["d"].sum()), int(g["ok"].sum())
        por_familia[fam] = {"inyectadas": i, "detectadas": d, "aciertos": ok,
                            "tasa_deteccion": round(ok / i, 4) if i else None,
                            "falsos_positivos": d - ok}
    i, ok = int(comun["i"].sum()), int(comun["ok"].sum())
    return {"inyectadas": i, "aciertos": ok, "tasa_deteccion": round(ok / i, 4) if i else None,
            "falsos_positivos": int(comun["d"].sum()) - ok, "por_familia": por_familia}


def evaluar(n: int, base: Path | None = None) -> dict:
    r = rutas(base or base_dir(), n)
    iny = pd.read_csv(r["raw"] / "anomalias_inyectadas.csv", dtype=str, keep_default_na=False)
    return comparar(iny[["tabla", "clave", "familia"]], leer_cuarentena(r["quarantine"]))


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Tasa de detección del escenario A.")
    parser.add_argument("--n", type=int, required=True)
    args = parser.parse_args(argv)
    print(json.dumps(evaluar(args.n), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
