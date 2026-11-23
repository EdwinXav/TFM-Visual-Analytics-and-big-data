"""Comparación del escenario A (pipeline) con el escenario B (Excel).

Uso:
    python -m src.evaluacion.comparar_ab

Entradas (resultados/):
    tiempos/escenario_A_experimento.csv     una fila por ejecución del DAG (serie expA)
    tiempos/escenario_B.csv                 una fila por fase y repetición (registro manual)
    escenario_B/kpi_n<N>_r<k>.csv           indicadores exportados desde Excel
    escenario_B/rechazados_n<N>_r<k>.csv    registros marcados como anómalos en Excel

Salidas (resultados/tiempos/):
    escenario_B_repeticiones.csv  una fila por repetición de B: tiempo por fase, latencia y calidad
    comparacion_AB.csv            por tamaño: latencia media de A y de B, cociente B/A y calidad

La latencia de B es la suma de F1–F4 (la exportación, F5, no cuenta), como fija
``referencia/procedimiento.md``. La de A es ``latencia_s``: desde la llegada del conjunto hasta
que los indicadores están disponibles. Los tamaños sin repeticiones de B (10^5 y 10^6) se
incluyen con B vacío: no se ejecutan en Excel.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

FASES_LATENCIA = ["F1", "F2", "F3", "F4"]


def leer_registro_b(fichero: Path) -> pd.DataFrame:
    """Lee el registro manual; admite coma decimal en «segundos» por si se rellena en Excel."""
    df = pd.read_csv(fichero, sep=None, engine="python", dtype=str, encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]
    df["n"] = df["n"].astype(int)
    df["repeticion"] = df["repeticion"].astype(int)
    df["fase"] = df["fase"].str.strip().str.upper()
    df["segundos"] = pd.to_numeric(df["segundos"].str.replace(",", ".", regex=False), errors="coerce")
    # Si «segundos» está vacío, se calcula con las horas de inicio y fin (hh:mm:ss del mismo día).
    vacio = df["segundos"].isna()
    if vacio.any():
        ini = pd.to_timedelta(df.loc[vacio, "inicio"].str.strip())
        fin = pd.to_timedelta(df.loc[vacio, "fin"].str.strip())
        df.loc[vacio, "segundos"] = (fin - ini).dt.total_seconds()
    if df["segundos"].isna().any() or (df["segundos"] < 0).any():
        raise ValueError(f"Fases sin tiempo válido:\n{df[df['segundos'].isna() | (df['segundos'] < 0)]}")
    return df


def tiempos_b(registro: pd.DataFrame) -> pd.DataFrame:
    """Una fila por (n, repetición) con los segundos de cada fase y la latencia (F1–F4)."""
    t = registro.pivot_table(index=["n", "repeticion"], columns="fase", values="segundos", aggfunc="sum")
    faltan = [f for f in FASES_LATENCIA if f not in t.columns]
    if faltan:
        raise ValueError(f"El registro de B no tiene las fases {faltan}")
    incompletas = t[t[FASES_LATENCIA].isna().any(axis=1)]
    if len(incompletas):
        raise ValueError(f"Repeticiones sin todas las fases F1–F4: {list(incompletas.index)}")
    t["latencia_s"] = t[FASES_LATENCIA].sum(axis=1)
    t.columns = [f"{c}_s" if c in FASES_LATENCIA + ["F5"] else c for c in t.columns]
    return t.reset_index()


def calidad_b(n: int, k: int, carpeta: Path, base: Path | None = None) -> dict:
    """Detección e indicadores de una repetición de B frente a la referencia (escenario_b.py)."""
    kpi, rech = carpeta / f"kpi_n{n}_r{k}.csv", carpeta / f"rechazados_n{n}_r{k}.csv"
    if not (kpi.exists() and rech.exists()):
        return {}
    from src.evaluacion.escenario_b import evaluar  # importa el pipeline (PySpark): solo si hace falta

    ev = evaluar(n, rech, kpi, base)
    d, i = ev["deteccion"], ev["indicadores"]
    return {"tasa_deteccion": d["tasa_deteccion"], "falsos_positivos": d["falsos_positivos"],
            "kpi_comparados": i["valores_comparados"], "kpi_coincidentes": i["coincidentes"],
            "kpi_sin_emparejar": len(i["sin_emparejar"])}


def resumir(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    """Por tamaño: latencia de A y de B (media, desviación típica, repeticiones) y cociente B/A."""
    a = a[a["estado"] == "success"]
    ra = a.groupby("n")["latencia_s"].agg(a_reps="count", a_latencia_media="mean", a_latencia_std="std")
    cols = {"b_reps": ("latencia_s", "count"), "b_latencia_media": ("latencia_s", "mean"),
            "b_latencia_std": ("latencia_s", "std")}
    # Calidad en el peor caso de las repeticiones: menor detección y coincidencia, más falsos positivos.
    peor = {"tasa_deteccion": "min", "kpi_coincidentes": "min", "falsos_positivos": "max", "kpi_comparados": "max"}
    for c, f in peor.items():
        if c in b.columns:
            cols[f"b_{c}_{f}"] = (c, f)
    rb = b.groupby("n").agg(**cols) if len(b) else pd.DataFrame(columns=list(cols))
    r = ra.join(rb, how="outer")
    r["cociente_b_a"] = r["b_latencia_media"] / r["a_latencia_media"]
    return r.round(3).reset_index()


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Compara los escenarios A (pipeline) y B (Excel).")
    p.add_argument("--resultados", type=Path, default=Path("resultados"))
    a = p.parse_args(argv)
    tiempos = a.resultados / "tiempos"

    exp_a = pd.read_csv(tiempos / "escenario_A_experimento.csv")
    b = tiempos_b(leer_registro_b(tiempos / "escenario_B.csv"))
    calidad = pd.DataFrame([{"n": n, "repeticion": k, **calidad_b(n, k, a.resultados / "escenario_B")}
                            for n, k in zip(b["n"], b["repeticion"])])
    b = b.merge(calidad, on=["n", "repeticion"], how="left")
    b.round(3).to_csv(tiempos / "escenario_B_repeticiones.csv", index=False)

    res = resumir(exp_a, b)
    res.to_csv(tiempos / "comparacion_AB.csv", index=False)
    print(b.round(3).to_string(index=False))
    print()
    print(res.to_string(index=False))


if __name__ == "__main__":
    main()
