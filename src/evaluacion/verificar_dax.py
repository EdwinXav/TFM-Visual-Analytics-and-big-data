"""Verificación del dashboard: compara las medidas DAX exportadas desde Power BI con kpi_resumen (Spark).

Uso:
    python -m src.evaluacion.verificar_dax --n 100000

Lee resultados/datos/verificacion_dax_n<N>.csv (tabla de la página «Verificación», exportada
desde Power BI) y la compara con data/analytics/n<N>/kpi_resumen.parquet. La diferencia se
recalcula aquí, sin fiarse de la columna «Diferencia» exportada.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

TOLERANCIA = 1e-3  # Spark redondea los KPI a 4 decimales
CLAVES = ["kpi", "dimension", "categoria"]


def a_numero(valor) -> float:
    """Convierte un número exportado con cualquier configuración regional.

    Los valores se exportan con 4 decimales, así que el último separador ('.' o ',') es el decimal
    y los anteriores son de miles: «96.000,0000» y «96,000.0000» dan 96000.0.
    """
    if pd.isna(valor):
        return float("nan")
    s = str(valor).strip().replace(" ", "").replace(" ", "")
    if s == "":
        return float("nan")
    pos = max(s.rfind("."), s.rfind(","))
    if pos == -1:
        return float(s)
    entero = s[:pos].replace(".", "").replace(",", "")
    return float(f"{entero}.{s[pos + 1:]}")


def leer_exportacion(ruta: Path) -> pd.DataFrame:
    df = pd.read_csv(ruta, dtype=str, encoding="utf-8-sig", sep=None, engine="python")
    df.columns = [c.strip() for c in df.columns]
    faltan = [c for c in CLAVES + ["Valor DAX"] if c not in df.columns]
    if faltan:
        raise SystemExit(f"Faltan columnas en la exportación: {faltan}. Columnas leídas: {list(df.columns)}")
    df["valor_dax"] = df["Valor DAX"].map(a_numero)
    return df[CLAVES + ["valor_dax"]]


def comparar(spark: pd.DataFrame, dax: pd.DataFrame) -> pd.DataFrame:
    m = spark.merge(dax, on=CLAVES, how="outer", indicator=True)
    m["diferencia"] = (m["valor_dax"] - m["valor"]).abs()
    m["ok"] = (m["_merge"] == "both") & (m["diferencia"] < TOLERANCIA)
    return m


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Compara las medidas DAX con los KPI de Spark.")
    parser.add_argument("--n", type=int, required=True)
    parser.add_argument("--base", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args(argv)

    spark = pd.read_parquet(args.base / "data" / "analytics" / f"n{args.n}" / "kpi_resumen.parquet")
    spark["categoria"] = spark["categoria"].astype(str)
    dax = leer_exportacion(args.base / "resultados" / "datos" / f"verificacion_dax_n{args.n}.csv")
    m = comparar(spark, dax)

    print(f"Filas Spark: {len(spark)} · filas exportadas: {len(dax)} · coinciden: {int(m['ok'].sum())}")
    print(f"Diferencia máxima: {m['diferencia'].max():.6f} (tolerancia {TOLERANCIA})")
    malas = m[~m["ok"]]
    if malas.empty:
        print("Verificación correcta: las medidas DAX reproducen los KPI de Spark.")
        return 0
    print("Filas que no cuadran:")
    print(malas[CLAVES + ["valor", "valor_dax", "diferencia", "_merge"]].to_string(index=False))
    return 1


if __name__ == "__main__":
    sys.exit(main())
