"""Resumen descriptivo de un conjunto generado (para el capítulo 5 de la memoria).

Uso:
    python -m src.generador.resumen data/raw/n10000 --salida resultados/datos/resumen_n10000.md
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.generador.generar import cargar_config


def _tabla(serie: pd.Series, cabecera: tuple[str, str]) -> str:
    filas = [f"| {cabecera[0]} | {cabecera[1]} |", "|---|---|"]
    filas += [f"| {k} | {v} |" for k, v in serie.items()]
    return "\n".join(filas)


def resumir(carpeta: Path, cfg: dict) -> str:
    leer = lambda f: pd.read_csv(carpeta / f, dtype=str, keep_default_na=False)
    t, inv, u, reg = leer("tickets.csv"), leer("inventario.csv"), leer("usuarios.csv"), \
        leer("anomalias_inyectadas.csv")
    limpios = t[~t["ticket_id"].isin(reg["clave"])].copy()
    cerrados = limpios[limpios["fecha_cierre"] != ""].copy()
    cerrados["horas"] = (pd.to_datetime(cerrados["fecha_cierre"])
                         - pd.to_datetime(cerrados["fecha_apertura"])).dt.total_seconds() / 3600
    cerrados["cumple"] = cerrados["horas"] <= cerrados["prioridad"].map(cfg["sla_horas"])
    equipos = inv.drop_duplicates("equipo_id")
    tasa = (limpios.groupby(limpios["equipo_id"].map(equipos.set_index("equipo_id")["modelo"])).size()
            / equipos.groupby("modelo").size()).sort_values(ascending=False)

    orden_p = list(cfg["sla_horas"])
    partes = [
        f"# Resumen del conjunto `{carpeta.name}`\n",
        f"- Usuarios: {len(u)} · Equipos: {len(equipos)} (+{len(inv) - len(equipos)} altas duplicadas)"
        f" · Tickets: {len(t)}",
        f"- Periodo: {t['fecha_apertura'].min()[:10]} a {t['fecha_apertura'].max()[:10]}",
        f"- Tickets sin resolver al final del periodo: {(limpios['fecha_cierre'] == '').mean():.1%}\n",
        "## Anomalías inyectadas\n", _tabla(reg["familia"].value_counts(), ("Familia", "Registros")),
        "\n## Distribución por prioridad (tickets válidos)\n",
        _tabla(limpios["prioridad"].value_counts(normalize=True).reindex(orden_p).map("{:.1%}".format),
               ("Prioridad", "%")),
        "\n## Tiempo de resolución y SLA (tickets cerrados válidos)\n",
        "| Prioridad | Mediana (h) | MTTR (h) | % SLA |", "|---|---|---|---|",
    ]
    g = cerrados.groupby("prioridad")
    for p in orden_p:
        partes.append(f"| {p} | {g['horas'].median()[p]:.1f} | {g['horas'].mean()[p]:.1f} | "
                      f"{g['cumple'].mean()[p]:.1%} |")
    partes += ["\n## % SLA por categoría\n",
               _tabla(cerrados.groupby("categoria")["cumple"].mean().sort_values().map("{:.1%}".format),
                      ("Categoría", "% SLA")),
               "\n## Tickets por equipo según modelo (5 mayores y mediana)\n",
               _tabla(tasa.head(5).round(1), ("Modelo", "Tickets/equipo")),
               f"\nMediana de todos los modelos: {tasa.median():.1f}\n"]
    return "\n".join(partes)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Resumen descriptivo de un conjunto generado.")
    parser.add_argument("carpeta")
    parser.add_argument("--config", default="src/generador/config.yaml")
    parser.add_argument("--salida")
    args = parser.parse_args(argv)
    texto = resumir(Path(args.carpeta), cargar_config(args.config))
    if args.salida:
        Path(args.salida).parent.mkdir(parents=True, exist_ok=True)
        Path(args.salida).write_text(texto, encoding="utf-8")
    print(texto)


if __name__ == "__main__":
    main()
