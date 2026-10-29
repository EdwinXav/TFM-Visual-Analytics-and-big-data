"""Utilidades comunes del pipeline: rutas, sesión de Spark, escritura y métricas."""

from __future__ import annotations

import csv
import json
import os
import shutil
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession

CONFIG_RELATIVA = Path("src/generador/config.yaml")


def base_dir() -> Path:
    """Raíz del proyecto (en el contenedor, /opt/proyecto vía TFM_DIR)."""
    return Path(os.environ.get("TFM_DIR", Path.cwd()))


def cargar_config(base: Path) -> dict:
    with open(base / CONFIG_RELATIVA, encoding="utf-8") as f:
        return yaml.safe_load(f)


def rutas(base: Path, n: int) -> dict[str, Path]:
    conjunto = f"n{n}"
    return {
        "raw": base / "data" / "raw" / conjunto,
        "curated": base / "data" / "curated" / conjunto,
        "quarantine": base / "data" / "quarantine" / conjunto,
        "analytics": base / "data" / "analytics" / conjunto,
        "calidad": base / "resultados" / "calidad" / conjunto,
        "tiempos": base / "resultados" / "tiempos",
    }


def crear_sesion(app: str = "tfm-pipeline") -> SparkSession:
    return (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_PARTICIONES", "8"))
        .config("spark.driver.memory", os.environ.get("SPARK_MEMORIA", "2g"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def escribir_parquet(df: DataFrame, ruta: Path) -> None:
    """Escribe un directorio Parquet (capas curated y quarantine)."""
    df.write.mode("overwrite").parquet(str(ruta))


def escribir_parquet_unico(df: DataFrame, fichero: Path) -> None:
    """Escribe un único fichero Parquet con nombre fijo (capa analítica, para Power BI)."""
    tmp = fichero.with_name(f"_tmp_{fichero.stem}")
    df.coalesce(1).write.mode("overwrite").parquet(str(tmp))
    parte = next(tmp.glob("part-*.parquet"))
    fichero.parent.mkdir(parents=True, exist_ok=True)
    if fichero.exists():
        fichero.unlink()
    shutil.move(str(parte), str(fichero))
    shutil.rmtree(tmp)


def guardar_json(datos: dict, fichero: Path) -> None:
    fichero.parent.mkdir(parents=True, exist_ok=True)
    fichero.write_text(json.dumps(datos, ensure_ascii=False, indent=2), encoding="utf-8")


def ahora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Metricas:
    """Registra la duración y los registros de cada paso; fuente de la evaluación (escenario A)."""

    COLUMNAS = ["run_id", "n", "etapa", "paso", "inicio_utc", "fin_utc", "segundos", "registros"]

    def __init__(self, run_id: str, n: int, base: Path):
        self.run_id, self.n = run_id, n
        self.fichero = rutas(base, n)["tiempos"] / "escenario_A_pasos.csv"
        self.filas: list[dict] = []

    @contextmanager
    def paso(self, etapa: str, nombre: str):
        info = {"registros": ""}
        inicio, t0 = ahora_iso(), time.perf_counter()
        yield info
        self.filas.append({
            "run_id": self.run_id, "n": self.n, "etapa": etapa, "paso": nombre,
            "inicio_utc": inicio, "fin_utc": ahora_iso(),
            "segundos": round(time.perf_counter() - t0, 3), "registros": info["registros"],
        })

    def guardar(self) -> None:
        self.fichero.parent.mkdir(parents=True, exist_ok=True)
        nuevo = not self.fichero.exists()
        with open(self.fichero, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=self.COLUMNAS)
            if nuevo:
                w.writeheader()
            w.writerows(self.filas)
        self.filas = []
