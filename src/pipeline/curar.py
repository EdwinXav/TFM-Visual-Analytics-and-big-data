"""Etapas 1-3 del pipeline: ingesta, validación de calidad y limpieza (raw -> curated + quarantine).

Cada registro se clasifica con la primera regla que incumple. Los válidos se
tipan y se escriben en la capa curated; los rechazados, con su motivo, en la
zona de cuarentena. Se genera además un informe de calidad por tabla y regla.
"""

from __future__ import annotations

from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.pipeline.comun import Metricas, escribir_parquet, escribir_parquet_unico, guardar_json, rutas

COLUMNAS = {
    "usuarios": ["usuario_id", "nombre", "departamento", "ubicacion", "rol"],
    "inventario": ["equipo_id", "tipo", "marca", "modelo", "fecha_adquisicion",
                   "fecha_fin_garantia", "estado", "ubicacion", "usuario_id"],
    "tickets": ["ticket_id", "fecha_apertura", "fecha_cierre", "categoria", "prioridad",
                "estado", "equipo_id", "usuario_id", "tecnico"],
}
# Reglas en orden de evaluación; cada una corresponde a una familia de anomalías
REGLAS = ["completitud", "validez", "consistencia_temporal", "unicidad", "integridad_referencial"]
ESTADOS_CERRADOS = ["Resuelto", "Cerrado"]
ESTADOS_ABIERTOS = ["Abierto", "En curso"]
FMT_FECHA, FMT_FECHA_HORA = "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss"


def leer_csv(spark: SparkSession, fichero: Path, columnas: list[str]) -> DataFrame:
    """Ingesta con esquema explícito: todo como texto para validar antes de tipar."""
    esquema = StructType([StructField(c, StringType(), True) for c in columnas])
    df = spark.read.csv(str(fichero), header=True, schema=esquema, mode="PERMISSIVE", encoding="UTF-8")
    # Orden de lectura, para conservar la primera aparición en la regla de unicidad
    return df.withColumn("_fila", F.monotonically_increasing_id())


def _falta(c: str):
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


def _alguno(condiciones):
    return reduce(lambda a, b: a | b, condiciones)


def _clasificar(df: DataFrame, condiciones: dict) -> DataFrame:
    """Añade la columna `motivo` con la primera regla incumplida (null si es válido)."""
    expr = None
    for regla in REGLAS:
        if regla in condiciones:
            cond = F.coalesce(condiciones[regla], F.lit(False))
            expr = F.when(cond, F.lit(regla)) if expr is None else expr.when(cond, F.lit(regla))
    return df.withColumn("motivo", expr.otherwise(F.lit(None)))


def _duplicado(clave: str):
    return F.row_number().over(Window.partitionBy(clave).orderBy("_fila")) > 1


def validar_usuarios(df: DataFrame, cfg: dict) -> DataFrame:
    cat = cfg["catalogos"]
    return _clasificar(df, {
        "completitud": _alguno([_falta(c) for c in ["usuario_id", "departamento", "ubicacion", "rol"]]),
        "validez": ~F.col("departamento").isin(cat["departamento"]) | ~F.col("ubicacion").isin(cat["ubicacion"])
        | ~F.col("rol").isin(cat["rol"]),
        "unicidad": _duplicado("usuario_id"),
    })


def validar_inventario(df: DataFrame, cfg: dict, ids_usuarios: DataFrame) -> DataFrame:
    cat = cfg["catalogos"]
    modelos_validos = [f"{m['marca']}|{m['modelo']}|{m['tipo']}" for m in cat["modelos"]]
    adq = F.to_date("fecha_adquisicion", FMT_FECHA)
    gar = F.to_date("fecha_fin_garantia", FMT_FECHA)
    df = df.join(F.broadcast(ids_usuarios.withColumnRenamed("usuario_id", "_u").withColumn("_u_ok", F.lit(True))),
                 F.col("usuario_id") == F.col("_u"), "left")
    df = _clasificar(df, {
        "completitud": _alguno([_falta(c) for c in COLUMNAS["inventario"] if c != "usuario_id"]),
        "validez": ~F.concat_ws("|", "marca", "modelo", "tipo").isin(modelos_validos)
        | ~F.col("estado").isin(cat["estado_equipo"]) | ~F.col("ubicacion").isin(cat["ubicacion"])
        | adq.isNull() | gar.isNull(),
        "consistencia_temporal": gar <= adq,
        "unicidad": _duplicado("equipo_id"),
        "integridad_referencial": ~_falta("usuario_id") & F.col("_u_ok").isNull(),
    })
    return df.drop("_u", "_u_ok")


def validar_tickets(df: DataFrame, cfg: dict, ids_equipos: DataFrame, ids_usuarios: DataFrame) -> DataFrame:
    cat = cfg["catalogos"]
    tecnicos = [f"T{i:02d}" for i in range(1, cat["num_tecnicos"] + 1)]
    ap = F.to_timestamp("fecha_apertura", FMT_FECHA_HORA)
    ci = F.to_timestamp("fecha_cierre", FMT_FECHA_HORA)
    df = (df.join(F.broadcast(ids_equipos.withColumnRenamed("equipo_id", "_e").withColumn("_e_ok", F.lit(True))),
                  F.col("equipo_id") == F.col("_e"), "left")
            .join(F.broadcast(ids_usuarios.withColumnRenamed("usuario_id", "_u").withColumn("_u_ok", F.lit(True))),
                  F.col("usuario_id") == F.col("_u"), "left"))
    obligatorios = [c for c in COLUMNAS["tickets"] if c != "fecha_cierre"]
    df = _clasificar(df, {
        "completitud": _alguno([_falta(c) for c in obligatorios])
        | (F.col("estado").isin(ESTADOS_CERRADOS) & _falta("fecha_cierre")),
        "validez": ~F.col("prioridad").isin(cat["prioridad"]) | ~F.col("categoria").isin(cat["categoria"])
        | ~F.col("estado").isin(cat["estado_ticket"]) | ~F.col("tecnico").isin(tecnicos)
        | ap.isNull() | (~_falta("fecha_cierre") & ci.isNull()),
        "consistencia_temporal": (ci < ap) | (F.col("estado").isin(ESTADOS_ABIERTOS) & ~_falta("fecha_cierre")),
        "unicidad": _duplicado("ticket_id"),
        "integridad_referencial": F.col("_e_ok").isNull() | F.col("_u_ok").isNull(),
    })
    return df.drop("_e", "_e_ok", "_u", "_u_ok")


def _tipar(tabla: str, df: DataFrame) -> DataFrame:
    if tabla == "inventario":
        df = (df.withColumn("fecha_adquisicion", F.to_date("fecha_adquisicion", FMT_FECHA))
                .withColumn("fecha_fin_garantia", F.to_date("fecha_fin_garantia", FMT_FECHA))
                .withColumn("usuario_id", F.when(_falta("usuario_id"), None).otherwise(F.col("usuario_id"))))
    elif tabla == "tickets":
        df = (df.withColumn("fecha_apertura", F.to_timestamp("fecha_apertura", FMT_FECHA_HORA))
                .withColumn("fecha_cierre", F.to_timestamp("fecha_cierre", FMT_FECHA_HORA)))
    return df


def curar(spark: SparkSession, cfg: dict, n: int, base: Path, metricas: Metricas) -> dict:
    r = rutas(base, n)
    informe = {"n": n, "run_id": metricas.run_id, "tablas": {}}
    validos: dict[str, DataFrame] = {}

    def procesar(tabla: str, clasificado: DataFrame):
        clasificado = clasificado.cache()
        with metricas.paso("curar", f"validar_{tabla}") as p:
            conteo = {row["motivo"]: row["count"] for row in clasificado.groupBy("motivo").count().collect()}
            p["registros"] = sum(conteo.values())
        ok = _tipar(tabla, clasificado.filter(F.col("motivo").isNull()).drop("motivo")).orderBy("_fila").drop("_fila")
        ko = (clasificado.filter(F.col("motivo").isNotNull()).orderBy("_fila").drop("_fila")
              .withColumn("tabla", F.lit(tabla)))
        with metricas.paso("curar", f"escribir_{tabla}") as p:
            escribir_parquet(ok, r["curated"] / tabla)
            escribir_parquet(ko, r["quarantine"] / tabla)
            p["registros"] = sum(conteo.values())
        rechazados = {k: v for k, v in conteo.items() if k is not None}
        informe["tablas"][tabla] = {
            "entrada": sum(conteo.values()), "validos": conteo.get(None, 0),
            "rechazados": sum(rechazados.values()), "por_regla": {regla: rechazados.get(regla, 0) for regla in REGLAS},
        }
        validos[tabla] = spark.read.parquet(str(r["curated"] / tabla))
        clasificado.unpersist()

    with metricas.paso("curar", "ingesta") as p:
        crudos = {t: leer_csv(spark, r["raw"] / f"{t}.csv", cols) for t, cols in COLUMNAS.items()}
        p["registros"] = 3  # ficheros; la lectura es perezosa y se materializa al validar

    procesar("usuarios", validar_usuarios(crudos["usuarios"], cfg))
    ids_u = validos["usuarios"].select("usuario_id").distinct()
    procesar("inventario", validar_inventario(crudos["inventario"], cfg, ids_u))
    ids_e = validos["inventario"].select("equipo_id").distinct()
    procesar("tickets", validar_tickets(crudos["tickets"], cfg, ids_e, ids_u))

    # Resumen de calidad: fichero del informe y tabla para la página de calidad del dashboard
    guardar_json(informe, r["calidad"] / f"calidad_{metricas.run_id}.json")
    guardar_json(informe, r["calidad"] / "calidad_ultima.json")
    filas = [(t, regla, d["por_regla"][regla], d["entrada"]) for t, d in informe["tablas"].items() for regla in REGLAS]
    resumen = spark.createDataFrame(filas, "tabla string, regla string, registros long, entrada long")
    escribir_parquet_unico(resumen, r["analytics"] / "calidad_resumen.parquet")
    return informe
