"""Etapa 4 del pipeline: modelo en estrella y cálculo de indicadores (curated -> analytics).

Tablas (un fichero Parquet cada una, para Power BI):
    fact_tickets, dim_equipo, dim_usuario, dim_fecha, kpi_resumen
Las definiciones de los KPI están en docs/indicadores.md.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.pipeline.comun import Metricas, escribir_parquet_unico, rutas


def _clave_fecha(col):
    return F.date_format(col, "yyyyMMdd").cast("int")


def construir_modelo(spark: SparkSession, cfg: dict, cur: dict[str, DataFrame]) -> dict[str, DataFrame]:
    corte = F.lit(cfg["periodo"]["fin"]).cast("date")
    sla = F.create_map(*[x for p, h in cfg["sla_horas"].items() for x in (F.lit(p), F.lit(float(h)))])

    dim_usuario = cur["usuarios"].select("usuario_id", "departamento", "ubicacion", "rol")

    dim_equipo = cur["inventario"].select(
        "equipo_id", "tipo", "marca", "modelo", "fecha_adquisicion", "fecha_fin_garantia", "estado",
        "ubicacion", "usuario_id",
        F.round(F.datediff(corte, "fecha_adquisicion") / 365.25, 2).alias("antiguedad_anios"),
        (F.col("fecha_fin_garantia") < corte).alias("garantia_vencida"),
        (F.col("estado") != "Baja").alias("activo"),
    )

    t = cur["tickets"]
    horas = (F.col("fecha_cierre").cast("long") - F.col("fecha_apertura").cast("long")) / 3600.0
    fact = t.select(
        "ticket_id", "fecha_apertura", "fecha_cierre",
        _clave_fecha("fecha_apertura").alias("fecha_apertura_key"),
        _clave_fecha("fecha_cierre").alias("fecha_cierre_key"),
        "equipo_id", "usuario_id", "categoria", "prioridad", "estado", "tecnico",
        F.round(horas, 4).alias("horas_resolucion"),
        sla[F.col("prioridad")].alias("sla_horas"),
        F.col("fecha_cierre").isNull().alias("abierto"),
    ).withColumn("cumple_sla", F.when(F.col("abierto"), None)
                 .otherwise(F.col("horas_resolucion") <= F.col("sla_horas")))

    inicio, fin = cfg["periodo"]["inicio"], cfg["periodo"]["fin"]
    dim_fecha = (spark.sql(f"SELECT explode(sequence(to_date('{inicio}'), to_date('{fin}'), interval 1 day)) AS fecha")
                 .select(_clave_fecha("fecha").alias("fecha_key"), "fecha",
                         F.year("fecha").alias("anio"), F.quarter("fecha").alias("trimestre"),
                         F.month("fecha").alias("mes"), F.weekofyear("fecha").alias("semana"),
                         F.dayofweek("fecha").alias("dia_semana")))
    return {"fact_tickets": fact, "dim_equipo": dim_equipo, "dim_usuario": dim_usuario, "dim_fecha": dim_fecha}


def calcular_kpis(modelo: dict[str, DataFrame]) -> DataFrame:
    """KPI K1-K10 en formato largo: kpi, indicador, dimension, categoria, valor."""
    f, e, u = modelo["fact_tickets"], modelo["dim_equipo"], modelo["dim_usuario"]
    cerrados = f.filter(~F.col("abierto"))
    activos = e.filter(F.col("activo"))
    partes = []

    def total(kpi, nombre, df, expr):
        partes.append(df.agg(expr.cast("double").alias("valor"))
                      .select(F.lit(kpi).alias("kpi"), F.lit(nombre).alias("indicador"),
                              F.lit("total").alias("dimension"), F.lit("total").alias("categoria"), "valor"))

    def por(kpi, nombre, df, dim, expr):
        partes.append(df.groupBy(dim).agg(expr.cast("double").alias("valor"))
                      .select(F.lit(kpi).alias("kpi"), F.lit(nombre).alias("indicador"),
                              F.lit(dim).alias("dimension"), F.col(dim).cast("string").alias("categoria"), "valor"))

    total("K1", "Tickets abiertos en el periodo", f, F.count("*"))
    total("K2", "Tickets cerrados en el periodo", cerrados, F.count("*"))
    total("K3", "Backlog al corte", f.filter("abierto"), F.count("*"))
    total("K4", "MTTR (horas)", cerrados, F.avg("horas_resolucion"))
    por("K4", "MTTR (horas)", cerrados, "prioridad", F.avg("horas_resolucion"))
    sla_pct = F.avg(F.col("cumple_sla").cast("int")) * 100
    total("K5", "% cumplimiento SLA", cerrados, sla_pct)
    por("K5", "% cumplimiento SLA", cerrados, "prioridad", sla_pct)
    por("K6", "Equipos por estado", e, "estado", F.count("*"))
    total("K7", "Antigüedad media (años)", activos, F.avg("antiguedad_anios"))
    total("K8", "Garantías vencidas", activos.filter("garantia_vencida"), F.count("*"))
    tickets_modelo = f.join(e.select("equipo_id", "modelo"), "equipo_id").groupBy("modelo").count()
    equipos_modelo = e.groupBy("modelo").agg(F.count("*").alias("equipos"))
    partes.append(tickets_modelo.join(equipos_modelo, "modelo")
                  .select(F.lit("K9").alias("kpi"), F.lit("Tickets por equipo según modelo").alias("indicador"),
                          F.lit("modelo").alias("dimension"), F.col("modelo").alias("categoria"),
                          (F.col("count") / F.col("equipos")).alias("valor")))
    fu = f.join(u.select("usuario_id", "departamento", "ubicacion"), "usuario_id")
    por("K10", "Tickets por departamento", fu, "departamento", F.count("*"))
    por("K10", "Tickets por ubicación", fu, "ubicacion", F.count("*"))

    kpis = partes[0]
    for p in partes[1:]:
        kpis = kpis.unionByName(p)
    return kpis.withColumn("valor", F.round("valor", 4)).orderBy("kpi", "dimension", "categoria")


def analitica(spark: SparkSession, cfg: dict, n: int, base: Path, metricas: Metricas) -> dict:
    r = rutas(base, n)
    with metricas.paso("analitica", "lectura_curated"):
        cur = {t: spark.read.parquet(str(r["curated"] / t)) for t in ("usuarios", "inventario", "tickets")}
    modelo = construir_modelo(spark, cfg, cur)
    recuentos = {}
    for nombre, df in modelo.items():
        with metricas.paso("analitica", f"escribir_{nombre}") as p:
            escribir_parquet_unico(df, r["analytics"] / f"{nombre}.parquet")
            recuentos[nombre] = p["registros"] = spark.read.parquet(str(r["analytics"] / f"{nombre}.parquet")).count()
    # Los KPI se calculan sobre el modelo ya escrito (lo que verá el dashboard)
    escrito = {k: spark.read.parquet(str(r["analytics"] / f"{k}.parquet")) for k in modelo}
    with metricas.paso("analitica", "calcular_kpis") as p:
        kpis = calcular_kpis(escrito)
        escribir_parquet_unico(kpis, r["analytics"] / "kpi_resumen.parquet")
        recuentos["kpi_resumen"] = p["registros"] = spark.read.parquet(str(r["analytics"] / "kpi_resumen.parquet")).count()
    return recuentos
