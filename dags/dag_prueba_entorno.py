"""DAG de humo (iteración 0): comprueba que Airflow puede lanzar Spark.

Crea una sesión de Spark en modo local, construye un DataFrame pequeño,
lo escribe en Parquet y lo vuelve a leer. No forma parte del pipeline.
"""

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="prueba_entorno",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tfm", "iteracion-0"],
)
def prueba_entorno():
    @task
    def spark_local() -> int:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[*]")
            .appName("prueba_entorno")
            .getOrCreate()
        )
        try:
            df = spark.createDataFrame(
                [("T0000001", "Alta"), ("T0000002", "Media")],
                ["ticket_id", "prioridad"],
            )
            ruta = "/tmp/prueba_entorno.parquet"
            df.write.mode("overwrite").parquet(ruta)
            n = spark.read.parquet(ruta).count()
            print(f"Spark {spark.version}: {n} registros leídos de Parquet")
            assert n == 2
            return n
        finally:
            spark.stop()

    spark_local()


prueba_entorno()
