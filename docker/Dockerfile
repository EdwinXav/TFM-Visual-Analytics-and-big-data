# Imagen única: Airflow + Java + PySpark (Spark en modo local[*]).
# Se evita un clúster Spark separado para mantener el entorno sencillo y
# reproducible en un portátil; ver CONTEXTO.md (decisiones de diseño).
FROM apache/airflow:3.3.2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY requirements.txt /tmp/requirements.txt
# Se reinstala la misma versión de Airflow para que pip no la cambie al
# resolver las dependencias del proyecto.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /tmp/requirements.txt
