"""Pruebas del registro de disponibilidad de indicadores (iteración 3)."""

import os
from datetime import datetime, timezone

import pandas as pd
import pytest

from src.evaluacion.disponibilidad import COLUMNAS, INDICADORES, guardar, indicadores_disponibles, medir

LLEGADA = "2026-11-07T10:00:00.000+00:00"
DETECTADO = "2026-11-07T10:00:30.000+00:00"


@pytest.fixture
def fichero_kpi(tmp_path):
    f = tmp_path / "kpi_resumen.parquet"
    pd.DataFrame({"kpi": INDICADORES, "valor": [1.0] * len(INDICADORES)}).to_parquet(f)
    t = datetime(2026, 11, 7, 10, 1, 30, tzinfo=timezone.utc).timestamp()
    os.utime(f, (t, t))
    return f


def test_latencia_desde_la_llegada(fichero_kpi):
    r = medir("r1", 1000, LLEGADA, DETECTADO, fichero_kpi)
    assert r["espera_s"] == 30
    assert r["proceso_s"] == 60
    assert r["latencia_s"] == 90
    assert r["espera_s"] + r["proceso_s"] == r["latencia_s"]


def test_ejecucion_forzada_mide_desde_la_deteccion(fichero_kpi):
    r = medir("r2", 1000, "2026-01-01T00:00:00.000+00:00", DETECTADO, fichero_kpi, forzado=True)
    assert r["forzado"] is True
    assert r["latencia_s"] == 60


def test_todos_los_indicadores_disponibles(fichero_kpi):
    r = medir("r3", 1000, LLEGADA, DETECTADO, fichero_kpi)
    assert (r["indicadores_definidos"], r["indicadores_disponibles"], r["proporcion_disponible"]) == (10, 10, 1)


def test_indicador_sin_valor_no_cuenta_como_disponible(tmp_path):
    f = tmp_path / "kpi.parquet"
    pd.DataFrame({"kpi": ["K1", "K2", "K3"], "valor": [1.0, None, 3.0]}).to_parquet(f)
    assert indicadores_disponibles(f) == ["K1", "K3"]


def test_guardar_acumula_filas_con_cabecera(fichero_kpi, tmp_path):
    destino = tmp_path / "t" / "disponibilidad.csv"
    guardar(medir("r4", 1000, LLEGADA, DETECTADO, fichero_kpi), destino)
    guardar(medir("r5", 1000, LLEGADA, DETECTADO, fichero_kpi), destino)
    df = pd.read_csv(destino)
    assert list(df.columns) == COLUMNAS
    assert list(df["run_id"]) == ["r4", "r5"]
