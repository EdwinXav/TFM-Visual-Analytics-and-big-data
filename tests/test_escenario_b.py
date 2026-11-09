"""Pruebas de la comparación del escenario B (Excel) con la referencia."""

import pandas as pd
import pytest

pytest.importorskip("pyspark")  # deteccion.py importa las rutas del pipeline

from src.evaluacion.escenario_b import comparar_kpi, leer_kpi_excel, leer_rechazados  # noqa: E402


def test_lee_csv_con_formato_regional_de_excel(tmp_path):
    f = tmp_path / "kpi.csv"
    f.write_text("kpi;dimension;categoria;valor\nK1;total;total;960\nK4;total;total;16,42\n", encoding="latin-1")
    k = leer_kpi_excel(f)
    assert k["valor"].tolist() == [960, 16.42]


def test_traduce_motivos_a_familias(tmp_path):
    f = tmp_path / "r.csv"
    f.write_text("tabla;clave;motivo\ntickets;T1;completitud\ninventario;E1;unicidad\n", encoding="latin-1")
    assert leer_rechazados(f)["familia"].tolist() == ["nulos", "duplicados"]


def _kpi(valor):
    return pd.DataFrame([{"kpi": "K4", "dimension": "total", "categoria": "total", "valor": valor}])


def test_diferencia_dentro_de_la_tolerancia_coincide():
    r = comparar_kpi(_kpi(16.4218), _kpi(16.4221))
    assert r["coincidentes"] == 1 and not r["distintos"]


def test_diferencia_real_se_informa():
    r = comparar_kpi(_kpi(16.0), _kpi(17.0))
    assert r["coincidentes"] == 0 and len(r["distintos"]) == 1


def test_indicador_ausente_se_informa():
    falta = pd.DataFrame([{"kpi": "K9", "dimension": "modelo", "categoria": "P2423", "valor": 1.0}])
    r = comparar_kpi(_kpi(1.0), falta)
    assert len(r["sin_emparejar"]) == 2


def test_categoria_sin_registros_no_es_discrepancia():
    excel = pd.DataFrame([
        {"kpi": "K4", "dimension": "total", "categoria": "total", "valor": 1.0},
        {"kpi": "K9", "dimension": "modelo", "categoria": "P2423", "valor": 0.0},
        {"kpi": "K9", "dimension": "modelo", "categoria": "iPad 10", "valor": float("nan")},
    ])
    r = comparar_kpi(excel, _kpi(1.0))
    assert r["coincidentes"] == 1 and not r["sin_emparejar"] and not r["distintos"]
