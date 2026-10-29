"""Pruebas del pipeline Spark (iteración 2) con un conjunto pequeño de resultado conocido.

Requieren Java y PySpark: se ejecutan dentro del contenedor.
"""

import shutil
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("pyspark")

from src.evaluacion.deteccion import evaluar  # noqa: E402
from src.generador.generar import cargar_config, generar  # noqa: E402
from src.pipeline.ejecutar import ejecutar  # noqa: E402

RAIZ = Path(__file__).resolve().parents[1]
N = 2000


@pytest.fixture(scope="module")
def proyecto(tmp_path_factory):
    base = tmp_path_factory.mktemp("proyecto")
    (base / "src" / "generador").mkdir(parents=True)
    shutil.copy(RAIZ / "src" / "generador" / "config.yaml", base / "src" / "generador" / "config.yaml")
    cfg = cargar_config(base / "src" / "generador" / "config.yaml")
    generar(cfg, N, base / "data" / "raw")
    resultado = ejecutar(N, base=base, run_id="prueba")
    return base, cfg, resultado


def _raw(base, f):
    return pd.read_csv(base / "data" / "raw" / f"n{N}" / f, dtype=str, keep_default_na=False)


def test_deteccion_completa_sin_falsos_positivos(proyecto):
    base, _, _ = proyecto
    ev = evaluar(N, base)
    assert ev["tasa_deteccion"] == 1.0
    assert ev["falsos_positivos"] == 0
    for familia, d in ev["por_familia"].items():
        assert d["tasa_deteccion"] == 1.0, familia


def test_informe_de_calidad_cuadra(proyecto):
    _, _, res = proyecto
    for tabla, d in res["calidad"]["tablas"].items():
        assert d["entrada"] == d["validos"] + d["rechazados"], tabla
        assert d["rechazados"] == sum(d["por_regla"].values()), tabla


def test_capa_analitica_un_fichero_por_tabla(proyecto):
    base, _, res = proyecto
    carpeta = base / "data" / "analytics" / f"n{N}"
    for tabla in ("fact_tickets", "dim_equipo", "dim_usuario", "dim_fecha", "kpi_resumen", "calidad_resumen"):
        assert (carpeta / f"{tabla}.parquet").is_file(), tabla
    assert res["analitica"]["fact_tickets"] == res["calidad"]["tablas"]["tickets"]["validos"]


def test_kpis_coinciden_con_calculo_independiente(proyecto):
    """Mismo resultado que obtendría el procedimiento manual sobre los datos limpios."""
    base, cfg, _ = proyecto
    reg = _raw(base, "anomalias_inyectadas.csv")
    t = _raw(base, "tickets.csv")
    t = t[~t["ticket_id"].isin(reg[reg["tabla"] == "tickets"]["clave"])].copy()
    inv = _raw(base, "inventario.csv").drop_duplicates("equipo_id", keep="first")
    k = pd.read_parquet(base / "data" / "analytics" / f"n{N}" / "kpi_resumen.parquet")
    kpi = lambda cod, dim="total", cat="total": k[(k.kpi == cod) & (k.dimension == dim) & (k.categoria == cat)]["valor"].item()

    cerr = t[t["fecha_cierre"] != ""].copy()
    cerr["h"] = (pd.to_datetime(cerr["fecha_cierre"]) - pd.to_datetime(cerr["fecha_apertura"])).dt.total_seconds() / 3600
    cerr["ok"] = cerr["h"] <= cerr["prioridad"].map(cfg["sla_horas"])

    assert kpi("K1") == len(t)
    assert kpi("K2") == len(cerr)
    assert kpi("K3") == len(t) - len(cerr)
    assert kpi("K4") == pytest.approx(cerr["h"].mean(), abs=1e-3)
    assert kpi("K5") == pytest.approx(cerr["ok"].mean() * 100, abs=1e-3)
    for p, g in cerr.groupby("prioridad"):
        assert kpi("K5", "prioridad", p) == pytest.approx(g["ok"].mean() * 100, abs=1e-3)
    for estado, cnt in inv["estado"].value_counts().items():
        assert kpi("K6", "estado", estado) == cnt
    corte = pd.Timestamp(cfg["periodo"]["fin"])
    activos = inv[inv["estado"] != "Baja"]
    assert kpi("K8") == (pd.to_datetime(activos["fecha_fin_garantia"]) < corte).sum()
    modelo = t["equipo_id"].map(inv.set_index("equipo_id")["modelo"])
    tasa = t.groupby(modelo).size() / inv.groupby("modelo").size()
    assert kpi("K9", "modelo", "ProBook 450") == pytest.approx(tasa["ProBook 450"], abs=1e-3)


def test_metricas_de_ejecucion(proyecto):
    base, _, _ = proyecto
    m = pd.read_csv(base / "resultados" / "tiempos" / "escenario_A_pasos.csv")
    assert set(m["run_id"]) == {"prueba"}
    assert {"sesion_spark", "curar", "analitica", "validar_tickets", "calcular_kpis"} <= set(m["paso"])
    assert (m["segundos"] >= 0).all()
