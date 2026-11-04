"""Pruebas del generador de datos simulados (iteración 1)."""

import hashlib
from pathlib import Path

import pandas as pd
import pytest

from src.generador.generar import cargar_config, generar

RAIZ = Path(__file__).resolve().parents[1]
CONFIG = cargar_config(RAIZ / "src" / "generador" / "config.yaml")
N = 2000
FICHEROS = ["usuarios.csv", "inventario.csv", "tickets.csv", "anomalias_inyectadas.csv"]


def _leer(carpeta: Path) -> dict[str, pd.DataFrame]:
    return {f[:-4]: pd.read_csv(carpeta / f, dtype=str, keep_default_na=False) for f in FICHEROS}


def _hash(carpeta: Path) -> dict[str, str]:
    return {f: hashlib.sha256((carpeta / f).read_bytes()).hexdigest() for f in FICHEROS}


@pytest.fixture(scope="module")
def conjunto(tmp_path_factory):
    carpeta = generar(CONFIG, N, tmp_path_factory.mktemp("a"))
    return carpeta, _leer(carpeta)


def violaciones(d: dict[str, pd.DataFrame]) -> dict[str, set]:
    """Recalcula de forma independiente qué registros incumplen cada regla."""
    cat = CONFIG["catalogos"]
    t, inv, u = d["tickets"], d["inventario"], d["usuarios"]
    obligatorios = ["ticket_id", "fecha_apertura", "categoria", "prioridad", "estado",
                    "equipo_id", "usuario_id", "tecnico"]
    nulos = t[(t[obligatorios] == "").any(axis=1)]["ticket_id"]

    equipos, usuarios = set(inv["equipo_id"]), set(u["usuario_id"])
    huerf = t[((t["equipo_id"] != "") & ~t["equipo_id"].isin(equipos))
              | ((t["usuario_id"] != "") & ~t["usuario_id"].isin(usuarios))]["ticket_id"]

    con_cierre = t[t["fecha_cierre"] != ""]
    fechas = con_cierre[pd.to_datetime(con_cierre["fecha_cierre"])
                        < pd.to_datetime(con_cierre["fecha_apertura"])]["ticket_id"]

    fuera = t[((t["prioridad"] != "") & ~t["prioridad"].isin(cat["prioridad"]))
              | ((t["categoria"] != "") & ~t["categoria"].isin(cat["categoria"]))
              | ~t["estado"].isin(cat["estado_ticket"])]["ticket_id"]

    dup = inv[inv["equipo_id"].duplicated(keep="first")]["equipo_id"]
    return {"nulos": set(nulos), "huerfanos": set(huerf), "fechas_incoherentes": set(fechas),
            "fuera_de_catalogo": set(fuera), "duplicados": list(dup)}


def test_reproducible(conjunto, tmp_path):
    carpeta, _ = conjunto
    otra = generar(CONFIG, N, tmp_path)
    assert _hash(carpeta) == _hash(otra)


def test_semilla_distinta_cambia_los_datos(conjunto, tmp_path):
    carpeta, _ = conjunto
    otra = generar(CONFIG, N, tmp_path, semilla=CONFIG["semilla"] + 1)
    assert _hash(carpeta)["tickets.csv"] != _hash(otra)["tickets.csv"]


def test_tamanos(conjunto):
    _, d = conjunto
    n_u = round(N * CONFIG["proporcion_usuarios_por_ticket"])
    n_e = round(n_u * CONFIG["equipos_por_usuario"])
    assert len(d["tickets"]) == N
    assert len(d["usuarios"]) == n_u
    assert len(d["inventario"]) == n_e + round(CONFIG["anomalias"]["duplicados"]["proporcion"] * n_e)


def test_recuento_de_anomalias(conjunto):
    _, d = conjunto
    reg = d["anomalias_inyectadas"]
    for familia, spec in CONFIG["anomalias"].items():
        n_tabla = len(d["tickets"]) if spec["tabla"] == "tickets" else \
            len(d["inventario"]) - (reg["familia"] == "duplicados").sum()
        assert (reg["familia"] == familia).sum() == max(1, round(spec["proporcion"] * n_tabla)), familia
    assert reg["anomalia_id"].is_unique


def test_un_registro_como_maximo_una_anomalia(conjunto):
    _, d = conjunto
    reg = d["anomalias_inyectadas"]
    assert reg[reg["tabla"] == "tickets"]["clave"].is_unique


def test_las_unicas_violaciones_son_las_registradas(conjunto):
    """Clave para la evaluación: tasa de detección = detectadas / inyectadas."""
    _, d = conjunto
    reg = d["anomalias_inyectadas"]
    v = violaciones(d)
    for familia in ("nulos", "huerfanos", "fechas_incoherentes", "fuera_de_catalogo"):
        esperadas = set(reg[reg["familia"] == familia]["clave"])
        assert v[familia] == esperadas, familia
    assert sorted(v["duplicados"]) == sorted(reg[reg["familia"] == "duplicados"]["clave"])


def test_coherencia_estado_y_fecha_cierre(conjunto):
    _, d = conjunto
    t = d["tickets"]
    validos = t[~t["ticket_id"].isin(d["anomalias_inyectadas"]["clave"])]
    abiertos = validos["estado"].isin(["Abierto", "En curso"])
    assert (validos.loc[abiertos, "fecha_cierre"] == "").all()
    assert (validos.loc[~abiertos, "fecha_cierre"] != "").all()
    prop = abiertos.mean()
    assert abs(prop - CONFIG["proporcion_tickets_abiertos"]) < 0.02


def test_modelo_y_tipo_coherentes(conjunto):
    _, d = conjunto
    modelos = {(m["marca"], m["modelo"]): m["tipo"] for m in CONFIG["catalogos"]["modelos"]}
    inv = d["inventario"]
    assert all(modelos[(r.marca, r.modelo)] == r.tipo for r in inv.itertuples())


def test_patrones_plausibles(conjunto):
    _, d = conjunto
    t = d["tickets"]
    # La prioridad Media es la más frecuente
    assert t["prioridad"].value_counts().idxmax() == "Media"
    # Las críticas se resuelven antes que las bajas (mediana de horas)
    c = t[(t["fecha_cierre"] != "") & ~t["ticket_id"].isin(d["anomalias_inyectadas"]["clave"])].copy()
    c["h"] = (pd.to_datetime(c["fecha_cierre"]) - pd.to_datetime(c["fecha_apertura"])).dt.total_seconds() / 3600
    med = c.groupby("prioridad")["h"].median()
    assert med["Critica"] < med["Alta"] < med["Media"] < med["Baja"]
    # Los modelos problemáticos tienen más tickets por equipo que la media
    inv = d["inventario"].drop_duplicates("equipo_id")
    tasa = t.groupby(t["equipo_id"].map(inv.set_index("equipo_id")["modelo"])).size() \
        / inv.groupby("modelo").size()
    for modelo in CONFIG["modelos_problematicos"]:
        assert tasa[modelo] > tasa.median(), modelo


def test_no_hay_fechas_fuera_de_periodo(conjunto):
    _, d = conjunto
    ap = pd.to_datetime(d["tickets"]["fecha_apertura"])
    assert ap.min() >= pd.Timestamp(CONFIG["periodo"]["inicio"])
    assert ap.max() <= pd.Timestamp(CONFIG["periodo"]["fin"]) + pd.Timedelta(days=1)


def test_marcador_listo(conjunto):
    """El DAG solo procesa un conjunto cuando existe el marcador, escrito al final."""
    import json
    carpeta, d = conjunto
    marcador = carpeta / "_LISTO"
    assert marcador.is_file()
    assert json.loads(marcador.read_text(encoding="utf-8"))["tickets"] == len(d["tickets"])
    assert all(marcador.stat().st_mtime >= (carpeta / f).stat().st_mtime for f in FICHEROS)
