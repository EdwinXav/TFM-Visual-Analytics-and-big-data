"""Generador de datos simulados de inventario, tickets y usuarios.

Produce, para cada tamaño (nº de tickets), una carpeta ``<salida>/n<tamaño>/`` con:

- ``usuarios.csv``, ``inventario.csv``, ``tickets.csv``: fuentes del pipeline.
- ``anomalias_inyectadas.csv``: verdad de referencia con cada anomalía inyectada.
- ``_LISTO``: marcador que se escribe al final; indica al DAG que el conjunto está completo.

Todo es reproducible: misma configuración y semilla -> ficheros idénticos.

Uso:
    python -m src.generador.generar --config src/generador/config.yaml
    python -m src.generador.generar --config src/generador/config.yaml --tamanos 1000 10000
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from faker import Faker

log = logging.getLogger("generador")

FORMATO_FECHA = "%Y-%m-%d"
FORMATO_FECHA_HORA = "%Y-%m-%d %H:%M:%S"
CAMPOS_TICKETS = ["ticket_id", "fecha_apertura", "fecha_cierre", "categoria", "prioridad",
                  "estado", "equipo_id", "usuario_id", "tecnico"]
CAMPOS_INVENTARIO = ["equipo_id", "tipo", "marca", "modelo", "fecha_adquisicion",
                     "fecha_fin_garantia", "estado", "ubicacion", "usuario_id"]
CAMPOS_USUARIOS = ["usuario_id", "nombre", "departamento", "ubicacion", "rol"]
MARCADOR_LISTO = "_LISTO"
CAMPOS_REGISTRO = ["anomalia_id", "familia", "tabla", "clave", "campo",
                   "valor_original", "valor_inyectado"]


def cargar_config(ruta: str | Path) -> dict:
    with open(ruta, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _elegir(rng: np.random.Generator, distribucion: dict, n: int) -> np.ndarray:
    """Muestra n valores de un diccionario {valor: probabilidad}."""
    valores = list(distribucion)
    p = np.array([distribucion[v] for v in valores], dtype=float)
    return rng.choice(valores, size=n, p=p / p.sum())


def _ids(prefijo: str, n: int, ancho: int) -> np.ndarray:
    return np.array([f"{prefijo}{i:0{ancho}d}" for i in range(1, n + 1)])


# ---------------------------------------------------------------------------
# Entidades
# ---------------------------------------------------------------------------

def generar_usuarios(cfg: dict, n_tickets: int, rng: np.random.Generator, fake: Faker) -> pd.DataFrame:
    n = max(20, round(n_tickets * cfg["proporcion_usuarios_por_ticket"]))
    cat = cfg["catalogos"]
    return pd.DataFrame({
        "usuario_id": _ids("U", n, 6),
        "nombre": [fake.name() for _ in range(n)],
        "departamento": rng.choice(cat["departamento"], size=n),
        "ubicacion": rng.choice(cat["ubicacion"], size=n),
        "rol": _elegir(rng, cfg["distribucion_rol"], n),
    })


def generar_inventario(cfg: dict, usuarios: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    n = round(len(usuarios) * cfg["equipos_por_usuario"])
    modelos = pd.DataFrame(cfg["catalogos"]["modelos"])
    idx_modelo = rng.integers(0, len(modelos), size=n)

    inicio = pd.Timestamp(cfg["periodo"]["inicio"])
    fin = pd.Timestamp(cfg["periodo"]["fin"])
    desde = inicio - pd.DateOffset(years=cfg["antiguedad_max_equipo_anios"])
    dias = (fin - pd.Timedelta(days=30) - desde).days
    adquisicion = desde + pd.to_timedelta(rng.integers(0, dias, size=n), unit="D")
    anios_garantia = rng.choice(cfg["garantia_anios"], size=n)
    fin_garantia = adquisicion + pd.to_timedelta(anios_garantia * 365, unit="D")

    estado = _elegir(rng, cfg["distribucion_estado_equipo"], n)
    asignado = np.isin(estado, ["En uso", "En reparacion"])
    idx_usuario = rng.integers(0, len(usuarios), size=n)
    usuario_id = np.where(asignado, usuarios["usuario_id"].to_numpy()[idx_usuario], "")
    ubicacion = np.where(asignado, usuarios["ubicacion"].to_numpy()[idx_usuario],
                         rng.choice(cfg["catalogos"]["ubicacion"], size=n))

    return pd.DataFrame({
        "equipo_id": _ids("E", n, 6),
        "tipo": modelos["tipo"].to_numpy()[idx_modelo],
        "marca": modelos["marca"].to_numpy()[idx_modelo],
        "modelo": modelos["modelo"].to_numpy()[idx_modelo],
        "fecha_adquisicion": adquisicion,
        "fecha_fin_garantia": fin_garantia,
        "estado": estado,
        "ubicacion": ubicacion,
        "usuario_id": usuario_id,
    })


def generar_tickets(cfg: dict, n: int, inventario: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    # Solo generan tickets los equipos asignados a un usuario; algunos modelos
    # tienen una tasa de incidencias superior (peso multiplicado).
    candidatos = inventario[inventario["usuario_id"] != ""].reset_index(drop=True)
    peso = candidatos["modelo"].map(cfg["modelos_problematicos"]).fillna(1.0).to_numpy()
    idx_equipo = rng.choice(len(candidatos), size=n, p=peso / peso.sum())
    equipos = candidatos.iloc[idx_equipo].reset_index(drop=True)

    inicio = pd.Timestamp(cfg["periodo"]["inicio"])
    fin = pd.Timestamp(cfg["periodo"]["fin"]) + pd.Timedelta(hours=23, minutes=59, seconds=59)
    # Apertura: un día entre max(inicio, adquisición del equipo) y el fin, en horario laboral
    desde = equipos["fecha_adquisicion"].where(equipos["fecha_adquisicion"] > inicio, inicio)
    rango_dias = (fin.normalize() - desde).dt.days.to_numpy()
    dia = desde + pd.to_timedelta(np.floor(rng.random(n) * (rango_dias + 1)), unit="D")
    segundos = rng.integers(8 * 3600, 19 * 3600, size=n)
    apertura = dia + pd.to_timedelta(segundos, unit="s")

    prioridad = _elegir(rng, cfg["distribucion_prioridad"], n)
    categoria = _elegir(rng, cfg["distribucion_categoria"], n)
    mediana = (pd.Series(prioridad).map(cfg["resolucion_mediana_horas"]).to_numpy()
               * pd.Series(categoria).map(cfg["factor_categoria"]).to_numpy())
    horas = rng.lognormal(mean=np.log(mediana), sigma=cfg["resolucion_sigma"])
    cierre = apertura + pd.to_timedelta(np.round(horas * 3600), unit="s")

    # Tickets sin resolver: los que cerrarían después del fin del periodo, más
    # tickets recientes al azar hasta alcanzar la proporción configurada.
    abierto = (cierre > fin).to_numpy().copy()  # pandas 3 devuelve vistas de solo lectura
    objetivo = round(cfg["proporcion_tickets_abiertos"] * n)
    faltan = objetivo - int(abierto.sum())
    if faltan > 0:
        orden = np.argsort(-apertura.to_numpy().astype("int64"), kind="stable")
        recientes = [i for i in orden[: max(len(orden) // 4, faltan)] if not abierto[i]]
        abierto[rng.choice(recientes, size=min(faltan, len(recientes)), replace=False)] = True

    estado = np.where(abierto,
                      _elegir(rng, cfg["distribucion_estado_abierto"], n),
                      _elegir(rng, cfg["distribucion_estado_cerrado"], n))
    n_tec = cfg["catalogos"]["num_tecnicos"]

    tickets = pd.DataFrame({
        "fecha_apertura": apertura,
        "fecha_cierre": cierre.where(~abierto, pd.NaT),
        "categoria": categoria,
        "prioridad": prioridad,
        "estado": estado,
        "equipo_id": equipos["equipo_id"].to_numpy(),
        "usuario_id": equipos["usuario_id"].to_numpy(),
        "tecnico": np.array([f"T{i:02d}" for i in rng.integers(1, n_tec + 1, size=n)]),
    })
    # Los identificadores siguen el orden de apertura, como en una herramienta de ticketing
    tickets = tickets.sort_values("fecha_apertura", kind="stable").reset_index(drop=True)
    tickets.insert(0, "ticket_id", _ids("T", n, 7))
    return tickets


# ---------------------------------------------------------------------------
# Anomalías
# ---------------------------------------------------------------------------

def _cantidad(cfg: dict, familia: str, n_tabla: int) -> int:
    return max(1, round(cfg["anomalias"][familia]["proporcion"] * n_tabla))


def inyectar_anomalias(cfg: dict, usuarios: pd.DataFrame, inventario: pd.DataFrame,
                       tickets: pd.DataFrame, rng: np.random.Generator):
    """Inyecta las 5 familias. Cada registro recibe como máximo una anomalía."""
    registro = []

    def anotar(familia, tabla, clave, campo, original, inyectado):
        registro.append([familia, tabla, clave, campo, original, inyectado])

    t = tickets.copy()
    n = len(t)
    cantidades = {f: _cantidad(cfg, f, n) for f in ("nulos", "huerfanos", "fechas_incoherentes",
                                                    "fuera_de_catalogo")}
    # Las fechas incoherentes solo pueden afectar a tickets con fecha de cierre
    cerrados = np.flatnonzero(t["fecha_cierre"].notna().to_numpy())
    idx_fechas = rng.choice(cerrados, size=cantidades["fechas_incoherentes"], replace=False)
    resto = np.setdiff1d(np.arange(n), idx_fechas)
    otros = rng.choice(resto, size=cantidades["nulos"] + cantidades["huerfanos"]
                       + cantidades["fuera_de_catalogo"], replace=False)
    idx_nulos = otros[: cantidades["nulos"]]
    idx_huerf = otros[cantidades["nulos"]: cantidades["nulos"] + cantidades["huerfanos"]]
    idx_fuera = otros[cantidades["nulos"] + cantidades["huerfanos"]:]

    # Columnas de texto como object para poder escribir cadenas vacías
    for c in ("categoria", "prioridad", "equipo_id", "usuario_id"):
        t[c] = t[c].astype(object)

    for i in idx_nulos:
        campo = rng.choice(["categoria", "prioridad", "equipo_id"])
        anotar("nulos", "tickets", t.at[i, "ticket_id"], campo, t.at[i, campo], "")
        t.at[i, campo] = ""

    max_e, max_u = len(inventario), len(usuarios)
    for k, i in enumerate(idx_huerf):
        campo = rng.choice(["equipo_id", "usuario_id"])
        falso = f"E{max_e + 900000 + k:06d}" if campo == "equipo_id" else f"U{max_u + 900000 + k:06d}"
        anotar("huerfanos", "tickets", t.at[i, "ticket_id"], campo, t.at[i, campo], falso)
        t.at[i, campo] = falso

    for i in idx_fechas:
        apertura = t.at[i, "fecha_apertura"]
        nuevo = apertura - pd.Timedelta(minutes=int(rng.integers(30, 72 * 60)))
        anotar("fechas_incoherentes", "tickets", t.at[i, "ticket_id"], "fecha_cierre",
               t.at[i, "fecha_cierre"].strftime(FORMATO_FECHA_HORA), nuevo.strftime(FORMATO_FECHA_HORA))
        t.at[i, "fecha_cierre"] = nuevo

    invalidos = cfg["valores_fuera_catalogo"]
    for i in idx_fuera:
        campo = rng.choice(list(invalidos))
        valor = rng.choice(invalidos[campo])
        anotar("fuera_de_catalogo", "tickets", t.at[i, "ticket_id"], campo, t.at[i, campo], valor)
        t.at[i, campo] = valor

    # Duplicados: el mismo equipo dado de alta dos veces (copia con otro estado)
    inv = inventario.copy()
    n_dup = _cantidad(cfg, "duplicados", len(inv))
    idx_dup = rng.choice(len(inv), size=n_dup, replace=False)
    copias = inv.iloc[idx_dup].copy()
    copias["estado"] = rng.choice(cfg["catalogos"]["estado_equipo"], size=n_dup)
    for _, fila in copias.iterrows():
        anotar("duplicados", "inventario", fila["equipo_id"], "equipo_id", "", fila["equipo_id"])
    inv = pd.concat([inv, copias], ignore_index=True)
    inv = inv.iloc[rng.permutation(len(inv))].reset_index(drop=True)

    reg = pd.DataFrame(registro, columns=CAMPOS_REGISTRO[1:])
    reg.insert(0, "anomalia_id", _ids("A", len(reg), 6))
    return usuarios, inv, t, reg


# ---------------------------------------------------------------------------
# Escritura
# ---------------------------------------------------------------------------

def _formatear(df: pd.DataFrame, columnas_fecha: dict) -> pd.DataFrame:
    df = df.copy()
    for col, fmt in columnas_fecha.items():
        df[col] = df[col].dt.strftime(fmt).fillna("")
    return df


def escribir(carpeta: Path, usuarios, inventario, tickets, registro) -> None:
    carpeta.mkdir(parents=True, exist_ok=True)
    (carpeta / MARCADOR_LISTO).unlink(missing_ok=True)  # el conjunto deja de estar listo mientras se reescribe
    opciones = dict(index=False, encoding="utf-8", lineterminator="\n")
    usuarios[CAMPOS_USUARIOS].to_csv(carpeta / "usuarios.csv", **opciones)
    _formatear(inventario, {"fecha_adquisicion": FORMATO_FECHA, "fecha_fin_garantia": FORMATO_FECHA})[
        CAMPOS_INVENTARIO].to_csv(carpeta / "inventario.csv", **opciones)
    _formatear(tickets, {"fecha_apertura": FORMATO_FECHA_HORA, "fecha_cierre": FORMATO_FECHA_HORA})[
        CAMPOS_TICKETS].to_csv(carpeta / "tickets.csv", **opciones)
    registro[CAMPOS_REGISTRO].to_csv(carpeta / "anomalias_inyectadas.csv", **opciones)
    # El marcador se escribe el último: si existe, todos los ficheros están completos
    (carpeta / MARCADOR_LISTO).write_text(json.dumps({
        "tickets": len(tickets),
        "ficheros": ["usuarios.csv", "inventario.csv", "tickets.csv", "anomalias_inyectadas.csv"],
        "escrito_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }), encoding="utf-8")


def generar(cfg: dict, n_tickets: int, salida: Path, semilla: int | None = None) -> Path:
    """Genera un conjunto completo de n_tickets y devuelve su carpeta."""
    semilla = cfg["semilla"] if semilla is None else semilla
    # Semilla distinta por tamaño para que cada conjunto sea independiente pero reproducible
    rng = np.random.default_rng([semilla, n_tickets])
    fake = Faker("es_ES")
    fake.seed_instance(semilla * 1_000_003 + n_tickets)

    t0 = time.perf_counter()
    usuarios = generar_usuarios(cfg, n_tickets, rng, fake)
    inventario = generar_inventario(cfg, usuarios, rng)
    tickets = generar_tickets(cfg, n_tickets, inventario, rng)
    usuarios, inventario, tickets, registro = inyectar_anomalias(cfg, usuarios, inventario, tickets, rng)
    carpeta = salida / f"n{n_tickets}"
    escribir(carpeta, usuarios, inventario, tickets, registro)
    log.info("n=%d -> %s | usuarios=%d inventario=%d tickets=%d anomalias=%d | %.1f s",
             n_tickets, carpeta, len(usuarios), len(inventario), len(tickets), len(registro),
             time.perf_counter() - t0)
    return carpeta


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Genera los datos simulados del TFM.")
    parser.add_argument("--config", default="src/generador/config.yaml")
    parser.add_argument("--tamanos", type=int, nargs="*", help="nº de tickets (por defecto, los del config)")
    parser.add_argument("--salida", default="data/raw")
    parser.add_argument("--semilla", type=int, default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = cargar_config(args.config)
    for n in args.tamanos or cfg["tamanos"]:
        generar(cfg, n, Path(args.salida), args.semilla)


if __name__ == "__main__":
    main()
