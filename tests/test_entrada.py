"""Pruebas del control de llegada de datos del DAG (iteración 3)."""

import os

import pytest

from src.pipeline.entrada import FICHEROS, hora_llegada, marcar_procesado, pendiente_de_procesar


@pytest.fixture
def carpeta(tmp_path):
    for f in FICHEROS:
        (tmp_path / f).write_text("x", encoding="utf-8")
    (tmp_path / "_LISTO").write_text("{}", encoding="utf-8")
    return tmp_path


def test_sin_marcador_no_hay_datos(carpeta):
    (carpeta / "_LISTO").unlink()
    assert hora_llegada(carpeta) is None
    assert not pendiente_de_procesar(carpeta)
    assert not pendiente_de_procesar(carpeta, forzar=True)


def test_sin_un_fichero_no_hay_datos(carpeta):
    (carpeta / "tickets.csv").unlink()
    assert not pendiente_de_procesar(carpeta)


def test_datos_nuevos_se_procesan_una_sola_vez(carpeta):
    assert pendiente_de_procesar(carpeta)
    marcar_procesado(carpeta, hora_llegada(carpeta))
    assert not pendiente_de_procesar(carpeta)


def test_forzar_permite_repetir_ejecuciones(carpeta):
    marcar_procesado(carpeta, hora_llegada(carpeta))
    assert pendiente_de_procesar(carpeta, forzar=True)


def test_una_nueva_llegada_vuelve_a_estar_pendiente(carpeta):
    marcar_procesado(carpeta, hora_llegada(carpeta))
    marcador = carpeta / "_LISTO"
    t = marcador.stat().st_mtime + 60
    os.utime(marcador, (t, t))
    assert pendiente_de_procesar(carpeta)
