"""Pruebas de la comparación de los escenarios A y B."""

import pandas as pd
import pytest

from src.evaluacion.comparar_ab import leer_registro_b, resumir, tiempos_b


def _registro(tmp_path, filas, sep=","):
    f = tmp_path / "escenario_B.csv"
    f.write_text("n,repeticion,fase,inicio,fin,segundos,incidencias\n".replace(",", sep)
                 + "\n".join(filas) + "\n", encoding="utf-8")
    return leer_registro_b(f)


def test_latencia_suma_f1_a_f4_sin_exportacion(tmp_path):
    r = _registro(tmp_path, ["1000,1,F1,,,100,", "1000,1,F2,,,200,", "1000,1,F3,,,50,",
                             "1000,1,F4,,,150,", "1000,1,F5,,,40,"])
    t = tiempos_b(r)
    assert t.loc[0, "latencia_s"] == 500
    assert t.loc[0, "F5_s"] == 40


def test_admite_registro_de_excel_con_punto_y_coma_y_coma_decimal(tmp_path):
    r = _registro(tmp_path, ["1000;1;f1;;;10,5;", "1000;1;F2;;;1;", "1000;1;F3;;;1;", "1000;1;F4;;;1;"], sep=";")
    assert tiempos_b(r).loc[0, "latencia_s"] == pytest.approx(13.5)


def test_repeticion_incompleta_da_error(tmp_path):
    r = _registro(tmp_path, ["1000,1,F1,,,1,", "1000,1,F2,,,1,", "1000,1,F3,,,1,", "1000,1,F4,,,1,",
                             "1000,2,F1,,,1,"])
    with pytest.raises(ValueError, match="sin todas las fases"):
        tiempos_b(r)


def test_resumen_cociente_y_tamanos_solo_en_a():
    a = pd.DataFrame({"n": [1000, 1000, 1000000], "estado": ["success", "success", "success"],
                      "latencia_s": [30.0, 40.0, 50.0]})
    b = pd.DataFrame({"n": [1000, 1000], "repeticion": [1, 2], "latencia_s": [700.0, 700.0],
                      "tasa_deteccion": [1.0, 0.9], "falsos_positivos": [0, 2]})
    r = resumir(a, b).set_index("n")
    assert r.loc[1000, "cociente_b_a"] == 20
    assert r.loc[1000, "b_tasa_deteccion_min"] == 0.9 and r.loc[1000, "b_falsos_positivos_max"] == 2
    assert pd.isna(r.loc[1000000, "b_latencia_media"])
