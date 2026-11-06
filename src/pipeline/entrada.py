"""Control de la llegada de datos: qué conjunto está completo y cuál falta por procesar.

No depende de Spark ni de Airflow, para poder probarse de forma aislada.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

FICHEROS = ("usuarios.csv", "inventario.csv", "tickets.csv")
MARCADOR_LISTO = "_LISTO"
MARCADOR_PROCESADO = "_PROCESADO"


def hora_llegada(carpeta: Path) -> str | None:
    """Hora UTC en que el conjunto quedó completo, o None si todavía no lo está."""
    marcador = carpeta / MARCADOR_LISTO
    if not marcador.is_file() or not all((carpeta / f).is_file() for f in FICHEROS):
        return None
    instante = datetime.fromtimestamp(marcador.stat().st_mtime, tz=timezone.utc)
    return instante.isoformat(timespec="milliseconds")


def pendiente_de_procesar(carpeta: Path, forzar: bool = False) -> bool:
    """True si hay un conjunto completo sin procesar (o se fuerza su reproceso)."""
    llegada = hora_llegada(carpeta)
    if llegada is None:
        return False
    procesado = carpeta / MARCADOR_PROCESADO
    if forzar or not procesado.is_file():
        return True
    return procesado.read_text(encoding="utf-8").strip() != llegada


def marcar_procesado(carpeta: Path, llegada: str) -> None:
    """Registra qué llegada se ha procesado, para no repetirla en la próxima ejecución."""
    (carpeta / MARCADOR_PROCESADO).write_text(llegada, encoding="utf-8")
