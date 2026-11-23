# Hoja de tiempos del escenario B (para imprimir)

Una hoja por repetición. Se apunta la **hora real del reloj** (hh:mm:ss) al empezar y al terminar
cada fase, sin redondear. No se estiman ni se reconstruyen tiempos después. Si hay una pausa o una
interrupción, se anota en «Incidencias» con su duración. Al terminar, las horas se pasan a
`resultados/tiempos/escenario_B.csv` (la columna `segundos` puede quedar vacía: se calcula a partir de las horas).

Tamaño (n): ☐ 1000  ☐ 10000        Repetición: ☐ 1  ☐ 2        Fecha: ____/____/2026

Libro: `B_n______ _r___.xlsx`

| Fase | Qué incluye | Inicio (hh:mm:ss) | Fin (hh:mm:ss) | Incidencias |
|------|-------------|-------------------|----------------|-------------|
| F1 | Importación | | | |
| F2 | Detección de anomalías y hoja `rechazados` | | | |
| F3 | Depuración y cálculo | | | |
| F4 | Indicadores (hoja `kpi`) | | | |
| F5 | Exportación (no cuenta en la latencia) | | | |

Comprobación al terminar (sin mirar el libro de ejemplo durante la repetición):

- K1 = ______ (10³: 960 · 10⁴: 9.600)
- Filas en `rechazados` = ______ (10³: 41 · 10⁴: 406)
- ¿Errores corregidos durante la repetición? ☐ No  ☐ Sí → describirlos en «Incidencias».
