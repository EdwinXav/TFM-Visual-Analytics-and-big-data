# Procedimiento del escenario de referencia (B) — Excel

Se escribe y se congela **antes** de ejecutarlo (iteración 1). Se aplica solo a los conjuntos de 10^3 y 10^4 tickets, 3 repeticiones cada uno, cronometrando cada fase.

Pendiente de redactar paso a paso:
1. Importación de `usuarios.csv`, `inventario.csv` y `tickets.csv` (Power Query desactivado / importación manual de texto).
2. Detección de anomalías por regla (filtros, `CONTAR.SI`, `BUSCARV`/`BUSCARX`, comparación de fechas) y registro de las detectadas.
3. Eliminación de registros inválidos.
4. Cálculo de los KPI K1–K10 de `docs/indicadores.md` con tablas dinámicas y fórmulas.
5. Hoja de resultados y marca de tiempo final.

Registro por ejecución: tamaño, repetición, hora de inicio/fin por fase, anomalías detectadas por familia, incidencias.
