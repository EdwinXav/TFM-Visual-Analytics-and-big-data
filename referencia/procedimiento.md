# Procedimiento del escenario de referencia (B): Excel

Reproduce cómo se calcularían los indicadores sin automatización. Se fija **antes** de ejecutarlo para que sea repetible y use las mismas definiciones que el escenario A (`docs/indicadores.md`). No se modifica una vez iniciadas las ejecuciones; cualquier incidencia se anota en el registro.

## Alcance y condiciones
- **Conjuntos:** `data/raw/n1000` y `data/raw/n10000`. Los tamaños mayores no se ejecutan: Excel admite 1.048.576 filas por hoja y su rendimiento se degrada mucho antes.
- **Repeticiones:** 3 por tamaño, cada una en un libro nuevo y en orden: todas las de 10³ y después las de 10⁴.
- **Herramienta:** Microsoft Excel para Microsoft 365, versión 2408, en inglés (fórmulas en inglés con separador `;`).
- **Ejecuta:** el autor, en el mismo equipo que el escenario A.
- **No se permite:** macros, scripts, herramientas de IA ni transformaciones de Power Query. La carga con `From Text/CSV` solo importa los ficheros.
- **Se permite:** copiar las fórmulas de este documento. Es la situación más favorable al procedimiento manual, porque no se mide el tiempo de escribirlas ni de depurarlas, y debe tenerse en cuenta al interpretar los resultados.
- **Antes de empezar:** comprobar que la cuenta de Office no muestra errores, cerrar otras aplicaciones y usar los datos tal como los deja el generador.

## Medición
Cronometrar cada fase y anotarla en `resultados/tiempos/escenario_B.csv` (plantilla en `referencia/registro_escenario_B.csv`): `n, repeticion, fase, inicio, fin, segundos, incidencias`. Si hay un error, se corrige sin parar el cronómetro y se anota como incidencia. Si hay una interrupción ajena al procedimiento, se detiene el cronómetro y se anota.

El tiempo total, desde el inicio de F1 hasta el fin de F4, es también la latencia del escenario B: los indicadores no están disponibles hasta entonces.

## Preparación (no se cronometra)
Crear un libro en blanco y guardarlo como `B_n<tamaño>_r<repetición>.xlsx`.

## F1. Importación
Para cada fichero: `Data > From Text/CSV`, elegir el fichero y `Load`. Después, en la pestaña `Table Design`, poner el nombre de tabla indicado.

| Fichero | Nombre de la tabla |
|---|---|
| `data/raw/n<tamaño>/usuarios.csv` | `tblU` |
| `data/raw/n<tamaño>/inventario.csv` | `tblE` |
| `data/raw/n<tamaño>/tickets.csv` | `tblT` |
| `referencia/catalogos.csv` | `tblCat` |
| `referencia/sla.csv` | `tblSLA` |

Comprobar que `fecha_apertura`, `fecha_cierre`, `fecha_adquisicion` y `fecha_fin_garantia` son fechas (alineadas a la derecha) y que `sla_horas` es un número. Si alguna no lo es, anotarlo como incidencia y cambiar su tipo.

## F2. Detección de anomalías
Añadir a cada tabla las columnas siguientes, en este orden, escribiendo el nombre en la cabecera y la fórmula en la primera fila. La columna `motivo` guarda la primera regla incumplida; vacía significa registro válido.

Los nombres con guion bajo llevan doble corchete (`[@[usuario_id]]`); sin ello, Excel rechaza la fórmula. Copia las fórmulas desde la vista normal del documento (por ejemplo, en GitHub), no desde su texto fuente: la barra vertical de `tblE` aparece allí escapada. Las fórmulas usan `;` como separador y coma decimal; en un equipo con otra configuración regional hay que adaptarlas.

### `tblU`
| Columna | Fórmula |
|---|---|
| `r_completitud` | `=IF(OR([@[usuario_id]]="";[@departamento]="";[@ubicacion]="";[@rol]="");1;0)` |
| `r_validez` | `=IF(OR(COUNTIFS(tblCat[catalogo];"departamento";tblCat[valor];[@departamento])=0;COUNTIFS(tblCat[catalogo];"ubicacion";tblCat[valor];[@ubicacion])=0;COUNTIFS(tblCat[catalogo];"rol";tblCat[valor];[@rol])=0);1;0)` |
| `r_unicidad` | `=IF(COUNTIF(INDEX([usuario_id];1):[@[usuario_id]];[@[usuario_id]])>1;1;0)` |
| `motivo` | `=IF([@[r_completitud]]=1;"completitud";IF([@[r_validez]]=1;"validez";IF([@[r_unicidad]]=1;"unicidad";"")))` |

### `tblE`
| Columna | Fórmula |
|---|---|
| `r_completitud` | `=IF(OR([@[equipo_id]]="";[@tipo]="";[@marca]="";[@modelo]="";[@[fecha_adquisicion]]="";[@[fecha_fin_garantia]]="";[@estado]="";[@ubicacion]="");1;0)` |
| `r_validez` | `=IF(OR(COUNTIFS(tblCat[catalogo];"modelo";tblCat[valor];[@marca]&"\|"&[@modelo]&"\|"&[@tipo])=0;COUNTIFS(tblCat[catalogo];"estado_equipo";tblCat[valor];[@estado])=0;COUNTIFS(tblCat[catalogo];"ubicacion";tblCat[valor];[@ubicacion])=0);1;0)` |
| `r_temporal` | `=IF([@[fecha_fin_garantia]]<=[@[fecha_adquisicion]];1;0)` |
| `r_unicidad` | `=IF(COUNTIF(INDEX([equipo_id];1):[@[equipo_id]];[@[equipo_id]])>1;1;0)` |
| `r_referencial` | `=IF(AND([@[usuario_id]]<>"";COUNTIF(tblU[usuario_id];[@[usuario_id]])=0);1;0)` |
| `motivo` | `=IF([@[r_completitud]]=1;"completitud";IF([@[r_validez]]=1;"validez";IF([@[r_temporal]]=1;"consistencia_temporal";IF([@[r_unicidad]]=1;"unicidad";IF([@[r_referencial]]=1;"integridad_referencial";"")))))` |

### `tblT`
| Columna | Fórmula |
|---|---|
| `r_completitud` | `=IF(OR([@[ticket_id]]="";[@[fecha_apertura]]="";[@categoria]="";[@prioridad]="";[@estado]="";[@[equipo_id]]="";[@[usuario_id]]="";[@tecnico]="";AND(OR([@estado]="Resuelto";[@estado]="Cerrado");[@[fecha_cierre]]=""));1;0)` |
| `r_validez` | `=IF(OR(COUNTIFS(tblCat[catalogo];"prioridad";tblCat[valor];[@prioridad])=0;COUNTIFS(tblCat[catalogo];"categoria";tblCat[valor];[@categoria])=0;COUNTIFS(tblCat[catalogo];"estado_ticket";tblCat[valor];[@estado])=0;COUNTIFS(tblCat[catalogo];"tecnico";tblCat[valor];[@tecnico])=0);1;0)` |
| `r_temporal` | `=IF(OR(AND([@[fecha_cierre]]<>"";[@[fecha_cierre]]<[@[fecha_apertura]]);AND(OR([@estado]="Abierto";[@estado]="En curso");[@[fecha_cierre]]<>""));1;0)` |
| `r_unicidad` | `=IF(COUNTIF(INDEX([ticket_id];1):[@[ticket_id]];[@[ticket_id]])>1;1;0)` |
| `r_referencial` | `=IF(OR(COUNTIF(tblE[equipo_id];[@[equipo_id]])=0;COUNTIF(tblU[usuario_id];[@[usuario_id]])=0);1;0)` |
| `motivo` | `=IF([@[r_completitud]]=1;"completitud";IF([@[r_validez]]=1;"validez";IF([@[r_temporal]]=1;"consistencia_temporal";IF([@[r_unicidad]]=1;"unicidad";IF([@[r_referencial]]=1;"integridad_referencial";"")))))` |

Crear la hoja `rechazados`, con `tabla`, `clave` y `motivo` en `A1:C1`, y en `A2`:

```
=LET(e;FILTER(tblE[equipo_id];tblE[motivo]<>"");me;FILTER(tblE[motivo];tblE[motivo]<>"");t;FILTER(tblT[ticket_id];tblT[motivo]<>"");mt;FILTER(tblT[motivo];tblT[motivo]<>"");VSTACK(HSTACK(IF(SEQUENCE(ROWS(e));"inventario");e;me);HSTACK(IF(SEQUENCE(ROWS(t));"tickets");t;mt)))
```

## F3. Depuración y cálculo
No se borran filas: los registros con `motivo` no vacío quedan excluidos porque todos los cálculos filtran por `motivo` vacío. Añadir estas columnas y comprobar que no muestran errores:

| Tabla | Columna | Fórmula |
|---|---|---|
| `tblE` | `antiguedad` | `=IF(AND([@motivo]="";[@estado]<>"Baja");ROUND((DATE(2025;12;31)-[@[fecha_adquisicion]])/365,25;2);"")` |
| `tblT` | `horas` | `=IF(OR([@motivo]<>"";[@[fecha_cierre]]="");"";([@[fecha_cierre]]-[@[fecha_apertura]])*24)` |
| `tblT` | `cumple_sla` | `=IF([@horas]="";"";IF([@horas]<=XLOOKUP([@prioridad];tblSLA[prioridad];tblSLA[sla_horas]);1;0))` |
| `tblT` | `modelo` | `=XLOOKUP([@[equipo_id]];tblE[equipo_id];tblE[modelo];"")` |
| `tblT` | `departamento` | `=XLOOKUP([@[usuario_id]];tblU[usuario_id];tblU[departamento];"")` |
| `tblT` | `ubicacion` | `=XLOOKUP([@[usuario_id]];tblU[usuario_id];tblU[ubicacion];"")` |

## F4. Indicadores
Crear la hoja `kpi`: importar `referencia/kpi_plantilla.csv` con `From Text/CSV` > `Load To...` en la celda `A1` de esa hoja, usar `Table Design > Convert to Range` y escribir `valor` en `D1`. Cada fórmula se escribe en la primera fila de su bloque y se arrastra hasta la última.

| Filas de la plantilla | Primera fila | Fórmula |
|---|---|---|
| K1 total | 2 | `=COUNTIFS(tblT[motivo];"")` |
| K2 total | 3 | `=COUNTIFS(tblT[motivo];"";tblT[fecha_cierre];"<>")` |
| K3 total | 4 | `=COUNTIFS(tblT[motivo];"";tblT[fecha_cierre];"")` |
| K4 total | 5 | `=AVERAGE(tblT[horas])` |
| K5 total | 6 | `=AVERAGE(tblT[cumple_sla])*100` |
| K4 por prioridad | 7 | `=AVERAGEIFS(tblT[horas];tblT[prioridad];C7)` |
| K5 por prioridad | 11 | `=AVERAGEIFS(tblT[cumple_sla];tblT[prioridad];C11)*100` |
| K6 por estado | 15 | `=COUNTIFS(tblE[motivo];"";tblE[estado];C15)` |
| K7 total | 19 | `=AVERAGE(tblE[antiguedad])` |
| K8 total | 20 | `=COUNTIFS(tblE[motivo];"";tblE[estado];"<>Baja";tblE[fecha_fin_garantia];"<"&DATE(2025;12;31))` |
| K9 por modelo | 21 | `=IFERROR(COUNTIFS(tblT[motivo];"";tblT[modelo];C21)/COUNTIFS(tblE[motivo];"";tblE[modelo];C21);"")` |
| K10 por departamento | 37 | `=COUNTIFS(tblT[motivo];"";tblT[departamento];C37)` |
| K10 por ubicación | 45 | `=COUNTIFS(tblT[motivo];"";tblT[ubicacion];C45)` |

Con todos los valores calculados, parar el cronómetro de F4: es el fin de la latencia.

## F5. Exportación (no cuenta para la latencia)
Guardar el libro. Para cada hoja, `kpi` y `rechazados`: botón derecho en la pestaña > `Move or Copy` > `Create a copy` en un libro nuevo, y guardarlo como `CSV (Comma delimited)`:
- `resultados/escenario_B/kpi_n<tamaño>_r<repetición>.csv`
- `resultados/escenario_B/rechazados_n<tamaño>_r<repetición>.csv`

No cambiar el formato de los números antes de exportar: el CSV guarda lo que se ve.

## Comprobación
```bash
docker compose exec -w /opt/proyecto airflow-scheduler python -m src.evaluacion.escenario_b --n 1000 --rechazados resultados/escenario_B/rechazados_n1000_r1.csv --kpi resultados/escenario_B/kpi_n1000_r1.csv
```
Devuelve la tasa de detección por familia, los falsos positivos y si los indicadores coinciden con los del escenario A (tolerancia de 0,01, por redondeo). Un error del operador debe quedar reflejado en el resultado, no corregirse después.

## Validación previa
Las fórmulas se comprobaron una vez, antes de fijar este documento, mediante automatización sobre `n1000` y `n10000`. Esa comprobación no forma parte del experimento ni aporta tiempos. Coincidieron con el escenario A: 41/41 y 406/406 anomalías sin falsos positivos, y 46/46 y 48/48 valores de indicadores.

## Limitaciones
El autor ejecuta el procedimiento, conoce las fórmulas y las copia, y sus tiempos pueden mejorar de una repetición a otra. Por eso el orden es fijo y se documenta cada repetición.
