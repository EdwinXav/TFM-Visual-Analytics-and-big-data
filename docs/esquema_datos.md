# Esquema de datos

Datos 100 % simulados (ver `src/generador/config.yaml`), generados con `src/generador/generar.py`. Cada tamaño se escribe en `data/raw/n<tickets>/` (p. ej. `data/raw/n10000/`); la muestra de 10^3 se versiona en `data/muestra/n1000/`. Ficheros CSV codificación UTF-8, separador `,`, fechas en ISO 8601 (`YYYY-MM-DD` o `YYYY-MM-DD HH:MM:SS`).

## Fuentes (capa raw)

### usuarios.csv
| Campo | Tipo | Obligatorio | Regla / dominio |
|---|---|---|---|
| usuario_id | string `U000001` | sí | clave primaria, única |
| nombre | string | sí | ficticio (Faker, `es_ES`) |
| departamento | string | sí | catálogo `departamento` |
| ubicacion | string | sí | catálogo `ubicacion` |
| rol | string | sí | catálogo `rol` |

### inventario.csv
| Campo | Tipo | Obligatorio | Regla / dominio |
|---|---|---|---|
| equipo_id | string `E000001` | sí | clave primaria, única |
| tipo | string | sí | catálogo `tipo_equipo` |
| marca | string | sí | según catálogo `modelos` |
| modelo | string | sí | catálogo `modelos` (marca y tipo coherentes) |
| fecha_adquisicion | date | sí | ≤ fin del periodo |
| fecha_fin_garantia | date | sí | > fecha_adquisicion (2–4 años) |
| estado | string | sí | catálogo `estado_equipo` |
| ubicacion | string | sí | catálogo `ubicacion` |
| usuario_id | string | no | FK → usuarios (vacío si está en stock) |

### tickets.csv
| Campo | Tipo | Obligatorio | Regla / dominio |
|---|---|---|---|
| ticket_id | string `T0000001` | sí | clave primaria, única; numeración en orden de apertura |
| fecha_apertura | timestamp | sí | dentro del periodo |
| fecha_cierre | timestamp | condicional | obligatoria si estado ∈ {Resuelto, Cerrado}; vacía si {Abierto, En curso}; ≥ fecha_apertura |
| categoria | string | sí | catálogo `categoria` |
| prioridad | string | sí | catálogo `prioridad` |
| estado | string | sí | catálogo `estado_ticket` |
| equipo_id | string | sí | FK → inventario |
| usuario_id | string | sí | FK → usuarios |
| tecnico | string `T01`… | sí | T01..T{num_tecnicos} |

### _LISTO
Marcador JSON que el generador escribe al terminar (nº de tickets, ficheros y hora UTC). El DAG no procesa el conjunto hasta que existe.

### anomalias_inyectadas.csv (verdad de referencia, no se usa en el pipeline)
| Campo | Descripción |
|---|---|
| anomalia_id | id único |
| familia | nulos · duplicados · huerfanos · fechas_incoherentes · fuera_de_catalogo |
| tabla | tabla afectada |
| clave | id del registro afectado |
| campo | campo alterado |
| valor_original / valor_inyectado | para trazabilidad |

## Reglas de validación

| Regla | Familia detectada | Tratamiento propuesto |
|---|---|---|
| Completitud | nulos | cuarentena |
| Unicidad de clave | duplicados | se conserva la primera aparición; resto a cuarentena |
| Integridad referencial | huérfanos | cuarentena |
| Consistencia temporal | fechas incoherentes | cuarentena |
| Validez de dominio | fuera de catálogo | cuarentena |

Los registros rechazados se guardan en `data/quarantine/` con el motivo, y el informe de calidad (`resultados/calidad/`) cuenta los incumplimientos por regla. Detección = registros de cuarentena que coinciden con `anomalias_inyectadas.csv`.

## Capas

`raw` (CSV) → `curated` (Parquet, depurado y tipado) + `quarantine` (rechazados) → `analytics` (Parquet, modelo en estrella).

## Modelo analítico (estrella)

- **fact_tickets**: ticket_id, fecha_apertura_key, fecha_cierre_key, equipo_id, usuario_id, categoria, prioridad, estado, tecnico, horas_resolucion, cumple_sla, abierto (bool).
- **dim_equipo**: equipo_id, tipo, marca, modelo, fecha_adquisicion, fecha_fin_garantia, estado, ubicacion, antiguedad_anios, garantia_vencida.
- **dim_usuario**: usuario_id, departamento, ubicacion, rol (sin nombre: no es necesario para el análisis).
- **dim_fecha**: fecha_key, fecha, anio, trimestre, mes, semana, dia_semana.

Para Power BI cada tabla se escribe como un único fichero Parquet (`coalesce(1)`).
