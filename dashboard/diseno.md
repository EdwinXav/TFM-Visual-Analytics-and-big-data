# Diseño del dashboard (Power BI Desktop)

Objetivo específico 3: dashboard interactivo sobre la capa `analytics` que muestre los KPI K1–K10 (`docs/indicadores.md`) y la calidad del dato, y que sirva para la prueba de usabilidad SUS (`docs/validacion_sus.md`).

Principios:
- **Una sola definición por KPI.** Las medidas DAX reproducen las fórmulas de `docs/indicadores.md`; sin filtros deben dar exactamente lo mismo que `kpi_resumen.parquet` (calculado por Spark). Se comprueba en una página de verificación (sección 6).
- **Power BI no transforma datos de negocio.** Power Query solo lee, tipa y añade columnas de presentación (orden, etiquetas). Toda la limpieza ocurre en el pipeline.
- **Cada tarea SUS se responde en una página con un máximo de dos clics** (sección 5).

## 1. Origen de datos

Conector Parquet (`Parquet.Document(File.Contents(...))`), un fichero por tabla en `data/analytics/n<N>/`.

Parámetros de Power Query:

| Parámetro | Valor por defecto | Uso |
|---|---|---|
| `RutaProyecto` | `C:\...\tfm-arquitectura-datos\` | raíz del repositorio en el equipo |
| `Conjunto` | `n100000` | tamaño cargado; cambiarlo y pulsar *Actualizar* |

Consulta tipo (se repite para cada tabla):

```m
let
    Origen = Parquet.Document(File.Contents(RutaProyecto & "data\analytics\" & Conjunto & "\fact_tickets.parquet"))
in
    Origen
```

`deteccion` se lee de `resultados/calidad/<Conjunto>/deteccion.csv`, que escribe `python -m src.evaluacion.deteccion --n <N>` (hay que ejecutarlo después del pipeline). Usa punto decimal: al cambiar el tipo de `tasa_deteccion` a número se indica la configuración regional «Inglés (Estados Unidos)», o Power BI en español leerá `1.0` como 10.

`dim_prioridad` se lee de `referencia/sla.csv` (separador `,`, cabecera) y se le añade la columna `orden` (Critica 1, Alta 2, Media 3, Baja 4).

La actualización de Power BI Desktop es manual: su duración se mide aparte en la evaluación de disponibilidad (`CONTEXTO.md`).

**Comprobar en la primera carga:** Spark escribe las marcas de tiempo (`fecha_apertura`, `fecha_cierre`) como `INT96`. Si Power Query no las interpreta bien (errores o fechas absurdas), se añade al pipeline `spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS` en `src/pipeline/comun.py` y se regenera `analytics`. Las horas se escriben en UTC (`spark.sql.session.timeZone`); en el dashboard se presentan tal cual, sin conversión.

## 2. Modelo

Estrella con `fact_tickets` en el centro (esquema de columnas en `docs/esquema_datos.md`).

| Tabla | Filas (n10000) | Papel |
|---|---|---|
| fact_tickets | 9.600 | hechos (tickets depurados) |
| dim_equipo | 650 | dimensión de equipo y base del parque |
| dim_usuario | 500 | dimensión de usuario (sin nombre) |
| dim_fecha | 731 | calendario 2024-01-01 – 2025-12-31 |
| dim_prioridad | 4 | orden y plazo de SLA de cada prioridad |
| calidad_resumen | 15 | incumplimientos por tabla y regla (sin relaciones) |
| deteccion | 6 | tasa de detección por familia y total (sin relaciones) |
| kpi_resumen | 48 | KPI de Spark, solo para verificación (sin relaciones, oculta) |
| _Medidas | – | tabla vacía que agrupa las medidas |

Relaciones (todas 1:*, dirección de filtro **única**):

| Desde (1) | Hacia (*) | Activa |
|---|---|---|
| dim_fecha[fecha_key] | fact_tickets[fecha_apertura_key] | sí |
| dim_fecha[fecha_key] | fact_tickets[fecha_cierre_key] | **no** (se activa con `USERELATIONSHIP`) |
| dim_equipo[equipo_id] | fact_tickets[equipo_id] | sí |
| dim_usuario[usuario_id] | fact_tickets[usuario_id] | sí |
| dim_prioridad[prioridad] | fact_tickets[prioridad] | sí |

Decisiones:
- **No se relaciona `dim_equipo[usuario_id]` con `dim_usuario`**: crearía dos caminos entre `fact_tickets` y `dim_usuario` y Power BI desactivaría uno de forma implícita.
- **Dos ubicaciones distintas.** La ubicación de los tickets es la del **usuario** (`dim_usuario[ubicacion]`, igual que K10 en Spark); la del parque es la del **equipo** (`dim_equipo[ubicacion]`). En los segmentadores se rotulan «Ubicación del usuario» y «Ubicación del equipo» para no confundirlas. No se crea una dimensión de ubicación compartida por el mismo problema de caminos ambiguos.
- `categoria`, `prioridad`, `estado` y `tecnico` se quedan en la tabla de hechos (dimensiones degeneradas): son catálogos cortos y no justifican tablas propias. `dim_prioridad` existe solo para ordenar y para el plazo de SLA.
- `dim_fecha` se marca como tabla de fechas (columna `fecha`). Columnas calculadas de presentación:
  - `Año-Mes = FORMAT(dim_fecha[fecha], "yyyy-MM")` (no puede llamarse `Mes`: Power BI no distingue mayúsculas en los nombres y ya existe `mes`)
  - `Año-Trimestre = dim_fecha[anio] & "-T" & dim_fecha[trimestre]`
- Se ocultan las claves (`*_key`, `*_id`) y las columnas que solo usan las medidas (`horas_resolucion`, `sla_horas`, `cumple_sla`, `abierto`, `activo`). Ordenación: `prioridad` por `dim_prioridad[orden]`.

## 3. Medidas DAX

Fecha de corte = último día del periodo filtrado (sin filtro: 31/12/2025, el fin del periodo simulado).

### Volumen (K1–K3)

```dax
Tickets abiertos =                                   -- K1: abiertos (registrados) en el periodo
COUNTROWS ( fact_tickets )

Tickets cerrados =                                   -- K2: cerrados en el periodo
CALCULATE (
    COUNTROWS ( fact_tickets ),
    fact_tickets[abierto] = FALSE (),
    USERELATIONSHIP ( dim_fecha[fecha_key], fact_tickets[fecha_cierre_key] )
)

Fecha de corte =
MAX ( dim_fecha[fecha] )

Backlog =                                            -- K3: pendientes al corte
VAR fin = [Fecha de corte] + 1                       -- fin del día de corte
RETURN
    CALCULATE (
        COUNTROWS ( fact_tickets ),
        REMOVEFILTERS ( dim_fecha ),
        fact_tickets[fecha_apertura] < fin,
        OR ( ISBLANK ( fact_tickets[fecha_cierre] ), fact_tickets[fecha_cierre] >= fin )
    )
```

`fact_tickets[abierto] = FALSE ()` en K2 es necesario: sin filtro de fecha, la relación inactiva también incluiría los tickets sin cierre (clave vacía).

En el lienzo, K1 se rotula «Tickets abiertos en el periodo» y K3 «Backlog (pendientes al corte)», para que no se confundan.

### Servicio (K4–K5)

MTTR y SLA se asignan al periodo **de cierre** (un ticket cuenta en el mes en que se resuelve).

```dax
MTTR (h) =                                           -- K4
CALCULATE (
    AVERAGE ( fact_tickets[horas_resolucion] ),
    fact_tickets[abierto] = FALSE (),
    USERELATIONSHIP ( dim_fecha[fecha_key], fact_tickets[fecha_cierre_key] )
)

Cerrados en SLA =
CALCULATE ( [Tickets cerrados], fact_tickets[cumple_sla] = TRUE () )

% cumplimiento SLA =                                 -- K5 (formato porcentaje)
DIVIDE ( [Cerrados en SLA], [Tickets cerrados] )
```

### Parque (K6–K8)

```dax
Equipos =                                            -- K6 (por estado en el visual)
COUNTROWS ( dim_equipo )

Equipos activos =
CALCULATE ( COUNTROWS ( dim_equipo ), dim_equipo[activo] = TRUE () )

Antigüedad media (años) =                            -- K7
CALCULATE ( AVERAGE ( dim_equipo[antiguedad_anios] ), dim_equipo[activo] = TRUE () )

Garantías vencidas =                                 -- K8
CALCULATE (
    COUNTROWS ( dim_equipo ),
    dim_equipo[activo] = TRUE (),
    dim_equipo[garantia_vencida] = TRUE ()
)

% garantías vencidas =
DIVIDE ( [Garantías vencidas], [Equipos activos] )
```

El parque es una **foto a 31/12/2025**: `antiguedad_anios` y `garantia_vencida` vienen calculados por Spark con ese corte y el estado de cada equipo es el del final del periodo. Por eso la página de parque no tiene segmentador de periodo: recalcular la antigüedad a una fecha anterior mezclaría estados finales con fechas pasadas.

### Relación (K9–K10)

```dax
Tickets por equipo =                                 -- K9 (por modelo en el visual)
DIVIDE ( [Tickets abiertos], [Equipos] )
```

K10 no necesita medida propia: es `[Tickets abiertos]` cruzado por `dim_usuario[departamento]` y `dim_usuario[ubicacion]`.

Nota: `[Equipos]` solo responde a los filtros de `dim_equipo`. Si se filtra por departamento, K9 pasa a ser «tickets del departamento por equipo del modelo»; por eso la página de relación aplica a K9 solo los filtros de periodo, categoría y prioridad (interacciones editadas).

### Calidad del dato

`calidad_resumen` repite la columna `entrada` en las cinco filas de cada tabla.

```dax
Registros de entrada =
SUMX ( VALUES ( calidad_resumen[tabla] ), CALCULATE ( MAX ( calidad_resumen[entrada] ) ) )

Incumplimientos =
SUM ( calidad_resumen[registros] )

Registros válidos =
SUMX (
    VALUES ( calidad_resumen[tabla] ),
    SWITCH (
        calidad_resumen[tabla],
        "tickets",    COUNTROWS ( fact_tickets ),
        "inventario", COUNTROWS ( dim_equipo ),
        "usuarios",   COUNTROWS ( dim_usuario )
    )
)

Registros rechazados =
[Registros de entrada] - [Registros válidos]

% rechazados =
DIVIDE ( [Registros rechazados], [Registros de entrada] )
```

`Incumplimientos` cuenta reglas incumplidas; `Registros rechazados` cuenta registros distintos (un registro puede incumplir más de una regla). En n10000 coinciden (406), pero se muestran con nombres distintos para no presentarlos como lo mismo.

Las medidas de calidad que usan `COUNTROWS` de las tablas del modelo deben ignorar los segmentadores de negocio: la página de calidad no tiene segmentadores y no se sincroniza con las demás.

## 4. Páginas

Lienzo 16:9 (1280 × 720). Estructura común: título y subtítulo a la izquierda («Datos simulados · conjunto `Conjunto` · corte 31/12/2025»), panel de segmentadores en una franja vertical a la izquierda, tarjetas de KPI arriba y gráficos debajo. Un único color de acento para los datos; la prioridad usa siempre la misma escala ordinal de cuatro tonos (Crítica la más intensa). Ningún dato se codifica solo con color: las barras llevan etiqueta de valor. Texto alternativo en cada visual.

Segmentadores sincronizados (páginas 1, 2 y 4): **periodo** (`dim_fecha[fecha]`, rango), **ubicación del usuario**, **departamento**, **categoría**, **prioridad**. Botón «Borrar filtros» (marcador) en cada página.

### Página 1 · Resumen
- Tarjetas: K1 tickets abiertos en el periodo · K2 cerrados · K3 backlog · K4 MTTR · K5 % SLA.
- Líneas: K1 y K2 por `Año-Mes`.
- Barras horizontales: backlog por ubicación del usuario (ordenadas de mayor a menor).
- Barras apiladas: tickets por categoría y prioridad.

### Página 2 · Servicio
- Barras: % SLA por prioridad, con el plazo (`dim_prioridad[sla_horas]`) en la información sobre herramientas.
- Barras: MTTR por prioridad.
- Matriz categoría × prioridad con MTTR y formato condicional.
- Líneas: % SLA por `Año-Trimestre` y prioridad.
- Segmentador adicional de `Año-Trimestre` (botones) para responder preguntas de un trimestre concreto.

### Página 3 · Parque (foto a 31/12/2025)
- Tarjetas: equipos activos · K7 antigüedad media · K8 garantías vencidas · % garantías vencidas.
- Barras: K6 equipos por estado.
- Barras: garantías vencidas por ubicación del equipo.
- Barras: antigüedad media por tipo de equipo.
- Segmentadores propios (no sincronizados): **ubicación del equipo**, tipo, marca.

### Página 4 · Relación
- Barras horizontales: K9 tickets por equipo según modelo, ordenadas de mayor a menor, con la media del parque como línea de referencia.
- Matriz departamento × ubicación del usuario con K10 y escala de color (mapa de calor).
- Segmentador adicional de tipo de equipo.

### Página 5 · Calidad del dato
- Tarjetas: registros de entrada · válidos · rechazados · % rechazados.
- Barras: incumplimientos por regla, coloreadas por tabla.
- Tabla: tabla × regla con incumplimientos y entrada.
- Tarjeta: tasa de detección (fila `total` de `deteccion`) y barras de tasa por familia de anomalía, con inyectadas, aciertos y falsos positivos en la información sobre herramientas. Nota visible en la página: «Solo calculable porque los datos son simulados: se compara la cuarentena con el registro de anomalías inyectadas».

### Página 6 · Verificación (oculta)
Tabla con `kpi_resumen` (kpi, indicador, dimension, categoria, valor de Spark) y dos medidas:

```dax
Valor DAX =
VAR k = SELECTEDVALUE ( kpi_resumen[kpi] )
VAR d = SELECTEDVALUE ( kpi_resumen[dimension] )
VAR c = SELECTEDVALUE ( kpi_resumen[categoria] )
RETURN
    SWITCH (
        d,
        "total",
            SWITCH (
                k,
                "K1", [Tickets abiertos],
                "K2", [Tickets cerrados],
                "K3", [Backlog],
                "K4", [MTTR (h)],
                "K5", [% cumplimiento SLA] * 100,
                "K7", [Antigüedad media (años)],
                "K8", [Garantías vencidas]
            ),
        "prioridad",
            CALCULATE (
                SWITCH ( k, "K4", [MTTR (h)], "K5", [% cumplimiento SLA] * 100 ),
                TREATAS ( { c }, dim_prioridad[prioridad] )
            ),
        "estado",       CALCULATE ( [Equipos], TREATAS ( { c }, dim_equipo[estado] ) ),
        "modelo",       CALCULATE ( [Tickets por equipo], TREATAS ( { c }, dim_equipo[modelo] ) ),
        "departamento", CALCULATE ( [Tickets abiertos], TREATAS ( { c }, dim_usuario[departamento] ) ),
        "ubicacion",    CALCULATE ( [Tickets abiertos], TREATAS ( { c }, dim_usuario[ubicacion] ) )
    )

Diferencia =
ABS ( [Valor DAX] - SUM ( kpi_resumen[valor] ) )
```

Criterio: `Diferencia` < 0,001 en las 48 filas (Spark redondea a 4 decimales). La tabla se exporta a `resultados/datos/verificacion_dax_<Conjunto>.csv` y es la evidencia de que el dashboard y el pipeline calculan lo mismo. La página se oculta antes de la prueba SUS.

## 5. Tareas SUS → dónde se responden

| Tarea (`docs/validacion_sus.md`) | Página | Interacción |
|---|---|---|
| 1. Modelo con más incidencias por equipo | Relación | leer la primera barra de K9 |
| 2. Prioridad con peor SLA en el último trimestre | Servicio | botón 2025-T4 → barra más baja de % SLA |
| 3. Ubicación con mayor backlog | Resumen | primera barra de backlog por ubicación |
| 4. Garantías vencidas en Madrid | Parque | ubicación del equipo = Madrid → tarjeta K8 |
| 5. Registros rechazados por integridad referencial | Calidad | barra «integridad_referencial» |

La tarea 4 se refiere a la ubicación del **equipo**; el enunciado de la tarea se ajustará a «equipos ubicados en Madrid» para que no sea ambiguo.

Las respuestas correctas de cada tarea se anotan con el conjunto usado en la prueba antes de las sesiones, para medir el % de tareas completadas.

## 6. Entregables y orden de construcción

El dashboard se guarda como **proyecto de Power BI (PBIP)**: `dashboard/indicadores_ti.pbip`, con el modelo en `indicadores_ti.SemanticModel/definition/` (formato TMDL) y el informe en `indicadores_ti.Report/definition/` (formato PBIR, un fichero JSON por página y por visual). A diferencia del `.pbix` (binario), son ficheros de texto: los cambios del modelo y de las medidas quedan visibles en el historial de git. La caché de datos (`.pbi/cache.abf`) no se versiona; al abrir el proyecto basta con pulsar *Actualizar*.

Estado (15/11/2026): pasos 1–4 hechos sobre `n100000`. Las fechas `INT96` se leen correctamente, sin cambiar el pipeline. La verificación (`python -m src.evaluacion.verificar_dax --n 100000`) da 48/48 filas coincidentes, con diferencia máxima 0. La tabla de verificación se ha obtenido con una consulta DAX contra el modelo de Power BI Desktop.

Estado (17/11/2026): paso 5 hecho. Las cinco páginas visibles y la de verificación (oculta, con su tabla) están construidas; se añadieron cinco medidas auxiliares (`Plazo SLA (h)`, `Tasa por familia`, `Anomalías inyectadas`, `Aciertos`, `Falsos positivos`). Decisiones tomadas al construirlas:
- Las tarjetas de KPI son tarjetas clásicas con unidades «Ninguna»: con las abreviaturas por defecto («1,3 mil») no se distinguía Valencia (1 261) de Bilbao (1 258), que es la tarea SUS 3. Las etiquetas de las barras usan el mismo criterio.
- El segmentador de trimestres de Servicio es una lista vertical (no mosaico) y excluye el valor en blanco.
- Tema: el base de Power BI más `tema_tfm.json` (escala ordinal de prioridad).

Tras guardar, la verificación sigue en 48/48 y las respuestas de las cinco tareas SUS se han comprobado por consulta DAX: ProBook 450 (31,16), Crítica 77,7 % en 2025-T4, Valencia 1 261, Madrid 679 e integridad referencial 1 000. ***[Pendiente: formato condicional de color de fondo en las matrices de Servicio y Relación, línea de promedio en K9, texto alternativo y ensayo de las tareas SUS con tiempos y clics (paso 6).]***

1. Parámetros y consultas; comprobar tipos (fechas `INT96`, booleanos). → captura del modelo.
2. Relaciones, tabla de fechas, columnas calculadas y ocultación.
3. Medidas de la sección 3 en `_Medidas`, con carpetas de visualización (Volumen, Servicio, Parque, Relación, Calidad).
4. Página de verificación y exportación de `resultados/datos/verificacion_dax_<Conjunto>.csv`: no se sigue hasta que todas las diferencias sean ~0.
5. Páginas 1–5 y sincronización de segmentadores.
6. Recorrido de las cinco tareas SUS por el autor (tiempo y clics) como ensayo.
7. Guardar el proyecto (`dashboard/indicadores_ti.pbip`) y capturas de cada página en `resultados/capturas/`.

Fuera de alcance: publicación en Power BI Service, actualización programada y seguridad a nivel de fila (entorno local, `CONTEXTO.md`).
