# Tarea 2 — Redis + HBase: del stream a la historia

Pipeline completo sobre los sensores del Gran Concepción (16 estaciones, ~2
eventos/s simulados): Redis mantiene el **presente** (stream y tablero) y HBase
guarda la **historia** (5 días de lecturas, repartidas en regiones).

```
generador (semilla 2026)
   │
   ├── 864.000 históricos ─────────────► HBase: lecturas + lecturas_tipo
   │                                      (batch, tablas pre-divididas)
   │
   └── 7.200 en vivo (20×) ──► Redis stream: sensores:t2
                                  │
                    ┌─────────────┴─────────────┐
              grupo ingesta                grupo tablero
                    │                           │
        put en las 2 tablas                  ultimo:<est>
                    │                        ranking:flujo
                 XACK                          ventana:<est>
                                               tipo:<tipo>
```

## Reproducir desde cero

Requisitos: Docker con ~4 GB de RAM, ~3 GB de disco y `bash` + Python 3.10+.

```bash
./run.sh
```

Ese único comando: levanta Redis 7 y HBase 2.5 (`docker compose up -d --wait`),
crea el venv e instala `requirements.txt`, vacía Redis, recrea las tablas de
HBase con sus SPLITS, carga los 864.000 históricos, corre la hora en vivo con
los dos consumidores, verifica los números de control y ejecuta las 6 consultas
de la Parte C. Las preguntas y los experimentos de la Parte D también se pueden
correr por separado (ver más abajo).

Tiempos medidos en el equipo de la sección "Hardware" (i5-12500H, 16 GB,
Docker con 4 GB):

| Etapa | Tiempo |
|---|---|
| `docker compose up -d --wait` (imagen ya construida) | ~15 s |
| Crear/borrar tablas con SPLITS | 36 s |
| Carga histórica (864.000 eventos, batch 2.000) | 45 s |
| Hora en vivo (7.200 eventos a 25 ms) | 180 s |
| Verificación (conteo de 2 tablas) | 20 s |
| Consultas P1–P6 | ~1,5 min |
| **Total** | **~6,5 min** |

Para desarrollo se puede reutilizar lo cargado o saltar las consultas:

```bash
KEEP=1 ./run.sh             # no borra las tablas de HBase
LIMITE=1000 ./run.sh        # corrida corta (no calza los números de control)
SIN_CONSULTAS=1 ./run.sh    # solo pipeline + verificación
```

> La primera vez, `docker compose build` descarga ~340 MB (imagen de HBase).

## Números de control (verificados)

Salida real de `python3 src/verificar.py`:

```
histórico cargado      864.000     864.000  OK
en vivo producidos       7.200       7.200  OK
total                  871.200     871.200  OK
grupo ingesta            7.200           0 pendientes
grupo tablero            7.200           0 pendientes
lecturas               853.953     853.953  OK
lecturas_tipo          853.953     853.953  OK
eventos - claves = 17.247 = 17.243 repetidos + 4 choques de clave
RESULTADO: TODO CALZA
```

Los 4 pares con la misma estación y milisegundo pero distinto valor se
absorben igual que los repetidos: la row key no incluye el valor ni la llegada.

## Decisiones de diseño

### Row keys e idempotencia

| Tabla | Row key | Pregunta que hace rápida | Pregunta que hace lenta |
|---|---|---|---|
| `lecturas` | `estacion#timestamp_medicion` | lecturas de una estación en un rango (P3), último por estación | todas las estaciones en un rango |
| `lecturas_tipo` | `tipo#timestamp_medicion#estacion` | un tipo en un rango, todas sus estaciones (P4b) | una estación suelta en un rango |

El timestamp es ISO-8601 con milisegundos y **offset fijo -03:00** (hora de
Chile), nunca UTC mezclado. Al ser ancho fijo, el orden lexicográfico de la row
key coincide con el orden temporal.

La clave sale de la **estación y de la medición**, nunca del instante de
llegada. Por eso:
* un evento repetido escribe la misma fila (sobrescribe);
* una lectura tardía cae en su lugar histórico, no al final;
* reprocesar un lote (D.1) no duplica filas, solo agrega una versión a la celda
  (`VERSIONS => 3`).

### SPLITS

* `lecturas`: **16 regiones**, una por estación (`CC-002`, `CC-003`, …, `TAL-003`
  como split keys). Así el rango de P3 toca una sola región.
* `lecturas_tipo`: **13 regiones**, una por (tipo, día): 4 cortes de medianoche
  por cada uno de los 3 tipos. El scan de P4b (calidad_aire, 05-nov 07:00–09:00)
  toca una sola región.

### Camino en vivo

* Stream `sensores:t2`, `XADD` con **MAXLEN ~10.000** aproximado. A 40 ev/s son
  ~250 s (4 min) de tolerancia de lag: holgado para consumidores que procesan
  miles de eventos/s. Si un consumidor se cae más de 4 min, Redis recorta y esos
  eventos se pierden **para ese grupo**; lo ya escrito en HBase no se afecta.
* Dos grupos independientes (`ingesta`, `tablero`): cada uno ve el stream
  completo.
* `ingesta` lee en lotes de 200 con `XREADGROUP`, escribe las dos tablas en
  batch y **recién después hace XACK**. Si muere entre put y XACK, el lote queda
  pendiente y otro consumidor lo recupera con `XAUTOCLAIM`.
* `tablero` mantiene `ultimo:<est>` (hash `valor`/`ts`/`tipo`), `ranking:flujo`
  (zset), `ventana:<est>` (zset con score = ts de medición, recortado a 10 min
  de la medición más reciente del stream) y `tipo:<tipo>` (set catálogo).
* **Último por medición**: antes de pisar `ultimo:<est>` se compara el ts de
  medición contra el guardado. Una lectura tardía no hace retroceder el tablero
  ni el ranking. Para la ventana se recorta contra `tablero:max_ts` (máximo del
  stream), no contra la llegada.

### Persistencia de Redis

**AOF con `appendfsync everysec` + volumen con nombre** (decisión explícita en
`docker-compose.yml`). Se pierde como máximo ~1 s de escrituras. RDB perdería
minutos; sin persistencia se pierde el stream completo (ver D.2). El histórico
en HBase vive en disco y no depende de Redis.

## Preguntas (Parte C)

Cada script mide la mediana de 5 corridas descartando la primera. Resultados
completos y comparaciones en `informe/informe.pdf`; resumen:

| # | Script | Idea | Tiempo medido |
|---|---|---|---|
| P1 | `src/p1_ahora.py` | "ahora" en Redis vs HBase (16 estaciones + top 5) | Redis 2,3 ms · HBase 205,4 ms (×89) |
| P2 | `src/p2_ventana.py` | promedio de 10 min desde `ventana:*` | 3,3 ms · 19.944 B para 8 ventanas |
| P3 | `src/p3_rango.py` | rango CC-001 04-nov 08:00–10:00 | rango 17,4 ms · tabla completa 600,8 ms (×34,5) |
| P4 | `src/p4_patrones.py` | catálogo Redis + HBase por estación vs por tipo | 60,1 ms (4 rangos) vs 40,2 ms (1 scan); 125 MB vs 72 MB |
| P5 | `src/p5_hotspotting.py` | hotspotting: `est#ts` vs `ts#est` vs `salt#ts#est` | 99,2 / 19,4 / 33,9 ms |
| P6 | `src/p6_propia.py` | top 5 de ahora (Redis) vs su historia (HBase) | 4,5 s |

## Parte D — Cuando algo se cae

```bash
python3 src/d1_falla_ingesta.py   # mata ingesta entre put y XACK y recupera
python3 src/d2_redis_persistencia.py  # reinicia Redis con AOF y sin persistencia
```

* **D.1**: quedan 200 entradas pendientes a nombre de `ingesta-1`;
  `ingesta-2` las recupera con `XAUTOCLAIM`, reescribe y hace XACK. HBase
  termina con 853.953 filas en cada tabla (ni una más ni una menos) y la celda
  reprocesada muestra 3 versiones con el mismo valor.
* **D.2**: con AOF sobreviven stream, grupo, 300 pendientes, `ultimo:*`,
  `ranking` y `ventana`. Sin persistencia no sobrevive nada. La historia en
  HBase está a salvo en ambos casos.

## Estructura

```
docker-compose.yml          Redis 7 + HBase 2.5 (healthchecks, volumen, AOF)
docker/hbase/Dockerfile     HBase standalone + Thrift (9090)
run.sh                      pipeline completo de punta a punta
requirements.txt
src/comun.py                constantes, row keys, splits, conexiones
src/carga_historica.py      B.1 carga batch a las dos tablas
src/productor.py            B.2 7.200 en vivo → sensores:t2
src/consumidor_ingesta.py   B.2 stream → HBase (XACK después del put)
src/consumidor_tablero.py   B.2 stream → tablero en Redis (último por medición)
src/verificar.py            B.3 números de control
src/p1_ahora.py … p6_propia.py   Parte C
src/d1_falla_ingesta.py     D.1 muerte entre put y XACK
src/d2_redis_persistencia.py D.2 reinicio de Redis
scripts/generador.py        fuente de datos (semilla 2026)
scripts/prueba_generador.py valida los números de control del generador
informe/informe.pdf         informe breve (máx. 5 páginas)
informe/informe.html        fuente del informe
informe/generar_pdf.py      regenera el PDF (requiere requirements-informe.txt)
```

## Qué no funcionó / limitaciones

* **Happybase 1.3 no permite `createTable` con split keys** (el Thrift1 de HBase
  no expone SPLITS). Las tablas se crean con `hbase shell` dentro del contenedor
  (`src/comun.py: crear_tabla`). Funciona, pero depende de que el servicio se
  llame `hbase` en el compose.
* `happybase` no expone las múltiples versiones de una celda; para mostrar las
  3 versiones de D.1 se usa `hbase shell get ... VERSIONS => 3`.
* En HBase standalone sobre un PC no hay cluster real: los tiempos de P5 no
  extrapolan linealmente a un cluster con regiones en máquinas distintas. Lo que
  sí extrapola es el **diseño** (16 vs 1 vs 4 regiones) y el hotspot de `ts#est`.
* La ventana de 10 min se recorta solo cuando avanza `tablero:max_ts`; si el
  stream se detiene, las ventanas quedan congeladas (correcto para "ahora").
* La métrica `writeRequestCount` de JMX se publica cada ~10 s; el script duerme
  12 s antes de leerla. Una corrida más rápida puede subestimar escrituras.

## Hardware de las mediciones

Intel Core i5-12500H (12 núcleos / 16 hilos), 16 GB RAM, Windows 11,
Docker Desktop con 4 GB asignados, HBase 2.5.15 y Redis 7 en contenedores
locales. El cliente Python corre en el host.
