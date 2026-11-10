# Índice posting+edges en memoria (v1): benchmark end-to-end contra HNSW

**Fecha:** 2026-11-10 · **Rama:** `feat/posting-edge-index` · **Paquete:** `adapters/repos/db/vector/postingedge` (nombre provisional)
**Dataset:** dbpedia-openai-1000k-angular (990 000 × 1536d, cosine, GT exacto top-100), el de las sims de `findings.md`
**Máquina:** Apple M1 Max (8P+2E), 64 GB, Go 1.27, GOMAXPROCS=10, escritorio en uso ligero

## v1.1: optimización del cuello (2026-11-10)

Segunda ronda, sobre el mismo dataset, la misma máquina y el mismo protocolo. El brazo HNSW es el del v1, sin re-medir. El índice se midió sobre el commit `243a571e65`; los defaults se fijaron después, a partir de estas medidas.

**Resultado: entre 1.5× y 1.7× más QPS a igual recall. El factor contra HNSW baja de ~3.2× a ~2.0× con 1 thread.**

Cada índice en su mejor configuración a cada recall, leída de su curva de ef:

| threads | recall | QPS v1 | QPS v1.1 | v1.1 / v1 | QPS HNSW | HNSW / v1 | HNSW / v1.1 |
|---|---|---|---|---|---|---|---|
| 1 | 0.90 | 1028 | 1555 | 1.51x | 3344 | 3.25x | 2.15x |
| 1 | 0.95 | 701 | 1221 | 1.74x | 2315 | 3.30x | 1.90x |
| 1 | 0.99 | 190 | 299 | 1.57x | 600 | 3.16x | 2.01x |
| 8 | 0.90 | 5563 | 9675 | 1.74x | 25006 | 4.49x | 2.58x |
| 8 | 0.95 | 4707 | 7888 | 1.68x | 16576 | 3.52x | 2.10x |
| 8 | 0.99 | 1263 | 1931 | 1.53x | 4353 | 3.45x | 2.25x |

| pre-registro | resultado |
|---|---|
| fase de endpoints ≤ 0.5 ms a iso-0.95 | **Se cumple: 377 µs** (eran 947 µs), con el margen elegido. Sin margen no se cumple: 795 µs. |
| ratio total contra HNSW ≤ 2.2× | **Se cumple a iso-0.95:** 1.90× con 1 thread y 2.10× con 8. A 0.90 y 0.99 queda en 2.15× y 2.01× con 1 thread, y en 2.58× y 2.25× con 8. |

![Recall@10 vs QPS, 1 thread, v1 y v1.1](testdata/bench/v1.1/recall-qps-threads1.svg)

![Recall@10 vs QPS, 8 threads, v1 y v1.1](testdata/bench/v1.1/recall-qps-threads8.svg)

En el punto default (ef=20, margen 0.06, rescore 64, lotes de 4, 1 thread): recall 0.9502, 1 219 QPS, latencia p50 0.70 ms y p95 1.73 ms, 2.3 rondas de media.

### Qué cambió

1. **Corte por margen (`SourceMargin`).** Solo se siguen las aristas de los vectores escaneados cuya estimación queda a menos de un margen del umbral de parada. Es una heurística, no una cota: cuesta recall. Sustituye a la poda triangular, que no poda (adenda más abajo).
2. **Scoring de endpoints por lotes.** Los endpoints de un posting se puntúan en una llamada que pide a la CPU cada código unos pasos por delante, el patrón de `DistancesToNodes`. Bit-idéntico al escalar. En microbenchmark, 49 ns por endpoint frente a 147 ns uno a uno, con el kernel en ~42 ns en caliente.
3. **Defaults** de ef, rescoreLimit y margen, al final de esta sección.

Lo que se probó y no entra:

- **Gather antes de puntuar** (copiar los códigos a un buffer contiguo): más lento que el prefetch por delante, porque la copia añade trabajo que los hints ya hacen innecesario.
- **Caché de las estimaciones re-escaneadas.** Cabe en menos de 20 líneas, pero necesita un array de 4 B/vector por búsqueda en curso y una consulta al visited-set por vector escaneado, para ahorrar como mucho 37 µs de 1.46 ms: las distancias que evita son las secuenciales, las baratas.
- **P menor que 256:** ningún valor del grid llega al recall de arista 0.94.

Cuánto aporta cada cambio, a ef=20 con 1 thread: el scoring por lotes solo, 695 → 781 QPS (+12 %) y la fase de endpoints de 947 a 795 µs; el corte por margen encima, 781 → 1 219 QPS (+56 %) y la fase a 377 µs.

### Corte por margen: calibración a igual ef

Recall contra sin corte, al mismo ef (P=256, rescore 64, lotes de 4, 5000 queries, sin cronometrar):

| M | ef=10 | ef=20 | ef=40 | ef=80 | ef=160 | ef=320 | ef=640 | peor |
|---|---|---|---|---|---|---|---|---|
| 0.04 | -0.0088 | -0.0056 | -0.0030 | -0.0030 | -0.0017 | -0.0007 | -0.0004 | -0.0088, fuera de tolerancia |
| 0.06 | -0.0029 | -0.0009 | -0.0009 | -0.0007 | -0.0004 | -0.0001 | -0.0001 | -0.0029, fuera de tolerancia |
| 0.08 | -0.0007 | -0.0003 | -0.0001 | -0.0000 | -0.0000 | -0.0001 | +0.0000 | -0.0007, dentro |
| 0.10 | -0.0001 | -0.0001 | +0.0000 | +0.0000 | +0.0000 | +0.0000 | +0.0000 | -0.0001, dentro |
| 0.12 | +0.0000 | +0.0000 | +0.0000 | +0.0000 | +0.0000 | +0.0000 | +0.0000 | +0.0000, dentro |

Endpoints puntuados por query:

| M | ef=10 | ef=20 | ef=40 | ef=80 | ef=160 | ef=320 | ef=640 | media sobre sin corte |
|---|---|---|---|---|---|---|---|---|
| sin corte | 8478 | 10944 | 15057 | 21491 | 31861 | 47934 | 73471 | 100% |
| 0.04 | 1529 (18%) | 2476 (23%) | 4000 (27%) | 6433 (30%) | 10376 (33%) | 16841 (35%) | 27686 (38%) | 29% |
| 0.06 | 3319 (39%) | 4953 (45%) | 7526 (50%) | 11577 (54%) | 18126 (57%) | 28749 (60%) | 46314 (63%) | 53% |
| 0.08 | 5291 (62%) | 7461 (68%) | 10936 (73%) | 16359 (76%) | 25167 (79%) | 39215 (82%) | 61947 (84%) | 75% |
| 0.10 | 6831 (81%) | 9280 (85%) | 13257 (88%) | 19446 (90%) | 29468 (92%) | 45141 (94%) | 70150 (95%) | 89% |
| 0.12 | 7757 (91%) | 10288 (94%) | 14426 (96%) | 20870 (97%) | 31238 (98%) | 47314 (99%) | 72830 (99%) | 96% |

- **Con la regla iso-ef pre-registrada el corte no paga:** el menor M dentro de 0.002 de recall en todos los ef es 0.08, y ahorra un 25 % de los endpoints de media, bajo el listón del 40 %.
- **M=0.06 solo sale de tolerancia en ef=10** (−0.0029). De ef=20 en adelante cuesta menos de 0.001 y ahorra entre el 55 % y el 37 %.
- **El diagnóstico previo era optimista.** Estimaba que con M=0.08 se puntuaría el 44 % de los endpoints; se puntúa entre el 62 % y el 84 %. Usaba el umbral final más bajo posible, y la búsqueda corta contra el umbral del momento, más alto al principio y con ef grande.
- El corte casi no cambia qué postings se cargan (10.1 a ef=20 con y sin él): lo que recorta es la puntuación.

### Corte por margen: el default se decide por frontera

El criterio final: el default es el M cuya frontera recall-QPS va al menos igual de rápido que sin corte en todo el rango medido. QPS interpolado a recall 0.90 / 0.95 / 0.99 y extremo rápido (mejor QPS con recall ≥ 0.92), rescore 64, lotes de 4:

| threads | M | QPS a 0.90 | QPS a 0.95 | QPS a 0.99 | extremo rápido | contra sin corte |
|---|---|---|---|---|---|---|
| 1 | sin corte | 950 | 788 | 212 | 950 |  |
| 1 | 0.06 | 1555 (+63.7%) | 1221 (+54.9%) | 299 (+41.0%) | 1555 (+63.7%) | al menos igual de rápido en todo |
| 1 | 0.08 | 1238 (+30.3%) | 939 (+19.1%) | 244 (+15.1%) | 1238 (+30.3%) | al menos igual de rápido en todo |
| 8 | sin corte | 6179 | 5145 | 1379 | 6179 |  |
| 8 | 0.06 | 9675 (+56.6%) | 7888 (+53.3%) | 1931 (+40.0%) | 9675 (+56.6%) | al menos igual de rápido en todo |
| 8 | 0.08 | 7337 (+18.7%) | 5922 (+15.1%) | 1431 (+3.8%) | 7337 (+18.7%) | al menos igual de rápido en todo |

**M=0.06 domina en las ocho comparaciones, por entre un 40 % y un 64 %, y es el default.** M=0.08 también domina, por menos. El margen está en unidades de la distancia: 0.06 vale para cosine. Para l2-squared no hay calibración y el default es sin corte.

### Dónde se va el tiempo a recall 0.95, antes y después

Microsegundos medios por query, 1 thread:

| | ef | rescore | margen | recall | etapa de centroides | scan de postings | puntuación de endpoints | selección de rondas | rescore | otros | total | endpoints puntuados |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| v1 | 20 | 64 | 0.00 | 0.9511 | 196 (13%) | 138 (9%) | 947 (65%) | 5 (0%) | 69 (5%) | 108 (7%) | 1462 | 10944 |
| v1.1 | 20 | 64 | 0.06 | 0.9502 | 181 (21%) | 139 (16%) | 377 (43%) | 3 (0%) | 69 (8%) | 100 (11%) | 869 | 4953 |

- **La puntuación de endpoints sigue siendo la mayor partida, pero ya es el 43 %, no el 65 %.** Son 4 950 endpoints a ~76 ns cada uno, contando el filtrado de aristas.
- **La etapa de centroides pasa a ser la segunda, con el 21 %.** No ha cambiado (181 µs): un HNSW con ef=64 sobre 4 195 centroides f32.
- Scan, rescore y "otros" tampoco han cambiado en términos absolutos: 139, 69 y 100 µs.

### Sweep de P

El constructor de aristas a cada P, sobre una misma partición (42 s) y asignación (133 s):

| P | tiempo de aristas (s) | candidatos por vector | aristas por vector | recall de arista |
|---|---|---|---|---|
| 32 | 53 | 6804 | 8.57 | 0.8044 |
| 64 | 94 | 13164 | 8.78 | 0.8741 |
| 128 | 171 | 25781 | 8.91 | 0.9274 |
| 256 | 330 | 51351 | 8.99 | 0.9609 |

- **P se queda en 256:** es el menor del grid con recall de arista ≥ 0.94. El umbral cae entre 128 y 256; un P intermedio no se midió.
- El tiempo de aristas es lineal en P, así que cada escalón de recall cuesta el doble que el anterior.
- Este sweep corre sobre su propia partición, no la del índice medido; su 0.961 a P=256 coincide con el 0.963 del índice dentro del ruido de la muestra de 1000 vectores.

### Defaults fijados

| parámetro | antes | ahora | por qué |
|---|---|---|---|
| `EF` | 0 (= k) | 20 | el menor ef del sweep con recall ≥ 0.95 (0.9502). A ef=k el recall es 0.922. |
| `RescoreLimit` | 512 | 64 | mismo recall que 512 (0.9511 frente a 0.9512 a ef=20) y 0.45 ms menos por query. |
| `SourceMargin` | 0 (sin corte) | automático: 0.06 para cosine, sin corte para l2-squared | domina la frontera de sin corte en todo el rango medido. |
| `BuildProbe` | 256 | 256 | ningún P menor del grid llega a recall de arista 0.94. |

### Notas

- **El extremo rápido lo limita ef=k.** A 0.90 el factor contra HNSW es 2.15× (2.58× con 8 threads) porque nuestro punto más rápido es ef=10 a recall 0.922 y no hay ninguno más barato. Lotes de 1 no ayudan con el corte puesto (1 485 QPS interpolados a 0.90, frente a 1 555).
- **Ruido:** 2 de los 63 puntos superan el 10 % de dispersión entre pasadas (M=0.08, ef=320, 8 threads; y lotes de 1, M=0.06, ef=10). Ninguno decide el default.
- **Build y memoria no cambian:** 490 s en este run (515 s en el v1, mismas etapas) y 263.5 MB de índice.
- **Lectura de las tablas iso-recall.** Desde esta ronda cada configuración se lee sobre su propia curva de ef y se toma la mejor, en vez de interpolar sobre la unión de todos los puntos, que con tres márgenes inventaba puntos intermedios entre curvas. Las cifras del v1 de más abajo están recalculadas así y se mueven menos de un 3 %.
- Tablas completas de puntos de operación, trabajo por query y memoria del v1.1: `testdata/bench/v1.1/tables.md`. Resultados: `testdata/bench/v1.1/*.json`.

## v1: veredicto contra las expectativas pre-registradas

| | expectativa | resultado |
|---|---|---|
| (a) | memoria del índice (sin f32) claramente bajo HNSW | **Se cumple.** 263.5 MB frente a 420.1 MB (−37 %). Los códigos pesan lo mismo en ambos (200 B/vector); la estructura es 3.1× menor: 79 B/vector (aristas + ids + centroides) frente a 245 B/vector del grafo. |
| (b) | recall ≥ 0.99 alcanzable con ef | **Se cumple, con matiz.** 0.9913 a ef=320 y 0.9945 a ef=640. Pero ef≈2k no "acerca a 1": da 0.951. La curva satura despacio; HNSW tampoco pasa de 0.9975 a ef=1024. |
| (c) | QPS más lento pero misma clase | **~3.2× más lento a iso-recall con 1 thread**, constante de 0.90 a 0.99 (3.25× / 3.30× / 3.16×), y ~3.5× con 8 threads, tomando el mejor punto medido de cada índice. Es un factor fijo, no una curva que se despega; si eso es "misma clase" queda a tu criterio. El cuello es la puntuación de endpoints: 65 % del tiempo a recall 0.95. |

Ningún stop-and-report se disparó: el recall llega a 0.99, y el benchmark no obligó a implementar persistencia (corre in-process).

La incógnita de la fase, la profundidad serial: a recall 0.95 (ef=20, lotes de 4) una query hace **2.3 rondas de media (p50 2, p95 5, máx 10)** tras la carga de entrada y carga 10 postings; a recall 0.99 (ef=320) son 14 rondas de media (p95 29, máx 56) y 58 postings. Con lotes de 1 las rondas se multiplican por 2.7–3.8 a igual ef. En RAM una ronda no cuesta nada (5 µs de selección por query a recall 0.95); el dato importa para la versión en disco, donde cada ronda es un lote de lecturas.

## Qué se midió y cómo

- **Harness in-process**, no server mode: `bench_test.go` (build tag `postingedgebench`), un brazo por proceso para que la memoria de un índice no contamine la del otro. El server mode no exige persistencia, pero sí exponer el tipo de índice en el schema y disparar el build en la primera query; queda fuera del alcance mínimo.
- **Igualdad de condiciones.** Los dos brazos usan la misma cuantización (RQ de 1 bit centrado, media sobre 10 000 vectores), leen los f32 del mismo array en RAM a través del mismo thunk (`TempVectorForIDWithViewThunk`, con copia al contenedor y normalización, como hace HNSW al rescorear), y corren con presupuesto de concurrencia 1 por query: ninguno reparte su rescore entre cores, un thread es un thread.
- **Protocolo.** 5000 queries de test, k=10. Por punto de operación: una pasada de calentamiento y tres medidas; se reporta la mediana de QPS y de cada percentil de latencia. Recall contra el GT exacto del dataset.
- **Brazo posting-edge:** defaults del paquete: posting objetivo de 48 KiB (236 vectores a 1536d), 16 vecinos por vector, P=256, rescate exacto de 64, entrada E=1, lotes B=4. Sweep de ef ∈ {10, 20, 40, 80, 160, 320, 640}.
- **Brazo HNSW:** defaults: maxConnections=32, efConstruction=128; se construye sin comprimir hasta el training limit, se hace el upgrade y el resto se indexa contra los códigos, como en producción. Sweep de ef ∈ {10 … 1024}.
- **Límite de rescore.** HNSW rescorea min(ef, rescoreLimit) candidatos; nuestro índice rescorea siempre los rescoreLimit mejores. Con el default de RQ-1 (512) eso nos cuesta 0.5 ms fijos por query. Por eso cada brazo se midió con 512 y con 64, y la comparación se da de las dos formas: mismo límite (512) y mejor punto de cada índice.
- **Ruido.** La máquina estaba en uso ligero. La dispersión entre las tres pasadas (máx − mín sobre la mediana) supera el 10 % en 1 de los 35 puntos de posting-edge y en 6 de los 44 de HNSW; las tablas de puntos listan el QPS de cada pasada. La mediana absorbe una pasada perturbada, no dos: el punto posting-edge de lotes de 1 y ef=40 (446 / 444 / 556 QPS) está subestimado.

## 1. Recall@10 vs QPS

![Recall@10 vs QPS, 1 thread](testdata/bench/recall-qps-threads1.svg)

![Recall@10 vs QPS, 8 threads](testdata/bench/recall-qps-threads8.svg)

Las dos curvas son paralelas en escala logarítmica: la distancia entre ellas es un factor casi constante, no una pendiente distinta.

Leído de la curva de ef de cada configuración, interpolando linealmente en recall y en log QPS; la mejor configuración de cada índice a cada recall.

| comparación | threads | recall | QPS posting-edge | QPS HNSW | HNSW / posting-edge |
|---|---|---|---|---|---|
| mismo rescore (512), lotes de 4 | 1 | 0.90 | 592 | 3344 | 5.65x |
| mismo rescore (512), lotes de 4 | 1 | 0.95 | 515 | 2315 | 4.50x |
| mismo rescore (512), lotes de 4 | 1 | 0.99 | 177 | 559 | 3.16x |
| mismo rescore (512), lotes de 4 | 8 | 0.90 | 3892 | 25006 | 6.42x |
| mismo rescore (512), lotes de 4 | 8 | 0.95 | 3410 | 16576 | 4.86x |
| mismo rescore (512), lotes de 4 | 8 | 0.99 | 1209 | 3799 | 3.14x |
| mejor punto medido de cada índice | 1 | 0.90 | 1028 | 3344 | 3.25x |
| mejor punto medido de cada índice | 1 | 0.95 | 701 | 2315 | 3.30x |
| mejor punto medido de cada índice | 1 | 0.99 | 190 | 600 | 3.16x |
| mejor punto medido de cada índice | 8 | 0.90 | 5563 | 25006 | 4.49x |
| mejor punto medido de cada índice | 8 | 0.95 | 4707 | 16576 | 3.52x |
| mejor punto medido de cada índice | 8 | 0.99 | 1263 | 4353 | 3.45x |

Lectura:

- Con el mismo límite de rescore (512) el factor crece hacia recall bajo (5.7× a 0.90) porque nuestro rescore fijo de 512 vectores pesa más cuanto más barata es la búsqueda. Con rescore 64 nuestro recall no cambia (0.9511 frente a 0.9512 a ef=20): el estimador de 1 bit centrado ya ordena bien el top, y 512 es puro coste.
- Con 8 threads los dos escalan parecido (HNSW 6.6–7.4×, nosotros 6.5–7.0×) y el factor queda en ~3.5× a 0.95 y 0.99. El 4.5× a 0.90 es un artefacto de la medición: nuestro mejor punto a 0.90 con 1 thread es de lotes de 1, que no se midió con 8 threads.
- Lotes de 1 frente a lotes de 4, en RAM: la curva es prácticamente la misma. A ef=20, lotes de 4 cargan un 40 % más de postings y ganan 1.6 pp de recall (a ef=160, un 9 % más y 0.1 pp), así que a iso-recall se compensan.

Puntos de operación completos, con latencias p50/p95/p99, al final del documento.

## 2. Trabajo por query

**Posting-edge**

Por query. Las distancias a código son los vectores escaneados más los endpoints puntuados.

| ef | rescore | lote | recall | postings cargados media / p50 / p95 | rondas media / p50 / p95 / máx | vectores escaneados | aristas seguidas | a postings ya cargados | endpoints repetidos | endpoints puntuados | distancias a código | distancias full-precision |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 512 | 4 | 0.9251 | 7.6 / 5 / 17 | 1.6 / 1 / 4 / 8 | 1937 | 18458 | 2311 (13%) | 7668 (42%) | 8478 | 10415 | 488 |
| 20 | 512 | 4 | 0.9512 | 10.1 / 9 / 21 | 2.3 / 2 / 5 / 10 | 2579 | 24721 | 3255 (13%) | 10521 (43%) | 10944 | 13523 | 498 |
| 40 | 512 | 4 | 0.9674 | 14.6 / 13 / 29 | 3.4 / 3 / 7 / 12 | 3710 | 35813 | 5051 (14%) | 15705 (44%) | 15057 | 18767 | 505 |
| 80 | 512 | 4 | 0.9791 | 22.2 / 21 / 45 | 5.3 / 5 / 11 / 19 | 5608 | 54438 | 8320 (15%) | 24627 (45%) | 21491 | 27099 | 509 |
| 160 | 512 | 4 | 0.9865 | 35.4 / 33 / 73 | 8.6 / 8 / 18 / 38 | 8894 | 86431 | 14408 (17%) | 40162 (46%) | 31861 | 40755 | 511 |
| 320 | 512 | 4 | 0.9913 | 57.5 / 53 / 117 | 14.1 / 13 / 29 / 56 | 14418 | 139922 | 25516 (18%) | 66471 (48%) | 47934 | 62352 | 512 |
| 640 | 512 | 4 | 0.9945 | 96.0 / 89 / 189 | 23.7 / 22 / 47 / 111 | 23972 | 231455 | 45923 (20%) | 112062 (48%) | 73471 | 97442 | 512 |
| 10 | 64 | 4 | 0.9250 | 7.6 / 5 / 17 | 1.6 / 1 / 4 / 8 | 1937 | 18458 | 2311 (13%) | 7668 (42%) | 8478 | 10415 | 64 |
| 20 | 64 | 4 | 0.9511 | 10.1 / 9 / 21 | 2.3 / 2 / 5 / 10 | 2579 | 24721 | 3255 (13%) | 10521 (43%) | 10944 | 13523 | 64 |
| 40 | 64 | 4 | 0.9673 | 14.6 / 13 / 29 | 3.4 / 3 / 7 / 12 | 3710 | 35813 | 5051 (14%) | 15705 (44%) | 15057 | 18767 | 64 |
| 80 | 64 | 4 | 0.9789 | 22.2 / 21 / 45 | 5.3 / 5 / 11 / 19 | 5608 | 54438 | 8320 (15%) | 24627 (45%) | 21491 | 27099 | 64 |
| 160 | 64 | 4 | 0.9862 | 35.4 / 33 / 73 | 8.6 / 8 / 18 / 38 | 8894 | 86431 | 14408 (17%) | 40162 (46%) | 31861 | 40755 | 64 |
| 320 | 64 | 4 | 0.9910 | 57.5 / 53 / 117 | 14.1 / 13 / 29 / 56 | 14418 | 139922 | 25516 (18%) | 66471 (48%) | 47934 | 62352 | 64 |
| 640 | 64 | 4 | 0.9941 | 96.0 / 89 / 189 | 23.7 / 22 / 47 / 111 | 23972 | 231455 | 45923 (20%) | 112062 (48%) | 73471 | 97442 | 64 |
| 10 | 64 | 1 | 0.8798 | 4.6 / 4 / 9 | 3.6 / 3 / 8 / 16 | 1182 | 11265 | 794 (7%) | 4819 (43%) | 5652 | 6834 | 64 |
| 20 | 64 | 1 | 0.9351 | 7.2 / 7 / 15 | 6.2 / 6 / 14 / 25 | 1849 | 17881 | 1591 (9%) | 7877 (44%) | 8412 | 10262 | 64 |
| 40 | 64 | 1 | 0.9598 | 11.8 / 11 / 25 | 10.8 / 10 / 24 / 38 | 2988 | 29146 | 3149 (11%) | 13235 (45%) | 12762 | 15750 | 64 |
| 80 | 64 | 1 | 0.9752 | 19.5 / 17 / 42 | 18.5 / 16 / 41 / 75 | 4920 | 48147 | 6146 (13%) | 22492 (47%) | 19509 | 24428 | 64 |
| 160 | 64 | 1 | 0.9849 | 32.5 / 29 / 70 | 31.5 / 28 / 69 / 118 | 8181 | 79924 | 11850 (15%) | 38102 (48%) | 29973 | 38154 | 64 |
| 320 | 64 | 1 | 0.9901 | 54.7 / 50 / 114 | 53.7 / 49 / 113 / 219 | 13710 | 133415 | 22522 (17%) | 64630 (48%) | 46263 | 59973 | 64 |
| 640 | 64 | 1 | 0.9939 | 93.1 / 86 / 185 | 92.1 / 85 / 184 / 397 | 23249 | 224858 | 42439 (19%) | 110467 (49%) | 71952 | 95201 | 64 |

**HNSW**

Por query, contadas sobre un segundo build con la misma configuración.

| ef | rescore | recall | distancias a código | distancias full-precision |
|---|---|---|---|---|
| 10 | 512 | 0.7119 | 557 | 10 |
| 16 | 512 | 0.8425 | 707 | 16 |
| 24 | 512 | 0.9052 | 898 | 24 |
| 32 | 512 | 0.9329 | 1079 | 32 |
| 48 | 512 | 0.9564 | 1426 | 48 |
| 64 | 512 | 0.9672 | 1758 | 64 |
| 96 | 512 | 0.9768 | 2389 | 96 |
| 128 | 512 | 0.9824 | 2983 | 128 |
| 192 | 512 | 0.9880 | 4099 | 192 |
| 256 | 512 | 0.9906 | 5146 | 256 |
| 384 | 512 | 0.9934 | 7109 | 384 |
| 512 | 512 | 0.9950 | 8952 | 512 |
| 768 | 512 | 0.9964 | 12397 | 512 |
| 1024 | 512 | 0.9975 | 15632 | 512 |
| 10 | 64 | 0.7119 | 557 | 10 |
| 16 | 64 | 0.8425 | 707 | 16 |
| 24 | 64 | 0.9052 | 898 | 24 |
| 32 | 64 | 0.9329 | 1079 | 32 |
| 48 | 64 | 0.9564 | 1426 | 48 |
| 64 | 64 | 0.9672 | 1758 | 64 |
| 96 | 64 | 0.9767 | 2389 | 64 |
| 128 | 64 | 0.9822 | 2983 | 64 |
| 192 | 64 | 0.9876 | 4099 | 64 |
| 256 | 64 | 0.9903 | 5146 | 64 |
| 384 | 64 | 0.9931 | 7109 | 64 |
| 512 | 64 | 0.9946 | 8952 | 64 |
| 768 | 64 | 0.9961 | 12397 | 64 |
| 1024 | 64 | 0.9971 | 15632 | 64 |

Lectura:

- **Hacemos ~10× más distancias a código que HNSW a igual recall, y tardamos ~3.3× más.** A recall 0.95: 13 500 nuestras (2 600 de scan + 10 900 endpoints, ef=20) frente a 1 430 de HNSW (ef=48). A recall 0.99: 62 400 (ef=320) frente a 5 150 (ef=256). Cada distancia nuestra sale ~3× más barata con todo incluido (~0.11 µs frente a ~0.32 µs por distancia, dividiendo la latencia media entre las distancias): scans secuenciales y fallos de caché solapados frente al recorrido del grafo.
- **Endpoints únicos.** De las aristas que salen de los postings cargados solo se puntúa entre el 46 % (ef=10) y el 32 % (ef=640): un 13–20 % apunta a postings ya cargados y un 42–48 % a endpoints ya puntuados por otra arista. El filtro de duplicados ya está en el código medido; lo nuevo es el conteo.
- **Por posting cargado** se escanean ~255 vectores y se puntúan ~1 080 endpoints (a ef=20). La expansión allsrc son 4× más distancias que el scan y 7× más tiempo.
- **Cola de postings cargados.** El p95 es ~2× la media en todo el rango (21 frente a 10.1 a recall 0.95).
- **Rescore.** Nosotros rescoreamos el límite entero cuando hay candidatos de sobra (64, o ~500 con límite 512: a ef bajo no siempre se han escaneado 512 vectores). HNSW rescorea min(ef, límite).

## 3. Memoria

| índice | componente | tipo | MB | bytes/vector |
|---|---|---|---|---|
| posting-edge | códigos | códigos | 188.8 | 200.0 |
| posting-edge | postings: doc ids y directorio | estructura | 11.3 | 12.0 |
| posting-edge | aristas | estructura | 37.7 | 39.9 |
| posting-edge | centroides: vectores f32 | estructura | 24.6 | 26.0 |
| posting-edge | centroides: grafo, y resto del heap (medido) | estructura | 1.0 | 1.1 |
| posting-edge | **total estructura** |  | 74.7 | 79.1 |
| posting-edge | **total índice (heap medido)** |  | 263.5 | 279.1 |
| posting-edge | vectores full-precision (array compartido) | f32 | 5800.8 | 6144.0 |
| HNSW | códigos | códigos | 188.8 | 200.0 |
| HNSW | grafo, y resto del heap (medido) | estructura | 231.3 | 245.0 |
| HNSW | **total índice (heap medido)** |  | 420.1 | 445.0 |
| HNSW | vectores full-precision (array compartido) | f32 | 5800.8 | 6144.0 |

**Grafo de HNSW por nivel**

De un segundo build con la misma configuración. Los MB empaquetados son las listas de conexiones serializadas, repartidas por número de conexiones.

| nivel | nodos | conexiones | grado medio | MB empaquetados |
|---|---|---|---|---|
| 0 | 990000 | 30993084 | 31.3 | 95.30 |
| 1 | 31035 | 739850 | 23.8 | 2.27 |
| 2 | 986 | 19619 | 19.9 | 0.06 |
| 3 | 32 | 992 | 31.0 | 0.00 |
| **todos** | | 31753545 | | 97.6 (103.4 bytes/vector) |

**Memoria del proceso**

RSS según `ps`; en macOS sigue contando las páginas que el runtime de Go ya liberó como reutilizables, cosa que el footprint físico no hace.

| índice | cuándo | RSS MB | footprint MB | heap vivo MB |
|---|---|---|---|---|
| posting-edge | dataset cargado | 5906 | 5885 | 5863 |
| posting-edge | tras el build | 13063 | 6230 | 6126 |
| posting-edge | tras 1000 queries | 13501 | 6239 | 6136 |
| posting-edge | pico durante el build | 13063 | — | — |
| HNSW | dataset cargado | 5906 | 5885 | 5863 |
| HNSW | tras el build | 13233 | 7754 | 6283 |
| HNSW | tras 1000 queries | 13233 | 7771 | 6305 |
| HNSW | pico durante el build | 13014 | — | — |

Lectura:

- **Códigos frente a estructura.** Los códigos son los mismos en ambos índices: 200 B/vector. Toda la diferencia es estructural: nuestras aristas pesan 39.9 B/vector (8.98 aristas de 4 B más un offset), los ids y el directorio 12.0, y los centroides 27.1 (4195 vectores f32 de 1536d, 24.6 MB, más su grafo, que cabe en ~1 MB). El grafo de HNSW pesa 245 B/vector.
- **Aristas frente a grafo.** Guardamos 9.0 aristas por vector a 4 B (35.9 B/vector); HNSW guarda 32.1 conexiones por vector, que empaquetadas ocupan 103 B/vector (97.6 MB, el 98 % en el nivel 0). El resto hasta los 245 B/vector medidos son los vértices, sus locks, las cabeceras de slice de la caché de códigos y el redondeo del allocator: el grafo de HNSW está medido como el heap del índice menos los códigos.
- Un posting nuestro (ids + códigos) son 212 B/vector, de los que 200 son los códigos.
- **Con f32 la diferencia se diluye:** 5.8 GB de vectores dominan en ambos. El ahorro estructural importa cuando los f32 no están en RAM, que es el caso de producción y el de la versión en disco.
- **RSS.** En macOS el RSS de `ps` cuenta las páginas que el runtime de Go ya devolvió como reutilizables, y ambos procesos arrastran ahí los transitorios del build. El footprint físico es la cifra que refleja lo que el proceso ocupa. Tras el build, posting-edge ocupa 6 230 MB de footprint (6 126 MB de heap vivo) y HNSW 7 754 MB (6 283 MB de heap vivo); 1000 queries después, 6 239 y 7 771 MB. La diferencia de footprint (1.5 GB) es mucho mayor que la del heap vivo (157 MB): el proceso de HNSW tiene ~570 MB de fragmentación (6 849 MB de heap en uso frente a 6 283 MB vivos) y el resto no lo he atribuido.

## 4. Indexado

Los dos builds corren con 10 threads (GOMAXPROCS). Una sola ejecución de cada uno.

| índice | etapa | segundos | parte |
|---|---|---|---|
| posting-edge | aristas | 342.0 | 66% |
| posting-edge | asignación exacta | 125.2 | 24% |
| posting-edge | partición | 44.0 | 9% |
| posting-edge | AddBatch | 1.8 | 0% |
| posting-edge | códigos (media + encode) | 1.4 | 0% |
| posting-edge | HNSW de centroides | 0.2 | 0% |
| posting-edge | **total** | **514.6** | |
| HNSW | inserción contra códigos | 123.5 | 99% |
| HNSW | inserción sin comprimir (10 000 vectores) | 1.6 | 1% |
| HNSW | upgrade a RQ | 0.1 | 0% |
| HNSW | **total** | **125.1** | |

Lectura:

- Nuestro build es 4.1× más lento que el de HNSW, y dos tercios son la construcción de aristas. Son ejecuciones únicas en una máquina en uso: el segundo build de posting-edge (el de la pasada de trabajo) tardó 629 s, un 22 % más, con la asignación en 188 s.
- **Aristas (342 s).** Con P=256 y postings de 236 vectores, cada vector puntúa 51 500 candidatos (en las sims, con postings de 38, eran 9 800). El recall de arista sale 0.963, por encima del 0.93 que las sims dieron por suficiente. P es la palanca obvia de v2, sin medir aquí.
- **Asignación exacta (125 s).** Son n·k distancias: cuadrática en n a tamaño de posting fijo. A 10M serían ~100× más.
- **Partición (44 s).** Los primeros niveles de la bisección son seriales: un `FitBalanced` sobre 990k vectores acumula sus centroides en float64 en un solo thread.
- El pico de memoria del build incluye una copia normalizada de todos los vectores (5.8 GB) y las listas de centroides cercanos (1 GB), que se sueltan al terminar.

## 5. Dónde se va el tiempo a recall 0.95

Microsegundos medios por query con 1 thread, en el primer punto de cada configuración que alcanza ese recall. "Otros" es lo que las fases no cubren: preparar la query, tomar el scratch, montar el resultado.

| rescore | lote | ef | recall | etapa de centroides | scan de postings | puntuación de endpoints | selección de rondas | rescore | otros | total |
|---|---|---|---|---|---|---|---|---|---|---|
| 512 | 4 | 20 | 0.9512 | 209 (10%) | 186 (9%) | 952 (47%) | 5 (0%) | 515 (26%) | 142 (7%) | 2008 |
| 64 | 4 | 20 | 0.9511 | 196 (13%) | 138 (9%) | 947 (65%) | 5 (0%) | 69 (5%) | 108 (7%) | 1462 |
| 64 | 1 | 40 | 0.9598 | 194 (12%) | 160 (10%) | 1094 (66%) | 17 (1%) | 68 (4%) | 116 (7%) | 1648 |

Lectura, como entrada para la lista de v2 (aquí no se optimizó nada más):

1. **Puntuación de endpoints: 947 µs, 65 %** (con rescore 64). Son 10 900 endpoints a ~87 ns cada uno; el kernel de distancia cuesta 42 ns en caliente, el resto son accesos dispersos (el código del endpoint, el directorio slot→posting, el conjunto de visitados). Es la partida que decide el factor contra HNSW.
2. **Etapa de centroides: 196 µs, 13 %**, fija por query e independiente de ef: un HNSW con ef=64 sobre 4 195 centroides f32 de 1536d. A ef bajo pesa más que el scan.
3. **Scan de postings: 138 µs, 9 %**: 2 600 códigos a ~54 ns, secuenciales.
4. **Rescore: 69 µs, 5 %** con límite 64 (~1.1 µs por vector: copia de 6 KB, normalización y distancia); con límite 512 son 515 µs, el 26 %.
5. **Selección de rondas: 5 µs.** Despreciable en RAM, también con lotes de 1 (17 µs).
6. **Otros: ~110 µs, 7 %**: preparar el estimador de la query (rotación y código de 5 bits), scratch y resultados. Incluye ~25 µs de contabilidad de stats: la latencia media del mismo punto en la pasada cronometrada es 1 439 µs, frente a los 1 462 µs de esta.

## Estructura resultante

| | |
|---|---|
| postings | 4195 |
| tamaño de posting: media / mín / p50 / p95 / máx | 236 / 2 / 234 / 380 / 786 |
| aristas por vector | 8.98 |
| vecinos (top-16 exacto) que ya están en el posting propio | 43 % |
| recall de arista (muestra de 1000 vectores, vecinos fuera del posting) | 0.963 |
| candidatos puntuados por vector al construir aristas | 51 523 |
| la entrada del grafo de centroides es el centroide exacto más cercano | 99.98 % de las queries |

- La bisección deja hojas equilibradas, pero la reasignación exacta al centroide más cercano no lo está: hay postings de 2 vectores y de 786.
- El grafo de centroides no es un problema de calidad (99.98 %), sí de coste: ver sección 5.

## Notas de implementación que afectan a los números

- **Pasada de "touch" antes de puntuar endpoints.** Los endpoints están dispersos por los 189 MB de códigos. Antes de puntuarlos se lee una palabra por línea de caché de cada código para que los fallos de caché se solapen. En un microbenchmark con ese tamaño baja el coste por endpoint de 182 ns a 62 ns. No cambia qué se puntúa. HNSW ya hace lo equivalente con su distancer por lotes.
- **Qué filtraba ya el run medido.** Desde el primer commit de la rama (`6a31ca8c9c`, el del run cronometrado) la expansión descarta las aristas que llevan a postings ya cargados y puntúa cada endpoint una sola vez por búsqueda (visited-set). Lo que no hay es estimación cacheada: la estimación de un endpoint solo se pliega en el rank de su posting, y si ese posting se carga después, el scan la vuelve a calcular. Le pasa al 27 % de los vectores escaneados a recall 0.95 (692 de 2 579; del 24 % a ef=10 al 47 % a ef=640), es decir, al 5 % de las distancias a código de la query.
- **Tamaño de posting.** Se pidió posting medio ≈ HFresh (~230). Las sims corrieron con postings primarios de 38 de media, así que sus cifras de cargas no son comparables una a una con estas: aquí cada carga trae 6× más vectores.
- **Código medido.** Los QPS de posting-edge se midieron sobre el commit `6a31ca8c9c`. Después se añadieron, sin re-medir: contadores y un temporizador que solo corren con stats activadas, y la validación de vectores y queries no finitos (un producto escalar de 1536d por query, ~70 ns sobre ~1 ms). Los QPS de HNSW se midieron con el harness ya ampliado; el código de HNSW no se tocó.
- **Segunda pasada.** El trabajo por query, el perfil por fases y la memoria del proceso de posting-edge salen de un segundo build con el mismo seed y sin pasadas cronometradas de QPS. Reproduce el primero: mismos postings, mismas aristas, y recall y postings cargados idénticos en los 21 puntos. Las distancias por query y el grafo por nivel de HNSW salen de un segundo build de HNSW, que no es idéntico al medido (la inserción es concurrente); su recall coincide con el del build medido a ±0.002.

## Decisiones abiertas

- Los defaults de ef, rescoreLimit y P quedaron fijados en el v1.1 (arriba).
- **Nombre del paquete y del tipo de índice** (`postingedge`), al abrir el PR.

## Adenda v1.1: la poda triangular por source no poda (gate, 2026-11-10)

La ronda v1.1 proponía saltar un source cuando `dist(q, source) − L_max(source) ≥ dist del ef-ésimo candidato`, con L_max la longitud de su arista más larga. El gate pre-registrado era parar si poda menos del 20 % de los sources. **Poda el 0.1 %**, así que se descartó. La sustituyó el corte por margen del v1.1.

No se implementó en el índice: se midió antes su mejor caso con un diagnóstico del harness (`-bench.reach`, 500 queries sobre el índice de 1M). El mejor caso es L_max exacto y sin cuantizar, y el umbral más bajo que una búsqueda puede alcanzar (la ef-ésima mejor estimación de todo el corpus). La cota se evalúa en ángulos: la distancia coseno no cumple la desigualdad triangular, el ángulo sí, y es la métrica más ajustada.

| sources podables, mejor caso | ef = k | ef = 2k |
|---|---|---|
| en los 16 postings más cercanos a la query | 0.10 % | 0.05 % |
| en todo el corpus | 0.49 % | 0.23 % |
| restando distancias coseno directamente (no es una cota), 16 postings | 4.6 % | 3.0 % |

Distribuciones, en distancia coseno:

| | mín | p1 | p5 | p25 | p50 | p75 | p95 | p99 |
|---|---|---|---|---|---|---|---|---|
| L_max por source | 0.009 | 0.082 | 0.115 | 0.141 | 0.155 | 0.169 | 0.186 | 0.197 |
| longitud de arista | | | 0.103 | | 0.150 | | 0.182 | |
| distancia estimada query→source, 16 postings más cercanos | | | 0.176 | | 0.231 | | 0.285 | 0.306 |
| umbral de parada (10.º mejor candidato) | | | | | 0.149 | | 0.178 | |

Lectura:

- **Las tres escalas coinciden.** Las aristas miden lo mismo que la distancia de la query a su décimo vecino (0.15), y los sources de los postings cargados están solo un poco más lejos (0.18–0.31). Con L_max y umbral medianos, un source es podable si está a distancia coseno ≥ 0.56 de la query; el p99 de los sources cercanos es 0.31.
- **No son aristas largas que se puedan acortar.** Son los vecinos más cercanos fuera del posting (recall de arista 0.963). Es la concentración de distancias de 1536d, la misma que en `findings.md` it1 hacía que "todo sea frontera".
- L_max en grados: p5 27.7°, p50 32.4°, p95 35.5°.

**Lo que el mismo diagnóstico sugiere en su lugar.** Un corte simple por distancia del source, sin cota: expandir solo los sources cuya estimación queda a menos de un margen del umbral. "Útil" es un endpoint fuera de los 16 postings más cercanos cuya estimación entra bajo el umbral, es decir, uno cuyo posting hay que cargar. Son 1.0 por query a ef=k y 2.3 a ef=2k, de 13 100 endpoints distintos alcanzables.

| margen sobre el umbral (coseno) | sources que se expanden | aristas seguidas | endpoints puntuados | endpoints útiles conservados, ef=k | ef=2k |
|---|---|---|---|---|---|
| 0.04 | 7 % | 4 % | 6 % | 90.8 % | 92.8 % |
| 0.06 | 23 % | 17 % | 21 % | 96.7 % | 97.7 % |
| 0.08 | 47 % | 40 % | 44 % | 99.2 % | 99.1 % |
| 0.10 | 70 % | 64 % | 67 % | 99.8 % | 99.9 % |
| 0.15 | 96 % | 95 % | 95 % | 100 % | 100 % |

- Con margen 0.08 se puntuaría el 44 % de los endpoints conservando el 99.2 % de los útiles; con 0.10, el 67 % y el 99.8 %.
- **Es una heurística, no una cota.** El diagnóstico usa el umbral final y los 16 postings más cercanos como aproximación de los cargados, y resultó optimista: la medición real está en la sección del v1.1.

Datos: `testdata/bench/dbpedia-1m-postingedge-reach.json`.

## Lo que este benchmark no cubre

- Disco: todo está en RAM. Las rondas y los bytes por carga están contados, no su latencia de I/O.
- Otras escalas y datasets: solo 1M × 1536d.
- Sensibilidad a P, al tamaño de posting, a E y a efConstruction/maxConnections de HNSW: un solo valor de cada uno.
- Filtros, borrados, updates e inserción incremental: no existen en v1.

## Reproducción

```bash
D=~/Documents/datasets/dbpedia-openai-1000k-angular.bin   # meta.txt, train.f32, test.f32, neighbors.i32
B=$PWD/adapters/repos/db/vector/postingedge/testdata/bench

# brazo posting-edge, cronometrado
go test -tags postingedgebench -run 'TestBenchmark$' -timeout 0 -v ./adapters/repos/db/vector/postingedge/ \
  -bench.dataset $D -bench.arm postingedge -bench.queries 5000 -bench.runs 3 \
  -bench.efs 10,20,40,80,160,320,640 \
  -bench.sweep "rescore=512 batch=4 threads=1,8" -bench.sweep "rescore=64 batch=4 threads=1,8" \
  -bench.sweep "rescore=64 batch=1 threads=1" -bench.out $B/dbpedia-1m-postingedge.json

# brazo HNSW, cronometrado
go test -tags postingedgebench -run 'TestBenchmark$' -timeout 0 -v ./adapters/repos/db/vector/postingedge/ \
  -bench.dataset $D -bench.arm hnsw -bench.queries 5000 -bench.runs 3 \
  -bench.efs 10,16,24,32,48,64,96,128,192,256,384,512,768,1024 \
  -bench.sweep "rescore=512 threads=1,8" \
  -bench.sweep "rescore=64 threads=1,8 ef=96,128,192,256,384,512,768,1024" -bench.out $B/dbpedia-1m-hnsw.json

# trabajo por query, perfil y memoria de posting-edge (sin cronometrar QPS)
go test -tags postingedgebench -run 'TestBenchmark$' -timeout 0 -v ./adapters/repos/db/vector/postingedge/ \
  -bench.dataset $D -bench.arm postingedge -bench.mode work -bench.queries 5000 \
  -bench.efs 10,20,40,80,160,320,640 \
  -bench.sweep "rescore=512 batch=4 threads=1" -bench.sweep "rescore=64 batch=4 threads=1" \
  -bench.sweep "rescore=64 batch=1 threads=1" -bench.out $B/dbpedia-1m-postingedge-work.json

# distancias por query y grafo por nivel de HNSW
go test -tags postingedgebench -run 'TestSearchWork$' -timeout 0 -v ./adapters/repos/db/vector/hnsw/ \
  -work.dataset $D -work.queries 5000 -work.rescores 512,64 -work.out $B/dbpedia-1m-hnsw-work.json

# tablas y gráficos de este informe
go test -tags postingedgebench -run 'TestBenchmarkReport$' ./adapters/repos/db/vector/postingedge/ \
  -bench.report.postingedge $B/dbpedia-1m-postingedge.json -bench.report.hnsw $B/dbpedia-1m-hnsw.json \
  -bench.report.postingedge.work $B/dbpedia-1m-postingedge-work.json \
  -bench.report.hnsw.work $B/dbpedia-1m-hnsw-work.json -bench.report.out $B
```

Los JSON de resultados, `tables.md` y los SVG están en `testdata/bench/`.

Las medidas del v1.1, con `V=$B/v1.1`:

```bash
# calibración del margen a igual ef (sin cronometrar)
go test -tags postingedgebench -run 'TestBenchmark$' -timeout 0 -v ./adapters/repos/db/vector/postingedge/ \
  -bench.dataset $D -bench.arm postingedge -bench.mode work -bench.queries 5000 \
  -bench.efs 10,20,40,80,160,320,640 \
  -bench.sweep "margin=0,0.04,0.06,0.08,0.10,0.12 rescore=64 batch=4 threads=1" -bench.out $V/dbpedia-1m-margin.json

# el constructor de aristas a cada P
go test -tags postingedgebench -run 'TestBenchmark$' -timeout 0 -v ./adapters/repos/db/vector/postingedge/ \
  -bench.dataset $D -bench.arm postingedge -bench.mode probes -bench.probes 32,64,128,256 \
  -bench.out $V/dbpedia-1m-probes.json

# sweep cronometrado
go test -tags postingedgebench -run 'TestBenchmark$' -timeout 0 -v ./adapters/repos/db/vector/postingedge/ \
  -bench.dataset $D -bench.arm postingedge -bench.queries 5000 -bench.runs 3 \
  -bench.efs 10,20,40,80,160,320,640 \
  -bench.sweep "margin=0,0.06,0.08 rescore=64 batch=4 threads=1,8" \
  -bench.sweep "margin=0 rescore=512 batch=4 threads=1" \
  -bench.sweep "margin=0,0.06 rescore=64 batch=1 threads=1" -bench.out $V/dbpedia-1m-postingedge.json

# tablas y gráficos, contra el v1 y el HNSW ya medido
go test -tags postingedgebench -run 'TestBenchmarkReport$' ./adapters/repos/db/vector/postingedge/ \
  -bench.report.postingedge $V/dbpedia-1m-postingedge.json -bench.report.hnsw $B/dbpedia-1m-hnsw.json \
  -bench.report.hnsw.work $B/dbpedia-1m-hnsw-work.json \
  -bench.report.before $B/dbpedia-1m-postingedge.json -bench.report.before.work $B/dbpedia-1m-postingedge-work.json \
  -bench.report.margin $V/dbpedia-1m-margin.json -bench.report.probes $V/dbpedia-1m-probes.json \
  -bench.report.out $V
```

## Puntos de operación

| ef | rescore | lote | threads | recall@10 | QPS | QPS de cada pasada | media ms | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 512 | 4 | 1 | 0.9251 | 592 | 591 / 592 / 592 | 1.69 | 1.54 | 3.04 | 3.80 |
| 20 | 512 | 4 | 1 | 0.9512 | 511 | 509 / 511 / 512 | 1.96 | 1.78 | 3.59 | 4.38 |
| 40 | 512 | 4 | 1 | 0.9674 | 416 | 416 / 417 / 416 | 2.40 | 2.14 | 4.55 | 5.51 |
| 80 | 512 | 4 | 1 | 0.9791 | 318 | 318 / 319 / 318 | 3.14 | 2.79 | 6.22 | 7.60 |
| 160 | 512 | 4 | 1 | 0.9865 | 231 | 231 / 231 / 231 | 4.32 | 3.87 | 8.72 | 10.64 |
| 320 | 512 | 4 | 1 | 0.9913 | 161 | 161 / 162 / 160 | 6.21 | 5.61 | 12.59 | 15.43 |
| 640 | 512 | 4 | 1 | 0.9945 | 106 | 103 / 106 / 106 | 9.40 | 8.74 | 18.66 | 22.92 |
| 10 | 512 | 4 | 8 | 0.9251 | 3892 | 3892 / 3752 / 3978 | 2.05 | 1.86 | 3.71 | 4.76 |
| 20 | 512 | 4 | 8 | 0.9512 | 3388 | 3426 / 3378 / 3388 | 2.36 | 2.14 | 4.37 | 5.35 |
| 40 | 512 | 4 | 8 | 0.9674 | 2729 | 2729 / 2735 / 2710 | 2.93 | 2.59 | 5.63 | 7.33 |
| 80 | 512 | 4 | 8 | 0.9791 | 2147 | 2142 / 2147 / 2171 | 3.72 | 3.28 | 7.46 | 9.10 |
| 160 | 512 | 4 | 8 | 0.9865 | 1579 | 1579 / 1560 / 1583 | 5.06 | 4.50 | 10.30 | 12.75 |
| 320 | 512 | 4 | 8 | 0.9913 | 1098 | 1110 / 1098 / 1076 | 7.27 | 6.52 | 15.00 | 18.58 |
| 640 | 512 | 4 | 8 | 0.9945 | 719 | 733 / 719 / 703 | 11.11 | 10.29 | 22.33 | 28.01 |
| 10 | 64 | 4 | 1 | 0.9250 | 855 | 854 / 855 / 856 | 1.17 | 1.00 | 2.46 | 3.21 |
| 20 | 64 | 4 | 1 | 0.9511 | 695 | 695 / 694 / 698 | 1.44 | 1.25 | 3.06 | 3.81 |
| 40 | 64 | 4 | 1 | 0.9673 | 529 | 529 / 530 / 529 | 1.89 | 1.61 | 4.03 | 5.02 |
| 80 | 64 | 4 | 1 | 0.9789 | 383 | 378 / 385 / 383 | 2.61 | 2.26 | 5.68 | 7.04 |
| 160 | 64 | 4 | 1 | 0.9862 | 257 | 256 / 260 / 257 | 3.89 | 3.40 | 8.40 | 10.43 |
| 320 | 64 | 4 | 1 | 0.9910 | 176 | 175 / 176 / 176 | 5.69 | 5.10 | 12.08 | 14.92 |
| 640 | 64 | 4 | 1 | 0.9941 | 108 | 105 / 108 / 112 | 9.23 | 8.46 | 19.07 | 24.28 |
| 10 | 64 | 4 | 8 | 0.9250 | 5563 | 5563 / 5731 / 5563 | 1.44 | 1.21 | 3.04 | 4.23 |
| 20 | 64 | 4 | 8 | 0.9511 | 4671 | 4681 / 4650 / 4671 | 1.71 | 1.47 | 3.66 | 4.66 |
| 40 | 64 | 4 | 8 | 0.9673 | 3611 | 3611 / 3624 / 3487 | 2.21 | 1.89 | 4.74 | 6.23 |
| 80 | 64 | 4 | 8 | 0.9789 | 2615 | 2611 / 2615 / 2632 | 3.06 | 2.61 | 6.68 | 8.52 |
| 160 | 64 | 4 | 8 | 0.9862 | 1787 | 1809 / 1787 / 1759 | 4.47 | 3.89 | 9.79 | 12.35 |
| 320 | 64 | 4 | 8 | 0.9910 | 1158 | 1205 / 1158 / 1140 | 6.90 | 6.09 | 14.87 | 18.88 |
| 640 | 64 | 4 | 8 | 0.9941 | 753 | 779 / 753 / 751 | 10.61 | 9.78 | 22.00 | 27.41 |
| 10 | 64 | 1 | 1 | 0.8798 | 1142 | 1094 / 1142 / 1172 | 0.88 | 0.77 | 1.74 | 2.14 |
| 20 | 64 | 1 | 1 | 0.9351 | 858 | 853 / 869 / 858 | 1.17 | 1.00 | 2.51 | 3.01 |
| 40 | 64 | 1 | 1 | 0.9598 | 446 | 446 / 444 / 556 | 2.24 | 1.65 | 5.08 | 8.66 |
| 80 | 64 | 1 | 1 | 0.9752 | 392 | 382 / 392 / 396 | 2.55 | 2.13 | 5.73 | 7.03 |
| 160 | 64 | 1 | 1 | 0.9849 | 268 | 270 / 268 / 263 | 3.73 | 3.20 | 8.37 | 10.25 |
| 320 | 64 | 1 | 1 | 0.9901 | 169 | 170 / 169 / 169 | 5.90 | 5.20 | 12.85 | 15.83 |
| 640 | 64 | 1 | 1 | 0.9939 | 107 | 107 / 107 / 107 | 9.32 | 8.55 | 19.48 | 24.05 |

| ef | rescore | threads | recall@10 | QPS | QPS de cada pasada | media ms | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|---|---|---|---|
| 10 | 512 | 1 | 0.7118 | 5203 | 5379 / 5203 / 5135 | 0.19 | 0.19 | 0.28 | 0.34 |
| 16 | 512 | 1 | 0.8438 | 4107 | 3927 / 4310 / 4107 | 0.24 | 0.23 | 0.34 | 0.48 |
| 24 | 512 | 1 | 0.9049 | 3285 | 3285 / 3163 / 3560 | 0.30 | 0.29 | 0.42 | 0.50 |
| 32 | 512 | 1 | 0.9329 | 2786 | 2784 / 2859 / 2786 | 0.36 | 0.35 | 0.49 | 0.56 |
| 48 | 512 | 1 | 0.9572 | 2141 | 2130 / 2141 / 2190 | 0.47 | 0.46 | 0.64 | 0.72 |
| 64 | 512 | 1 | 0.9679 | 1663 | 1627 / 1723 / 1663 | 0.60 | 0.58 | 0.83 | 0.99 |
| 96 | 512 | 1 | 0.9778 | 1233 | 1229 / 1249 / 1233 | 0.81 | 0.80 | 1.11 | 1.24 |
| 128 | 512 | 1 | 0.9829 | 949 | 995 / 949 / 782 | 1.05 | 1.03 | 1.48 | 1.70 |
| 192 | 512 | 1 | 0.9878 | 672 | 649 / 672 / 676 | 1.49 | 1.46 | 2.07 | 2.26 |
| 256 | 512 | 1 | 0.9908 | 524 | 524 / 525 / 519 | 1.91 | 1.87 | 2.66 | 2.90 |
| 384 | 512 | 1 | 0.9937 | 383 | 381 / 384 / 383 | 2.61 | 2.57 | 3.63 | 4.00 |
| 512 | 512 | 1 | 0.9951 | 299 | 299 / 299 / 238 | 3.35 | 3.31 | 4.66 | 5.06 |
| 768 | 512 | 1 | 0.9964 | 209 | 203 / 209 / 211 | 4.77 | 4.70 | 6.75 | 7.48 |
| 1024 | 512 | 1 | 0.9975 | 180 | 166 / 180 / 187 | 5.54 | 5.51 | 7.81 | 8.58 |
| 10 | 512 | 8 | 0.7118 | 39949 | 39949 / 40068 / 39924 | 0.20 | 0.19 | 0.29 | 0.37 |
| 16 | 512 | 8 | 0.8438 | 30966 | 31464 / 30786 / 30966 | 0.26 | 0.25 | 0.37 | 0.51 |
| 24 | 512 | 8 | 0.9049 | 24546 | 24699 / 24381 / 24546 | 0.33 | 0.32 | 0.45 | 0.55 |
| 32 | 512 | 8 | 0.9329 | 20222 | 20222 / 20372 / 20173 | 0.39 | 0.38 | 0.54 | 0.66 |
| 48 | 512 | 8 | 0.9572 | 15247 | 15287 / 15247 / 15177 | 0.52 | 0.51 | 0.71 | 0.88 |
| 64 | 512 | 8 | 0.9679 | 12181 | 12181 / 12148 / 12287 | 0.66 | 0.64 | 0.90 | 1.14 |
| 96 | 512 | 8 | 0.9778 | 8525 | 8827 / 8525 / 8383 | 0.94 | 0.89 | 1.33 | 1.98 |
| 128 | 512 | 8 | 0.9829 | 6852 | 7038 / 6803 / 6852 | 1.17 | 1.11 | 1.66 | 2.65 |
| 192 | 512 | 8 | 0.9878 | 5024 | 4801 / 5032 / 5024 | 1.59 | 1.56 | 2.20 | 2.58 |
| 256 | 512 | 8 | 0.9908 | 3435 | 3421 / 3435 / 3447 | 2.33 | 2.11 | 4.08 | 5.52 |
| 384 | 512 | 8 | 0.9937 | 2464 | 2535 / 2464 / 2400 | 3.24 | 3.08 | 5.17 | 6.58 |
| 512 | 512 | 8 | 0.9951 | 1910 | 1910 / 1916 / 1859 | 4.19 | 3.98 | 6.59 | 8.25 |
| 768 | 512 | 8 | 0.9964 | 1458 | 1468 / 1458 / 1456 | 5.48 | 5.26 | 8.39 | 10.84 |
| 1024 | 512 | 8 | 0.9975 | 1292 | 1242 / 1313 / 1292 | 6.18 | 6.06 | 8.91 | 10.92 |
| 96 | 64 | 1 | 0.9776 | 1317 | 1311 / 1317 / 1318 | 0.76 | 0.75 | 1.05 | 1.18 |
| 128 | 64 | 1 | 0.9826 | 1030 | 1073 / 1010 / 1030 | 0.97 | 0.95 | 1.38 | 1.59 |
| 192 | 64 | 1 | 0.9875 | 748 | 761 / 748 / 733 | 1.34 | 1.30 | 1.95 | 2.21 |
| 256 | 64 | 1 | 0.9904 | 577 | 587 / 558 / 577 | 1.73 | 1.69 | 2.53 | 2.95 |
| 384 | 64 | 1 | 0.9933 | 424 | 424 / 438 / 420 | 2.36 | 2.30 | 3.51 | 4.11 |
| 512 | 64 | 1 | 0.9947 | 349 | 343 / 349 / 349 | 2.86 | 2.83 | 4.23 | 4.66 |
| 768 | 64 | 1 | 0.9960 | 265 | 265 / 265 / 257 | 3.78 | 3.73 | 5.53 | 6.24 |
| 1024 | 64 | 1 | 0.9971 | 197 | 197 / 195 / 198 | 5.08 | 5.01 | 7.43 | 8.38 |
| 96 | 64 | 8 | 0.9776 | 8612 | 8619 / 8585 / 8612 | 0.93 | 0.87 | 1.50 | 1.98 |
| 128 | 64 | 8 | 0.9826 | 7213 | 7169 / 7382 / 7213 | 1.11 | 1.05 | 1.71 | 2.28 |
| 192 | 64 | 8 | 0.9875 | 4965 | 5534 / 4965 / 4819 | 1.61 | 1.51 | 2.69 | 3.50 |
| 256 | 64 | 8 | 0.9904 | 4252 | 4026 / 4252 / 4318 | 1.88 | 1.78 | 3.01 | 3.64 |
| 384 | 64 | 8 | 0.9933 | 2967 | 2970 / 2913 / 2967 | 2.69 | 2.55 | 4.42 | 6.08 |
| 512 | 64 | 8 | 0.9947 | 2478 | 2442 / 2574 / 2478 | 3.23 | 3.12 | 4.93 | 6.12 |
| 768 | 64 | 8 | 0.9960 | 1665 | 1665 / 1631 / 1688 | 4.80 | 4.59 | 7.77 | 9.71 |
| 1024 | 64 | 8 | 0.9971 | 1419 | 1437 / 1419 / 1170 | 5.63 | 5.52 | 8.43 | 10.22 |
