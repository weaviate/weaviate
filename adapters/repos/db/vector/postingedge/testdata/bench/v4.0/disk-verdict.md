### Estado

- **Dataset:** dbpedia-openai-1000k-angular.bin, 990000 vectores de 1536 dimensiones, 5000 queries, k = 10.
- **Celdas de posting-edge:** código completo en RAM, 512 bits en RAM, 384 bits en RAM, halo de 512 bits, halo de 512 bits, podado.
- **Cronometradas:** ninguna todavía.
- **HFresh:** construido, sin cronometrar.

### Corrección: cada celda en disco frente a su equivalente en memoria

| celda | recall objetivo | ef | recall | queries comparadas | resultado |
|---|---|---|---|---|---|
| código completo en RAM | 0.95 | 20 | 0.9506 | 5000 | los mismos ids, distancias y trabajo |
| código completo en RAM | 0.99 | 284 | 0.9901 | 5000 | los mismos ids, distancias y trabajo |
| 512 bits en RAM | 0.95 | 21 | 0.9513 | 5000 | los mismos ids, distancias y trabajo |
| 512 bits en RAM | 0.99 | 187 | 0.9900 | 5000 | los mismos ids, distancias y trabajo |
| 384 bits en RAM | 0.95 | 21 | 0.9500 | 5000 | los mismos ids, distancias y trabajo |
| 384 bits en RAM | 0.99 | 164 | 0.9901 | 5000 | los mismos ids, distancias y trabajo |
| halo de 512 bits | 0.95 | 21 | 0.9513 | 5000 | los mismos ids, distancias y trabajo |
| halo de 512 bits | 0.99 | 187 | 0.9900 | 5000 | los mismos ids, distancias y trabajo |
| halo de 512 bits, podado | 0.95 | 25 | 0.9503 | 5000 | los mismos ids, distancias y trabajo |
| halo de 512 bits, podado | 0.99 | 239 | 0.9900 | 5000 | los mismos ids, distancias y trabajo |

La escritura para si alguna query difiere: lo que hay en el manifiesto pasó.

### Criterio 1: lecturas reales frente al modelo de v3.0

Por query, del store en modo pread, que es donde cuenta. El rescore, aparte.

| celda | recall | cargas | rondas | lecturas del store | bytes del store | modelo de v3.0 | real entre modelo | miembros | aristas | halo | rescore: lecturas | bytes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| código completo en RAM | 0.95 | 10.14 | 2.29 | 20.3 | 0.64 MB | 0.64 MB | 1.005 | 0.54 MB | 0.10 MB | — | 64 | 0.39 MB |
| código completo en RAM | 0.99 | 52.61 | 12.91 | 105.2 | 3.27 MB | 3.26 MB | 1.005 | 2.74 MB | 0.53 MB | — | 64 | 0.39 MB |
| 512 bits en RAM | 0.95 | 18.59 | 4.40 | 37.2 | 1.16 MB | 1.16 MB | 1.005 | 0.97 MB | 0.19 MB | — | 64 | 0.39 MB |
| 512 bits en RAM | 0.99 | 76.30 | 18.83 | 152.6 | 4.73 MB | 4.71 MB | 1.005 | 3.96 MB | 0.77 MB | — | 64 | 0.39 MB |
| 384 bits en RAM | 0.95 | 23.68 | 5.67 | 47.4 | 1.48 MB | 1.47 MB | 1.005 | 1.24 MB | 0.24 MB | — | 64 | 0.39 MB |
| 384 bits en RAM | 0.99 | 91.10 | 22.53 | 182.2 | 5.64 MB | 5.62 MB | 1.005 | 4.73 MB | 0.91 MB | — | 64 | 0.39 MB |
| halo de 512 bits | 0.95 | 18.59 | 4.40 | 55.8 | 3.40 MB | 3.39 MB | 1.002 | 0.97 MB | 0.19 MB | 2.24 MB | 64 | 0.39 MB |
| halo de 512 bits | 0.99 | 76.30 | 18.83 | 228.9 | 13.86 MB | 13.84 MB | 1.002 | 3.96 MB | 0.77 MB | 9.13 MB | 64 | 0.39 MB |
| halo de 512 bits, podado | 0.95 | 20.22 | 4.81 | 60.7 | 2.74 MB | 2.74 MB | 1.002 | 1.06 MB | 0.12 MB | 1.57 MB | 64 | 0.39 MB |
| halo de 512 bits, podado | 0.99 | 87.18 | 21.55 | 261.5 | 11.70 MB | 11.67 MB | 1.002 | 4.51 MB | 0.50 MB | 6.69 MB | 64 | 0.39 MB |

**Se cumple:** la celda que más se separa lee 1.005 veces su modelo, y el margen es de 0.75 a 1.25.

### HFresh real

- **Configuración por defecto:** réplicas 4, posting máximo de 48 KB (227 entradas, mínimo 75), searchProbe 256, rescoreLimit 350, distancia cosine.
- **Códigos:** RQ de 1 bit. Una entrada de posting son 209 B: id, versión y código.
- **Rescore:** los mejores candidatos, 350 por defecto, con su vector exacto del bucket `objects`.
- **El build:** 28466 postings y 5194013 entradas, 5.25 por vector. Inserts en 115 s, mantenimiento en 82 s, 422 compactaciones.
- **Segmentos por bucket:** hfresh_postings_hfresh 2, hfresh_shared_hfresh 1, objects 34, vectors_compressed 2.
- **Con su configuración por defecto da un recall de 0.9674.**

**Criterio 4: hasta qué recall llega.** Recall por searchProbe, con cada rescoreLimit; el de por defecto es 350:

| searchProbe | rescoreLimit 64 | rescoreLimit 100 | rescoreLimit 200 | rescoreLimit 350 | queries |
|---|---|---|---|---|---|
| 16 | 0.8322 | 0.8384 | 0.8408 | 0.8410 | 5000 |
| 32 | 0.8725 | 0.8804 | 0.8843 | 0.8847 | 5000 |
| 64 | 0.8996 | 0.9091 | 0.9140 | 0.9148 | 5000 |
| 128 | 0.9275 | 0.9383 | 0.9444 | 0.9456 | 5000 |
| 256 (por defecto) | 0.9471 | 0.9588 | 0.9659 | 0.9674 | 5000 |
| 512 | 0.9606 | 0.9731 | 0.9807 | 0.9824 | 5000 |
| 1024 | 0.9683 | 0.9814 | 0.9893 | 0.9911 | 5000 |
| 2048 | 0.9729 | 0.9863 | 0.9945 | 0.9963 | 5000 |
| 4096 | 0.9748 | 0.9881 | — | 0.9983 | 5000 |
| 8192 | 0.9750 | 0.9890 | — | 0.9988 | 1000 |
| 16384 | 0.9750 | 0.9889 | — | 0.9988 | 1000 |
| 28466 (todos los postings) | 0.9751 | 0.9890 | — | 0.9989 | 1000 |

- **Recall máximo con el rescoreLimit por defecto: 0.9989.**
- Un hueco es un searchProbe que no hizo falta medir: el barrido de un rescoreLimit para cuando llega a 0.99.

**Las candidatas: el menor searchProbe que llega a cada recall, por rescoreLimit.** Se elige la que menos lecturas hace.

| recall | rescoreLimit | searchProbe | recall medido | lecturas por query | postings: lecturas | bytes | rescore: lecturas | bytes | |
|---|---|---|---|---|---|---|---|---|---|
| 0.95 | 64 | 284 | 0.9500 | 386.7 | 322.7 | 11.55 MB | 64.0 | 0.40 MB |  |
| 0.99 | 64 | no llega | — | — | — | — | — | — | |
| 0.95 | 100 | 185 | 0.9500 | 310.2 | 210.2 | 7.52 MB | 100.0 | 0.62 MB | **la mejor** |
| 0.99 | 100 | no llega | — | — | — | — | — | — | |
| 0.95 | 200 | 151 | 0.9501 | 371.5 | 171.5 | 6.14 MB | 200.0 | 1.24 MB |  |
| 0.99 | 200 | 1080 | 0.9900 | 1424.9 | 1224.9 | 44.04 MB | 200.0 | 1.24 MB |  |
| 0.95 | 350 | 145 | 0.9501 | 514.7 | 164.7 | 5.90 MB | 350.0 | 2.16 MB |  |
| 0.99 | 350 | 928 | 0.9900 | 1402.9 | 1052.9 | 37.84 MB | 350.0 | 2.16 MB | **la mejor** |
| por defecto: 0.9674 | 350 | 256 | 0.9674 | 640.9 | 290.9 | 10.41 MB | 350.0 | 2.16 MB | |


### Latencia y QPS, en caliente

Tras una pasada sin cronometrar. La pasada de QPS mediano de las cronometradas.

**Recall 0.95, 1 thread:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|

**Recall 0.95, 8 threads:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|

**Recall 0.99, 1 thread:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|

**Recall 0.99, 8 threads:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|

### Latencia y QPS, en frío

Cada tanda, tras tirar la caché y reabrir el índice, con el índice de los segmentos ya leído.

**Recall 0.95, 1 thread:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|

**Recall 0.95, 8 threads:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|

**Recall 0.99, 1 thread:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|

**Recall 0.99, 8 threads:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|

**Las tandas en frío:**

| índice | recall | threads | tandas | queries por tanda | cargas que repiten posting | p50 de cada mitad, ms | reapertura, ms |
|---|---|---|---|---|---|---|---|

**La primera query de cada tanda frente a las demás,** con 1 thread. La primera es la que menos encuentra en la caché:

| índice | recall | primera: p50, ms | media | las demás: p50, ms | media | rescore de la primera, ms | de las demás |
|---|---|---|---|---|---|---|---|

### Criterios 2 y 3: en frío, frente a HFresh, a 0.95

Se leen contra la mejor configuración de HFresh a 0.95 (enmienda 4). La de por defecto va al lado.

Sin medida en frío de HFresh todavía.

### RAM residente y disco

| índice | heap en uso tras abrir | huella del proceso | disco: store | vectores exactos | lo residente, en ficheros | total |
|---|---|---|---|---|---|---|
| código completo en RAM | — | — | 242.97 MB | 6082.56 MB | 232.47 MB | 6558.00 MB |
| 512 bits en RAM | — | — | 242.97 MB | 6082.56 MB | 105.75 MB | 6431.28 MB |
| 384 bits en RAM | — | — | 242.97 MB | 6082.56 MB | 89.91 MB | 6415.44 MB |
| halo de 512 bits | — | — | 663.42 MB | 6082.56 MB | 34.47 MB | 6780.45 MB |
| halo de 512 bits, podado | — | — | 494.74 MB | 6082.56 MB | 34.47 MB | 6611.77 MB |
| HFresh | — | — | 1296.08 MB | 6221.75 MB | 7.77 MB | 7525.60 MB |

- **Las celdas de posting-edge comparten un solo fichero de vectores,** enlazado: en disco está una vez.
- **El heap es el de Go tras abrir el índice y recolectar.** La caché del sistema no cuenta.
- **En HFresh, «vectores exactos» es su bucket `objects`,** y «store» sus otros buckets.

### Lo que vale y lo que no

- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre índices, medidos igual.
- **Es una sesión,** sin repetir.
- **El p99 en frío sale de las 5 queries más lentas de 500.**
