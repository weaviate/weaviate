### Estado

- **Dataset:** dbpedia-openai-1000k-angular.bin, 990000 vectores de 1536 dimensiones, 5000 queries, k = 10.
- **Celdas de posting-edge:** código completo en RAM, 512 bits en RAM, 384 bits en RAM, halo de 512 bits, halo de 512 bits, podado.
- **Cronometradas:** ninguna todavía.
- **HFresh:** sin construir.

### Corrección: cada celda en disco frente a su equivalente en memoria

| celda | recall objetivo | ef | recall | queries comparadas | resultado |
|---|---|---|---|---|---|
| código completo en RAM | 0.95 | 20 | 0.9507 | 5000 | los mismos ids, distancias y trabajo |
| código completo en RAM | 0.99 | 284 | 0.9901 | 5000 | los mismos ids, distancias y trabajo |
| 512 bits en RAM | 0.95 | 21 | 0.9510 | 5000 | los mismos ids, distancias y trabajo |
| 512 bits en RAM | 0.99 | 188 | 0.9900 | 5000 | los mismos ids, distancias y trabajo |
| 384 bits en RAM | 0.95 | 21 | 0.9505 | 5000 | los mismos ids, distancias y trabajo |
| 384 bits en RAM | 0.99 | 164 | 0.9901 | 5000 | los mismos ids, distancias y trabajo |
| halo de 512 bits | 0.95 | 21 | 0.9510 | 5000 | los mismos ids, distancias y trabajo |
| halo de 512 bits | 0.99 | 188 | 0.9900 | 5000 | los mismos ids, distancias y trabajo |
| halo de 512 bits, podado | 0.95 | 25 | 0.9502 | 5000 | los mismos ids, distancias y trabajo |
| halo de 512 bits, podado | 0.99 | 239 | 0.9900 | 5000 | los mismos ids, distancias y trabajo |

La escritura para si alguna query difiere: lo que hay en el manifiesto pasó.

### Criterio 1: lecturas reales frente al modelo de v3.0

Por query, del store en modo pread, que es donde cuenta. El rescore, aparte.

| celda | recall | cargas | rondas | lecturas del store | bytes del store | modelo de v3.0 | real entre modelo | miembros | aristas | halo | rescore: lecturas | bytes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| código completo en RAM | 0.95 | 10.13 | 2.28 | 20.3 | 0.64 MB | 0.64 MB | 1.005 | 0.54 MB | 0.10 MB | — | 64 | 0.39 MB |
| código completo en RAM | 0.99 | 52.60 | 12.90 | 105.2 | 3.27 MB | 3.25 MB | 1.005 | 2.74 MB | 0.53 MB | — | 64 | 0.39 MB |
| 512 bits en RAM | 0.95 | 18.57 | 4.39 | 37.1 | 1.16 MB | 1.16 MB | 1.005 | 0.97 MB | 0.19 MB | — | 64 | 0.39 MB |
| 512 bits en RAM | 0.99 | 76.57 | 18.89 | 153.1 | 4.75 MB | 4.73 MB | 1.005 | 3.98 MB | 0.77 MB | — | 64 | 0.39 MB |
| 384 bits en RAM | 0.95 | 23.67 | 5.67 | 47.3 | 1.48 MB | 1.47 MB | 1.005 | 1.24 MB | 0.24 MB | — | 64 | 0.39 MB |
| 384 bits en RAM | 0.99 | 91.09 | 22.52 | 182.2 | 5.64 MB | 5.62 MB | 1.005 | 4.73 MB | 0.91 MB | — | 64 | 0.39 MB |
| halo de 512 bits | 0.95 | 18.57 | 4.39 | 55.7 | 3.40 MB | 3.39 MB | 1.002 | 0.97 MB | 0.19 MB | 2.23 MB | 64 | 0.39 MB |
| halo de 512 bits | 0.99 | 76.57 | 18.89 | 229.7 | 13.91 MB | 13.89 MB | 1.002 | 3.98 MB | 0.77 MB | 9.16 MB | 64 | 0.39 MB |
| halo de 512 bits, podado | 0.95 | 20.20 | 4.80 | 60.6 | 2.74 MB | 2.73 MB | 1.002 | 1.05 MB | 0.12 MB | 1.57 MB | 64 | 0.39 MB |
| halo de 512 bits, podado | 0.99 | 87.17 | 21.54 | 261.5 | 11.69 MB | 11.67 MB | 1.002 | 4.50 MB | 0.50 MB | 6.69 MB | 64 | 0.39 MB |

**Se cumple:** la celda que más se separa lee 1.005 veces su modelo, y el margen es de 0.75 a 1.25.

### HFresh real

Sin construir.

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

Sin medida en frío de HFresh todavía.

### RAM residente y disco

| índice | heap en uso tras abrir | huella del proceso | disco: store | vectores exactos | lo residente, en ficheros | total |
|---|---|---|---|---|---|---|
| código completo en RAM | — | — | 242.97 MB | 6082.56 MB | 232.47 MB | 6558.00 MB |
| 512 bits en RAM | — | — | 242.97 MB | 6082.56 MB | 105.75 MB | 6431.28 MB |
| 384 bits en RAM | — | — | 242.97 MB | 6082.56 MB | 89.91 MB | 6415.44 MB |
| halo de 512 bits | — | — | 663.42 MB | 6082.56 MB | 34.47 MB | 6780.45 MB |
| halo de 512 bits, podado | — | — | 494.74 MB | 6082.56 MB | 34.47 MB | 6611.77 MB |

- **Las celdas de posting-edge comparten un solo fichero de vectores,** enlazado: en disco está una vez.
- **El heap es el de Go tras abrir el índice y recolectar.** La caché del sistema no cuenta.
- **En HFresh, «vectores exactos» es su bucket `objects`,** y «store» sus otros buckets.

### Lo que vale y lo que no

- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre índices, medidos igual.
- **Es una sesión,** sin repetir.
- **El p99 en frío sale de las 5 queries más lentas de 500.**
