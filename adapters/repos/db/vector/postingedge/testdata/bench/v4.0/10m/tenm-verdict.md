### Las dos condiciones

La ventaja en frío a 0.95 frente a HFresh de 8 en 8 es de 3 veces o más, por el p50 con 1 thread de la primera ronda, en cada una de las dos celdas; y las cargas por query a 0.95 no crecen más de un 15 % frente a 1M.

| condición | medido | hacía falta | |
|---|---|---|---|
| en frío a 0.95, HFresh de 8 en 8 entre la celda de códigos de ruteo en RAM | 35.62 ms entre 4.02: 8.86x | 3 o más | **sí** |
| en frío a 0.95, HFresh de 8 en 8 entre la celda de halo | 35.62 ms entre 6.57: 5.42x | 3 o más | **sí** |
| cargas por query a 0.95, a 10M entre las de 1M | 21.25 entre 22.56: 0.942, −5.8 % | 1.15 o menos | **sí** |

**El pre-registro se cumple: las dos condiciones, en las dos celdas.**

### Nuestras celdas: sus puntos, a 1M y a 10M, sin cronometrar


| celda | corpus | recall | ef | rescore | recall medido | cargas por query | rondas | lecturas del store | bytes del store | frente a 1M, cargas |
|---|---|---|---|---|---|---|---|---|---|---|
| códigos de ruteo en RAM | 1M | 0.95 | 36 | 64 | 0.9508 | 22.56 | 5.39 | 45.1 | 0.86 MB |  |
| códigos de ruteo en RAM | 10M | 0.95 | 29 | 64 | 0.9520 | 21.25 | 5.06 | 42.5 | 0.87 MB | 0.942, −5.8 % |
| códigos de ruteo en RAM | 1M | 0.99 | no llega: el mayor es 0.9815 | | | | | | | |
| códigos de ruteo en RAM | 10M | 0.99 | 315 | 64 | 0.9900 | 141.58 | 35.14 | 283.2 | 5.72 MB |  |
| halo | 1M | 0.95 | 36 | 64 | 0.9508 | 22.56 | 5.39 | 67.7 | 3.58 MB |  |
| halo | 10M | 0.95 | 29 | 64 | 0.9520 | 21.25 | 5.06 | 63.7 | 4.02 MB | 0.942, −5.8 % |
| halo | 1M | 0.99 | no llega: el mayor es 0.9815 | | | | | | | |
| halo | 10M | 0.99 | 315 | 64 | 0.9900 | 141.58 | 35.14 | 424.7 | 27.29 MB |  |

| celda | corpus | postings | aristas | store | vectores | residente en RAM | build, s |
|---|---|---|---|---|---|---|---|
| códigos de ruteo en RAM | 1M | 4 238 | 9 739 724 | 0.15 GB | 3.07 GB | 93.9 MB | 481 |
| halo | 1M | 4 238 | 9 739 724 | 0.59 GB | 3.07 GB | 21.9 MB | 481 |
| códigos de ruteo en RAM | 10M | 42 373 | 114 091 246 | 1.59 GB | 30.72 GB | 940.0 MB | 13 653 |
| halo | 10M | 42 373 | 114 091 246 | 7.31 GB | 30.72 GB | 220.0 MB | 13 653 |

### HFresh a 10M, como en un nodo, sin cronometrar

- **El índice:** 10 000 000 vectores de 768 dimensiones, 146 926 postings, 50 229 862 entradas, 5.02 por vector; postings de 135 a 407 entradas de 113 bytes. Réplicas 4, maxPostingSizeKB 48, RQ de 1 bits.
- **El build:** 2 893 s de inserts y 1 842 de mantenimiento.
- **El grafo de centroides, limpio:** de 294 087 nodos, 147 301 de ellos borrados, a 146 786.
- **En disco:** 39.28 GB. objects, 32.13 GB; hfresh_postings_hfresh, 6.18 GB; hfresh_shared_hfresh, 0.65 GB; vectors_compressed, 0.26 GB; centroids, 0.07 GB

| configuración | recall | searchProbe | rescore | recall medido | postings leídos | bytes de postings | lecturas de rescore | lecturas por query | |
|---|---|---|---|---|---|---|---|---|---|
| default | — | 256 | 350 | 0.9497 | 392.2 | 10.85 MB | 350 | 742.2 |  |
| r200@0.95 | 0.95 | 402 | 200 | 0.9501 | 616.6 | 17.03 MB | 200 | 816.6 |  |
| r350@0.95 | 0.95 | 261 | 350 | 0.9500 | 399.9 | 11.06 MB | 350 | 749.9 | **la mejor** |
| r800@0.95 | 0.95 | 229 | 800 | 0.9500 | 350.8 | 9.70 MB | 800 | 1150.8 |  |
| r800@0.99 | 0.99 | 2595 | 800 | 0.9900 | 3998.6 | 109.86 MB | 800 | 4798.6 |  |
| r1200@0.95 | 0.95 | 228 | 1200 | 0.9502 | 349.3 | 9.66 MB | 1200 | 1549.3 |  |
| r1200@0.99 | 0.99 | 2212 | 1200 | 0.9900 | 3407.8 | 93.65 MB | 1200 | 4607.8 | **la mejor** |
| r2000@0.95 | 0.95 | 227 | 2000 | 0.9500 | 347.7 | 9.62 MB | 2000 | 2347.7 |  |
| r2000@0.99 | 0.99 | 2145 | 2000 | 0.9900 | 3304.7 | 90.82 MB | 2000 | 5304.7 |  |

### Latencia, primera ronda

**Recall 0.95, en frío:**

| índice | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|
| HFresh, postings uno a uno | 92.62 | 120.22 | 126.39 | 11 | 194.40 | 259.24 | 279.91 | 37 |
| HFresh, postings de 8 en 8 | 35.62 | 40.61 | 42.03 | 28 | 182.57 | 285.15 | 366.68 | 38 |
| posting-edge, códigos de ruteo en RAM | 4.02 | 8.86 | 13.32 | 218 | 11.15 | 19.98 | 25.78 | 547 |
| posting-edge, halo | 6.57 | 16.42 | 26.50 | 128 | 18.49 | 37.79 | 52.25 | 308 |

**Recall 0.95, en caliente:**

| índice | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|
| HFresh, postings uno a uno | 18.45 | 21.15 | 23.13 | 55 | 79.34 | 123.24 | 169.33 | 97 |
| HFresh, postings de 8 en 8 | 17.16 | 19.57 | 21.35 | 59 | 79.23 | 125.93 | 166.17 | 96 |
| posting-edge, códigos de ruteo en RAM | 1.17 | 3.55 | 5.79 | 666 | 1.62 | 4.51 | 7.59 | 3 901 |
| posting-edge, halo | 1.82 | 5.88 | 10.18 | 418 | 2.60 | 8.65 | 14.39 | 2 327 |

**Recall 0.99, en frío:**

| índice | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|
| HFresh, postings uno a uno | 595.74 | 794.92 | 826.93 | 2 | 886.94 | 1252.01 | 1291.06 | 8 |
| HFresh, postings de 8 en 8 | 176.00 | 217.68 | 224.85 | 6 | 879.64 | 1251.84 | 1393.93 | 8 |
| posting-edge, códigos de ruteo en RAM | 16.21 | 50.74 | 72.49 | 48 | 33.97 | 80.25 | 102.80 | 147 |
| posting-edge, halo | 31.43 | 108.52 | 159.12 | 24 | 76.72 | 194.09 | 254.94 | 63 |

**Recall 0.99, en caliente:**

| índice | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|
| HFresh, postings uno a uno | 87.91 | 96.89 | 100.37 | 11 | 359.31 | 509.31 | 566.08 | 22 |
| HFresh, postings de 8 en 8 | 78.48 | 88.28 | 91.43 | 13 | 360.94 | 521.66 | 622.19 | 22 |
| posting-edge, códigos de ruteo en RAM | 6.64 | 25.06 | 35.15 | 109 | 8.67 | 31.53 | 45.31 | 666 |
| posting-edge, halo | 10.90 | 42.48 | 61.11 | 65 | 15.75 | 61.51 | 87.45 | 356 |

**HFresh entre cada celda,** cuántas veces el p50 de la celda:

| recall | HFresh | celda | en frío, 1 thread | en frío, 8 threads | en caliente, 1 thread | en caliente, 8 threads |
|---|---|---|---|---|---|---|
| 0.95 | postings uno a uno | códigos de ruteo en RAM | 23.04x | 17.44x | 15.83x | 48.94x |
| 0.95 | postings uno a uno | halo | 14.10x | 10.51x | 10.13x | 30.53x |
| 0.95 | postings de 8 en 8 | códigos de ruteo en RAM | 8.86x | 16.38x | 14.72x | 48.87x |
| 0.95 | postings de 8 en 8 | halo | 5.42x | 9.87x | 9.42x | 30.49x |
| 0.99 | postings uno a uno | códigos de ruteo en RAM | 36.74x | 26.11x | 13.24x | 41.42x |
| 0.99 | postings uno a uno | halo | 18.95x | 11.56x | 8.07x | 22.82x |
| 0.99 | postings de 8 en 8 | códigos de ruteo en RAM | 10.86x | 25.90x | 11.82x | 41.61x |
| 0.99 | postings de 8 en 8 | halo | 5.60x | 11.47x | 7.20x | 22.92x |

### Las curvas: dos rondas, deriva y actividad

| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |
|---|---|---|---|---|---|
| HFresh, postings uno a uno, a 0.95 | 11-22 06:15 a 06:26 | 11-22 07:28 a 07:39 | no | +1.4 % (0.95, 8 threads, en caliente: 79.34 y 80.42 ms) | — |
| HFresh, postings de 8 en 8, a 0.95 | 11-22 06:26 a 06:36 | 11-22 07:39 a 07:50 | no | +1.2 % (0.95, 1 thread, en caliente: 17.16 y 17.37 ms) | — |
| HFresh, postings uno a uno, a 0.99 | 11-22 06:36 a 06:58 | 11-22 07:50 a 08:12 | no | +1.6 % (0.99, 8 threads, en caliente: 359.31 y 365.06 ms) | — |
| HFresh, postings de 8 en 8, a 0.99 | 11-22 06:58 a 07:17 | 11-22 08:12 a 08:31 | no | −4.1 % (0.99, 8 threads, en frío: 879.64 y 843.83 ms) | — |
| posting-edge, códigos de ruteo en RAM | 11-22 07:17 a 07:23 | 11-22 08:31 a 08:37 | no | +1.9 % (0.95, 1 thread, en caliente: 1.17 y 1.19 ms) | — |
| posting-edge, halo | 11-22 07:23 a 07:28 | 11-22 08:37 a 08:42 | no | −2.4 % (0.95, 8 threads, en frío: 18.49 y 18.04 ms) | — |

- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.
- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.

### RAM y disco de los seis procesos

| índice | heap en uso tras abrir | huella del proceso | en disco |
|---|---|---|---|
| HFresh, postings uno a uno, a 0.95 | 422.2 MB | 1073.4 MB | 39.28 GB |
| HFresh, postings de 8 en 8, a 0.95 | 415.4 MB | 1077.8 MB | 39.28 GB |
| HFresh, postings uno a uno, a 0.99 | 414.2 MB | 1065.4 MB | 39.28 GB |
| HFresh, postings de 8 en 8, a 0.99 | 414.2 MB | 1065.3 MB | 39.28 GB |
| posting-edge, códigos de ruteo en RAM | 956.4 MB | 1777.9 MB | 33.25 GB |
| posting-edge, halo | 236.4 MB | 466.9 MB | 38.25 GB |

- **El heap lleva lo del arnés:** las queries y el manifiesto.

