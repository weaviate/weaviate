### Las dos condiciones

La ventaja en frío a 0.95 frente a HFresh de 8 en 8 es de 3 veces o más, por el p50 con 1 thread de la primera ronda, en cada una de las dos celdas; y las cargas por query a 0.95 no crecen más de un 15 % frente a 1M.

| condición | medido | hacía falta | |
|---|---|---|---|
| en frío a 0.95, HFresh de 8 en 8 entre la celda de códigos de ruteo en RAM | 35.86 ms entre 5.20: 6.90x | 3 o más | **sí** |
| en frío a 0.95, HFresh de 8 en 8 entre la celda de halo | 35.86 ms entre 7.55: 4.75x | 3 o más | **sí** |
| cargas por query a 0.95, a 10M entre las de 1M | 20.70 entre 19.24: 1.076, +7.6 % | 1.15 o menos | **sí** |

**El pre-registro se cumple: las dos condiciones, en las dos celdas.**

### Nuestras celdas: sus puntos, a 1M y a 10M, sin cronometrar

- **La celda de códigos de ruteo en RAM, a 10M, por rescore:** con 64, el mayor recall es 0.9900; con 150, el mayor recall es 0.9914.
- **La celda de halo, a 10M, por rescore:** con 64, el mayor recall es 0.9900; con 150, el mayor recall es 0.9914.

| celda | corpus | recall | ef | rescore | recall medido | cargas por query | rondas | lecturas del store | bytes del store | frente a 1M, cargas |
|---|---|---|---|---|---|---|---|---|---|---|
| códigos de ruteo en RAM | 1M | 0.95 | 28 | 150 | 0.9523 | 19.24 | 4.56 | 38.5 | 0.73 MB |  |
| códigos de ruteo en RAM | 10M | 0.95 | 28 | 150 | 0.9504 | 20.70 | 4.92 | 41.4 | 0.85 MB | 1.076, +7.6 % |
| códigos de ruteo en RAM | 1M | 0.99 | 199 | 150 | 0.9900 | 75.07 | 18.52 | 150.1 | 2.85 MB |  |
| códigos de ruteo en RAM | 10M | 0.99 | 177 | 150 | 0.9900 | 88.87 | 21.97 | 177.7 | 3.60 MB | 1.184, +18.4 % |
| halo | 1M | 0.95 | 28 | 150 | 0.9523 | 19.24 | 4.56 | 57.7 | 3.05 MB |  |
| halo | 10M | 0.95 | 28 | 150 | 0.9504 | 20.70 | 4.92 | 62.1 | 3.91 MB | 1.076, +7.6 % |
| halo | 1M | 0.99 | 199 | 150 | 0.9900 | 75.07 | 18.52 | 225.2 | 11.92 MB |  |
| halo | 10M | 0.99 | 177 | 150 | 0.9900 | 88.87 | 21.97 | 266.6 | 17.11 MB | 1.184, +18.4 % |

| celda | corpus | postings | aristas | store | vectores | residente en RAM | build, s |
|---|---|---|---|---|---|---|---|
| códigos de ruteo en RAM | 1M | 4 238 | 9 739 724 | 0.15 GB | 3.07 GB | 93.9 MB | 251 |
| halo | 1M | 4 238 | 9 739 724 | 0.59 GB | 3.07 GB | 21.9 MB | 251 |
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
| HFresh, postings de 8 en 8 | 35.86 | 40.74 | 50.55 | 28 | 181.18 | 300.30 | 386.94 | 38 |
| posting-edge, códigos de ruteo en RAM | 5.20 | 9.67 | 14.22 | 177 | 17.35 | 28.69 | 35.85 | 380 |
| posting-edge, halo | 7.55 | 16.92 | 27.87 | 114 | 24.21 | 45.37 | 62.39 | 249 |

**Recall 0.95, en caliente:**

| índice | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|
| HFresh, postings de 8 en 8 | 17.42 | 20.11 | 22.17 | 58 | 78.56 | 128.72 | 168.59 | 97 |
| posting-edge, códigos de ruteo en RAM | 1.30 | 3.58 | 5.99 | 619 | 1.99 | 5.25 | 8.30 | 3 295 |
| posting-edge, halo | 1.87 | 6.03 | 10.05 | 407 | 2.92 | 8.95 | 14.93 | 2 144 |

**Recall 0.99, en frío:**

| índice | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|
| posting-edge, códigos de ruteo en RAM | 12.22 | 34.12 | 51.90 | 68 | 30.67 | 64.94 | 79.74 | 177 |
| posting-edge, halo | 21.92 | 75.41 | 110.48 | 35 | 58.54 | 143.41 | 179.53 | 87 |

**Recall 0.99, en caliente:**

| índice | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|
| posting-edge, códigos de ruteo en RAM | 4.42 | 16.24 | 24.29 | 167 | 5.76 | 20.47 | 32.35 | 1 012 |
| posting-edge, halo | 6.90 | 28.13 | 41.49 | 102 | 10.20 | 39.93 | 58.04 | 556 |

**HFresh entre cada celda,** cuántas veces el p50 de la celda:

| recall | HFresh | celda | en frío, 1 thread | en frío, 8 threads | en caliente, 1 thread | en caliente, 8 threads |
|---|---|---|---|---|---|---|
| 0.95 | postings de 8 en 8 | códigos de ruteo en RAM | 6.90x | 10.44x | 13.38x | 39.48x |
| 0.95 | postings de 8 en 8 | halo | 4.75x | 7.48x | 9.31x | 26.91x |

### Las curvas: dos rondas, deriva y actividad

| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |
|---|---|---|---|---|---|
| HFresh, postings uno a uno, a 0.95 |  |  | no |  | — |
| HFresh, postings de 8 en 8, a 0.95 | 11-22 08:55 a 09:06 | 11-22 09:17 a 09:27 | no | +2.3 % (0.95, 8 threads, en frío: 181.18 y 185.42 ms) | — |
| HFresh, postings uno a uno, a 0.99 |  |  | no |  | — |
| HFresh, postings de 8 en 8, a 0.99 |  |  | no |  | — |
| posting-edge, códigos de ruteo en RAM | 11-22 09:06 a 09:12 | 11-22 09:27 a 09:33 | no | −3.1 % (0.99, 8 threads, en caliente: 5.76 y 5.58 ms) | — |
| posting-edge, halo | 11-22 09:12 a 09:17 | 11-22 09:33 a 09:38 | no | −2.0 % (0.95, 8 threads, en caliente: 2.92 y 2.86 ms) | — |

- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.
- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.

### RAM y disco de los seis procesos

| índice | heap en uso tras abrir | huella del proceso | en disco |
|---|---|---|---|
| HFresh, postings de 8 en 8, a 0.95 | 421.8 MB | 1073.4 MB | 39.28 GB |
| posting-edge, códigos de ruteo en RAM | 956.3 MB | 1778.9 MB | 33.25 GB |
| posting-edge, halo | 236.4 MB | 466.1 MB | 38.25 GB |

- **El heap lleva lo del arnés:** las queries y el manifiesto.

