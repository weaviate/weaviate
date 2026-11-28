| ms por insert, y parte del tiempo del llamador | 100k, 1 | 100k, 2 | 100k, 4 | 100k, 8 | 10M, 1 | 10M, 2 | 10M, 4 | 10M, 8 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| plan: centroides | 0.04 (1 %) | 0.04 (1 %) | 0.04 (1 %) | 0.05 (1 %) | 0.09 (1 %) | 0.09 (0 %) | 0.09 (0 %) | 0.10 (0 %) |
| plan: filas, pedirlas al store y decodificarlas | 1.00 (38 %) | 1.11 (34 %) | 1.24 (30 %) | 1.87 (31 %) | 2.60 (17 %) | 2.59 (14 %) | 2.88 (12 %) | 3.74 (10 %) |
| plan: recorrido | 1.15 (43 %) | 1.22 (38 %) | 1.27 (31 %) | 1.38 (23 %) | 9.58 (62 %) | 9.84 (54 %) | 10.36 (44 %) | 10.94 (31 %) |
| plan: vectores exactos | 0.30 (11 %) | 0.35 (11 %) | 0.45 (11 %) | 0.69 (12 %) | 2.21 (14 %) | 2.63 (14 %) | 3.74 (16 %) | 9.33 (26 %) |
| plan: resto | 0.04 (2 %) | 0.04 (1 %) | 0.04 (1 %) | 0.05 (1 %) | 0.47 (3 %) | 0.54 (3 %) | 0.58 (2 %) | 0.63 (2 %) |
| espera a empezar a buscar mientras un lote se aplica | 0.00 (0 %) | 0.10 (3 %) | 0.49 (12 %) | 1.31 (22 %) | 0.00 (0 %) | 0.52 (3 %) | 2.85 (12 %) | 7.41 (21 %) |
| quien aplica: espera a que acaben las búsquedas en curso | 0.00 (0 %) | 0.15 (5 %) | 0.21 (5 %) | 0.22 (4 %) | 0.00 (0 %) | 0.89 (5 %) | 1.33 (6 %) | 1.36 (4 %) |
| quien aplica: aplicar bajo el cerrojo | 0.12 (4 %) | 0.11 (3 %) | 0.11 (3 %) | 0.12 (2 %) | 0.46 (3 %) | 0.46 (3 %) | 0.45 (2 %) | 0.48 (1 %) |
| espera a que su lote se aplique | 0.00 (0 %) | 0.03 (1 %) | 0.09 (2 %) | 0.12 (2 %) | 0.00 (0 %) | 0.16 (1 %) | 0.46 (2 %) | 0.64 (2 %) |
| admisión del lote y `Flush` de la ronda | 0.01 (0 %) | 0.01 (0 %) | 0.01 (0 %) | 0.01 (0 %) | 0.01 (0 %) | 0.01 (0 %) | 0.01 (0 %) | 0.01 (0 %) |
| fuera de `AddBatch`: la ronda del arnés espera a su llamador más lento | 0.00 (0 %) | 0.07 (2 %) | 0.13 (3 %) | 0.20 (3 %) | 0.00 (0 %) | 0.57 (3 %) | 0.99 (4 %) | 1.19 (3 %) |
| **tiempo del llamador por insert** | **2.66** | **3.23** | **4.07** | **6.01** | **15.42** | **18.30** | **23.74** | **35.84** |
| inserts por segundo, las dos pasadas | 374 y 379 | 617 y 621 | 982 y 981 | 1327 y 1335 | 67 y 63 | 110 y 108 | 170 y 168 | 226 y 220 |

| por insert | 100k, 1 | 100k, 2 | 100k, 4 | 100k, 8 | 10M, 1 | 10M, 2 | 10M, 4 | 10M, 8 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| inserts por segundo | 377 | 619 | 982 | 1 331 | 65 | 109 | 169 | 223 |
| cores ocupados | 1.86 | 3.28 | 5.11 | 6.58 | 1.12 | 1.96 | 3.12 | 4.14 |
| ms de CPU por insert | 4.95 | 5.29 | 5.21 | 4.95 | 17.33 | 17.96 | 18.51 | 18.56 |
| millones de instrucciones por insert | 35.9 | 36.6 | 35.3 | 34.0 | 132.2 | 132.9 | 132.2 | 131.6 |
| instrucciones por ciclo | 2.38 | 2.29 | 2.27 | 2.34 | 2.48 | 2.42 | 2.35 | 2.34 |
| Ps ociosos, según Go | 7.78 | 6.16 | 4.13 | 2.44 | 7.80 | 6.20 | 4.06 | 2.33 |
| hilos listos sin core | 0.35 | 0.52 | 0.64 | 0.63 | 0.12 | 0.14 | 0.16 | 0.21 |
| espera en mutex, ms por insert | 1.17 | 1.66 | 2.37 | 2.43 | 0.84 | 0.62 | 0.82 | 0.81 |
| CPU del GC, ms por insert | 0.71 | 0.71 | 0.49 | 0.32 | 0.38 | 0.35 | 0.31 | 0.29 |
| reservado, MB por insert | 5.15 | 5.70 | 5.62 | 5.85 | 9.41 | 9.64 | 9.97 | 10.43 |
| leído de disco, KB por insert | 0 | 0 | 0 | 0 | 1 683 | 1 826 | 1 976 | 2 186 |
| lecturas de disco por insert | 0.2 | 0.0 | 0.0 | 0.0 | 97.8 | 104.5 | 112.3 | 122.4 |
| ms por lectura de disco | 0.26 | 0.29 | 0.31 | 0.32 | 0.19 | 0.17 | 0.20 | 0.21 |
| hilos esperando al disco | 0.00 | 0.00 | 0.00 | 0.00 | 1.42 | 1.80 | 2.37 | 3.01 |
| escrito a disco, KB por insert | 8.7 | 8.6 | 8.9 | 8.8 | 183.7 | 163.9 | 167.0 | 149.2 |
| fallos de página por insert | 2.1 | 2.5 | 3.8 | 6.8 | 23.9 | 27.5 | 37.3 | 51.0 |
| planes por lote aplicado | 4.0 | 27.3 | 37.8 | 54.8 | 4.0 | 26.1 | 36.8 | 55.7 |

| por insert, 8 llamadores | 100k | 10M | veces |
|---|---:|---:|---:|
| cargas de posting | 35.9 | 108.6 | 3.03 |
| rondas | 8.7 | 26.9 | 3.08 |
| filas leídas del store | 71.8 | 217.2 | 3.03 |
| MB de filas | 1.36 | 4.68 | 3.45 |
| segmentos consultados por fila | 1.24 | 1.73 | 1.39 |
| segmentos donde estaba la fila | 1.10 | 1.15 | 1.05 |
| miembros recorridos | 9 033 | 28 235 | 3.13 |
| aristas seguidas | 35 762 | 170 518 | 4.77 |
| extremos dentro de un posting ya cargado | 8 949 | 25 720 | 2.87 |
| extremos puntuados | 10 843 | 75 148 | 6.93 |
| vectores exactos | 150.0 | 150.0 | 1.00 |
| vecinos mirados para aristas inversas | 7.28 | 11.16 | 1.53 |
| aristas inversas entradas | 4.43 | 6.17 | 1.40 |
| splits por mil inserts | 4.45 | 4.25 | 0.96 |
| filas escritas | 0.088 | 0.223 | 2.53 |
| de ellas, por el presupuesto | 0.000 | 0.215 | — |
| KB de diario | 3.27 | 8.39 | 2.57 |

| pasada con 8 llamadores | 100k: inserts/s | cargas | 10M: inserts/s | cargas | lecturas de disco por insert |
|---|---:|---:|---:|---:|---:|
| `m2` sin tocar | 1 336 | 35.9 | 234 | 108.6 | 113 |
| diagnóstico, relojes apagados | 1 338 | 35.9 | 240 | 108.6 | 104 |
| diagnóstico, relojes puestos | 1 332 | 35.9 | 233 | 108.6 | 114 |
| fases, primera | 1 327 | 35.9 | 226 | 108.6 | 121 |
| fases, segunda | 1 335 | 35.9 | 220 | 108.6 | 124 |
| con los contadores de trabajo | 1 280 | 35.9 | 214 | 108.6 | 127 |
| sin compactación | 1 013 | 35.9 | 228 | 108.6 | 118 |
| las 1 000 queries | 780 | 46.5 | 182 | 87.8 | 145 |
| filas 100 000 a 119 999 | 969 | 43.7 | — | — | — |

| la búsqueda del insert, ms | 100k: sola, 1 hilo | insert, 1 llamador | sola, 8 hilos | insert, 8 llamadores | 10M: sola, 1 hilo | insert, 1 llamador | sola, 8 hilos | insert, 8 llamadores |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| por segundo | 372 | 377 | 1 534 | 1 331 | 67 | 65 | 343 | 223 |
| plan, ms | 2.68 | 2.53 | 5.21 | 4.03 | 15.02 | 14.95 | 23.34 | 24.75 |
| filas | 1.13 | 1.00 | 2.86 | 1.87 | 2.74 | 2.60 | 4.42 | 3.74 |
| recorrido | 1.19 | 1.15 | 1.48 | 1.38 | 9.70 | 9.58 | 11.38 | 10.94 |
| vectores exactos | 0.29 | 0.30 | 0.76 | 0.69 | 2.02 | 2.21 | 6.75 | 9.33 |
| cores ocupados | 1.92 | 1.86 | 8.24 | 6.58 | 1.17 | 1.12 | 6.34 | 4.14 |
| ms de CPU | 5.16 | 4.95 | 5.37 | 4.95 | 17.64 | 17.33 | 18.51 | 18.56 |
| millones de instrucciones | 39.5 | 35.9 | 37.2 | 34.0 | 132.6 | 132.2 | 129.8 | 131.6 |
| leído de disco, KB | 0 | 0 | 0 | 0 | 1 576 | 1 683 | 1 453 | 2 186 |

| por insert, 8 llamadores | 100k | 100k sin compactación | 10M | 10M sin compactación |
|---|---:|---:|---:|---:|
| inserts por segundo | 1 331 | 1 013 | 223 | 228 |
| escrito a disco por el proceso, KB | 8.8 | 5.8 | 149.2 | 23.1 |
| vectores, KB | 3.02 | 3.03 | 3.06 | 3.07 |
| WAL del store, KB | 3.12 | 3.16 | 8.34 | 8.34 |
| volcados de memtable, KB | 2.59 | 0.54 | 11.34 | 8.05 |
| compactación, KB | 2.5 | 0.0 | 267.3 | 0.0 |
| diario, KB, por su contador | 3.28 | 3.28 | 8.39 | 8.39 |
| escrituras de disco | 0.13 | 0.06 | 5.70 | 0.86 |
| leído de disco, KB | 0 | 0 | 2 186 | 2 109 |
| lecturas de disco | 0.0 | 0.0 | 122.4 | 117.5 |
| fallos de página | 6.8 | 6.1 | 51.0 | 34.0 |
| segmentos consultados por fila | 1.22 | 2.71 | 1.79 | 3.59 |
| segmentos donde estaba la fila | 1.07 | 2.03 | 1.16 | 1.76 |
| plan: filas, ms | 1.87 | 3.17 | 3.74 | 4.57 |
| reservado, MB | 5.85 | 10.18 | 10.43 | 15.15 |
100k phases-c8-a edgedists:2>2 map:2>2 pending:3>1 postings:2>2 routing:2>2 vectors_compressed_centroids:2>1
100k nocompaction-c8 edgedists:2>2 map:2>2 pending:3>3 postings:2>3 routing:2>2 vectors_compressed_centroids:2>2
10M phases-c8-a edgedists:3>2 map:2>2 pending:7>5 postings:3>3 routing:2>2 vectors_compressed_centroids:8>8
10M nocompaction-c8 edgedists:3>4 map:3>4 pending:7>8 postings:3>5 routing:3>4 vectors_compressed_centroids:9>10
