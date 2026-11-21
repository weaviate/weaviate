### Las curvas: punto de operación, corrección y lecturas, sin cronometrar

Un build para las tres: 990 000 vectores, 4 195 postings, 8 888 026 aristas, en 650 s.

| curva | recall | ef | recall medido | cargas | rondas | lecturas de bloques | bytes de bloques | real entre modelo | lecturas de rescore | bytes de rescore | lecturas por query | bytes por query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | 0.95 | 21 | 0.9513 | 18.58 | 4.40 | 37.2 | 1.16 MB | 1.005 | 64.0 | 0.39 MB | 101.2 | 1.56 MB |
| la actual: 512 bits en RAM, rescore 64 | 0.99 | 188 | 0.9900 | 76.56 | 18.89 | 153.1 | 4.75 MB | 1.005 | 64.0 | 0.39 MB | 217.1 | 5.14 MB |
| scan con 512 bits, rescore 200 | 0.95 | 50 | 0.9502 | 17.97 | 4.25 | 18.0 | 0.18 MB | 1.028 | 199.8 | 1.23 MB | 217.8 | 1.41 MB |
| scan con 512 bits, rescore 200 | 0.99 | no llega: el mejor recall es 0.9822 con ef hasta 2 048 | | | | | | | | | | |
| scan con 512 bits, rescore 400 | 0.95 | 43 | 0.9508 | 16.42 | 3.86 | 16.4 | 0.17 MB | 1.028 | 397.4 | 2.44 MB | 413.8 | 2.61 MB |
| scan con 512 bits, rescore 400 | 0.99 | 711 | 0.9900 | 120.43 | 29.86 | 120.4 | 1.19 MB | 1.028 | 400.0 | 2.46 MB | 520.4 | 3.65 MB |
| scan con 512 bits, rescore 800 | 0.95 | 41 | 0.9501 | 15.95 | 3.74 | 15.9 | 0.16 MB | 1.028 | 787.6 | 4.84 MB | 803.5 | 5.00 MB |
| scan con 512 bits, rescore 800 | 0.99 | 385 | 0.9900 | 75.04 | 18.51 | 75.0 | 0.75 MB | 1.028 | 799.5 | 4.91 MB | 874.6 | 5.66 MB |
| scan con 768 bits, rescore 200 | 0.95 | 30 | 0.9511 | 12.96 | 2.99 | 13.0 | 0.13 MB | 1.028 | 199.7 | 1.23 MB | 212.7 | 1.36 MB |
| scan con 768 bits, rescore 200 | 0.99 | 397 | 0.9900 | 72.14 | 17.79 | 72.1 | 0.72 MB | 1.028 | 200.0 | 1.23 MB | 272.1 | 1.95 MB |
| scan con 768 bits, rescore 400 | 0.95 | 29 | 0.9500 | 12.69 | 2.92 | 12.7 | 0.13 MB | 1.028 | 395.4 | 2.43 MB | 408.1 | 2.56 MB |
| scan con 768 bits, rescore 400 | 0.99 | 291 | 0.9900 | 57.12 | 14.03 | 57.1 | 0.57 MB | 1.028 | 400.0 | 2.46 MB | 457.1 | 3.03 MB |
| scan con 768 bits, rescore 800 | 0.95 | 29 | 0.9500 | 12.69 | 2.92 | 12.7 | 0.13 MB | 1.028 | 779.7 | 4.79 MB | 792.3 | 4.92 MB |
| scan con 768 bits, rescore 800 | 0.99 | 281 | 0.9900 | 55.64 | 13.66 | 55.6 | 0.56 MB | 1.028 | 799.3 | 4.91 MB | 854.9 | 5.47 MB |

- **Cada curva devuelve de disco lo mismo que su equivalente en memoria,** en los puntos que alcanza: los mismos ids, distancias y trabajo, sobre las 5 000 queries.
- **No todas las curvas llegan a 0.99:** scan con 512 bits, rescore 200 se queda en 0.9822.

### Disco y RAM

| índice | store | vectores exactos | lo residente, en ficheros | total en disco | heap en uso tras abrir | huella del proceso |
|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM | 243.0 MB | 6 083 MB | 105.7 MB | 6 431 MB | 145.8 MB | 287.8 MB |
| scan con 512 bits | 36.8 MB | 6 083 MB | 105.7 MB | 6 225 MB | 153.8 MB | 287.8 MB |
| scan con 768 bits | 36.8 MB | 6 083 MB | 137.4 MB | 6 257 MB | 185.2 MB | 351.3 MB |

- **El heap lleva unos 40 MB del arnés y del proceso,** como en v4.0 («El heap a 1M, por componente»).

### Con el presupuesto de 8: 8.0 por query con 1 thread y 8.0 con 8

**Recall 0.95, en frío, primera ronda:**

| curva | punto | 1 thread: p50, ms | frente a la actual | p95 | p99 | QPS | 8 threads: p50, ms | frente a la actual | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | ef 21 | 3.62 | 1.00x | 8.20 | 12.48 | 237 | 11.76 | 1.00x | 22.61 | 31.48 | 540 |
| scan con 512 bits, rescore 200 | ef 50 | 5.78 | 1.60x | 8.51 | 9.43 | 168 | 21.36 | 1.82x | 33.92 | 37.47 | 324 |
| scan con 512 bits, rescore 400 | ef 43 | 8.79 | 2.43x | 12.60 | 15.89 | 106 | 37.00 | 3.15x | 62.70 | 165.21 | 171 |
| scan con 512 bits, rescore 800 | ef 41 | 13.35 | 3.68x | 17.94 | 19.60 | 76 | 35.68 | 3.03x | 65.64 | 117.21 | 185 |
| scan con 768 bits, rescore 200 | ef 30 | 5.41 | 1.49x | 7.82 | 8.84 | 179 | 20.86 | 1.77x | 36.13 | 91.98 | 305 |
| scan con 768 bits, rescore 400 | ef 29 | 8.27 | 2.28x | 11.20 | 12.65 | 121 | 34.43 | 2.93x | 56.87 | 61.25 | 199 |
| scan con 768 bits, rescore 800 | ef 29 | 12.37 | 3.41x | 16.81 | 18.16 | 82 | 31.78 | 2.70x | 54.13 | 64.15 | 206 |

**Recall 0.95, en caliente, primera ronda:**

| curva | punto | 1 thread: p50, ms | frente a la actual | p95 | p99 | QPS | 8 threads: p50, ms | frente a la actual | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | ef 21 | 0.90 | 1.00x | 2.95 | 4.43 | 837 | 1.27 | 1.00x | 4.10 | 6.46 | 4 762 |
| scan con 512 bits, rescore 200 | ef 50 | 1.17 | 1.30x | 2.72 | 3.28 | 742 | 2.05 | 1.61x | 4.24 | 6.54 | 3 467 |
| scan con 512 bits, rescore 400 | ef 43 | 1.34 | 1.48x | 2.74 | 3.30 | 669 | 3.35 | 2.64x | 6.72 | 9.59 | 2 167 |
| scan con 512 bits, rescore 800 | ef 41 | 1.75 | 1.95x | 3.15 | 3.78 | 528 | 6.35 | 5.00x | 10.92 | 14.15 | 1 210 |
| scan con 768 bits, rescore 200 | ef 30 | 1.02 | 1.13x | 2.35 | 2.84 | 854 | 1.90 | 1.49x | 4.03 | 6.01 | 3 733 |
| scan con 768 bits, rescore 400 | ef 29 | 1.24 | 1.38x | 2.54 | 3.05 | 723 | 3.27 | 2.57x | 6.64 | 9.62 | 2 240 |
| scan con 768 bits, rescore 800 | ef 29 | 1.69 | 1.88x | 3.04 | 3.56 | 550 | 6.55 | 5.15x | 11.39 | 14.29 | 1 177 |

**Recall 0.99, en frío, primera ronda:**

| curva | punto | 1 thread: p50, ms | frente a la actual | p95 | p99 | QPS | 8 threads: p50, ms | frente a la actual | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | ef 188 | 8.83 | 1.00x | 26.88 | 38.39 | 88 | 22.31 | 1.00x | 56.06 | 70.53 | 244 |
| scan con 512 bits, rescore 400 | ef 711 | 17.00 | 1.92x | 29.89 | 33.34 | 56 | 46.21 | 2.07x | 78.17 | 87.34 | 145 |
| scan con 512 bits, rescore 800 | ef 385 | 19.68 | 2.23x | 30.55 | 33.28 | 49 | 47.34 | 2.12x | 66.90 | 81.91 | 152 |
| scan con 768 bits, rescore 200 | ef 397 | 10.69 | 1.21x | 21.20 | 23.71 | 86 | 26.76 | 1.20x | 46.10 | 51.94 | 252 |
| scan con 768 bits, rescore 400 | ef 291 | 12.36 | 1.40x | 21.74 | 24.38 | 75 | 43.08 | 1.93x | 72.21 | 80.39 | 156 |
| scan con 768 bits, rescore 800 | ef 281 | 17.73 | 2.01x | 27.76 | 30.25 | 54 | 45.02 | 2.02x | 76.09 | 107.84 | 152 |

**Recall 0.99, en caliente, primera ronda:**

| curva | punto | 1 thread: p50, ms | frente a la actual | p95 | p99 | QPS | 8 threads: p50, ms | frente a la actual | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | ef 188 | 3.16 | 1.00x | 10.71 | 15.49 | 238 | 4.26 | 1.00x | 14.72 | 21.03 | 1 403 |
| scan con 512 bits, rescore 400 | ef 711 | 6.24 | 1.98x | 13.46 | 16.10 | 147 | 8.03 | 1.88x | 16.26 | 19.64 | 922 |
| scan con 512 bits, rescore 800 | ef 385 | 4.50 | 1.42x | 9.67 | 11.55 | 200 | 8.49 | 1.99x | 16.37 | 21.38 | 870 |
| scan con 768 bits, rescore 200 | ef 397 | 4.04 | 1.28x | 10.00 | 12.08 | 214 | 4.87 | 1.14x | 11.41 | 13.86 | 1 428 |
| scan con 768 bits, rescore 400 | ef 291 | 3.51 | 1.11x | 8.61 | 10.26 | 245 | 5.30 | 1.24x | 11.20 | 14.01 | 1 355 |
| scan con 768 bits, rescore 800 | ef 281 | 3.92 | 1.24x | 8.99 | 10.60 | 223 | 8.01 | 1.88x | 15.59 | 19.74 | 921 |

**Las partes de la latencia media, a 0.95 con 1 thread, en frío:**

| curva | media, ms | centroides | espera de los bloques | scan y ruteo | rescore | presupuesto por query |
|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | 4.22 | 0.07 | 1.89 | 0.65 | 1.56 | 8.0 |
| scan con 512 bits, rescore 200 | 5.93 | 0.07 | 1.22 | 0.78 | 3.81 | 8.0 |
| scan con 512 bits, rescore 400 | 9.39 | 0.08 | 1.26 | 0.74 | 7.26 | 8.0 |
| scan con 512 bits, rescore 800 | 13.14 | 0.08 | 1.16 | 0.75 | 11.06 | 8.0 |
| scan con 768 bits, rescore 200 | 5.58 | 0.07 | 0.99 | 0.65 | 3.82 | 8.0 |
| scan con 768 bits, rescore 400 | 8.23 | 0.07 | 0.98 | 0.66 | 6.45 | 8.0 |
| scan con 768 bits, rescore 800 | 12.13 | 0.08 | 0.97 | 0.70 | 10.28 | 8.0 |

**Las partes de la latencia media, a 0.95 con 1 thread, en caliente:**

| curva | media, ms | centroides | espera de los bloques | scan y ruteo | rescore | presupuesto por query |
|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | 1.19 | 0.06 | 0.29 | 0.67 | 0.13 | 8.0 |
| scan con 512 bits, rescore 200 | 1.35 | 0.06 | 0.15 | 0.81 | 0.29 | 8.0 |
| scan con 512 bits, rescore 400 | 1.49 | 0.06 | 0.14 | 0.76 | 0.48 | 8.0 |
| scan con 512 bits, rescore 800 | 1.89 | 0.06 | 0.13 | 0.79 | 0.84 | 8.0 |
| scan con 768 bits, rescore 200 | 1.17 | 0.06 | 0.11 | 0.67 | 0.30 | 8.0 |
| scan con 768 bits, rescore 400 | 1.38 | 0.06 | 0.11 | 0.68 | 0.49 | 8.0 |
| scan con 768 bits, rescore 800 | 1.82 | 0.06 | 0.11 | 0.73 | 0.84 | 8.0 |

### Con el presupuesto de un nodo: 20.0 por query con 1 thread y 14.4 con 8

**Recall 0.95, en frío, primera ronda:**

| curva | punto | 1 thread: p50, ms | frente a la actual | p95 | p99 | QPS | 8 threads: p50, ms | frente a la actual | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | ef 21 | 3.43 | 1.00x | 8.07 | 12.40 | 246 | 11.50 | 1.00x | 23.19 | 30.28 | 539 |
| scan con 512 bits, rescore 200 | ef 50 | 5.38 | 1.57x | 7.96 | 8.92 | 179 | 20.99 | 1.83x | 34.17 | 37.50 | 330 |
| scan con 512 bits, rescore 400 | ef 43 | 7.85 | 2.29x | 10.73 | 11.80 | 125 | 35.96 | 3.13x | 58.14 | 65.74 | 191 |
| scan con 512 bits, rescore 800 | ef 41 | 11.83 | 3.45x | 15.86 | 16.96 | 85 | 44.36 | 3.86x | 92.65 | 105.84 | 148 |
| scan con 768 bits, rescore 200 | ef 30 | 5.00 | 1.46x | 7.29 | 8.08 | 193 | 19.91 | 1.73x | 32.80 | 35.35 | 343 |
| scan con 768 bits, rescore 400 | ef 29 | 7.57 | 2.21x | 10.07 | 11.18 | 133 | 34.39 | 2.99x | 56.30 | 60.50 | 202 |
| scan con 768 bits, rescore 800 | ef 29 | 11.27 | 3.29x | 15.03 | 16.74 | 90 | 39.53 | 3.44x | 83.56 | 95.64 | 168 |

**Recall 0.95, en caliente, primera ronda:**

| curva | punto | 1 thread: p50, ms | frente a la actual | p95 | p99 | QPS | 8 threads: p50, ms | frente a la actual | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | ef 21 | 0.91 | 1.00x | 2.96 | 4.48 | 829 | 1.28 | 1.00x | 4.02 | 6.33 | 4 767 |
| scan con 512 bits, rescore 200 | ef 50 | 1.16 | 1.27x | 2.67 | 3.21 | 753 | 2.00 | 1.56x | 4.26 | 6.05 | 3 546 |
| scan con 512 bits, rescore 400 | ef 43 | 1.35 | 1.48x | 2.73 | 3.29 | 666 | 3.38 | 2.65x | 6.70 | 9.67 | 2 155 |
| scan con 512 bits, rescore 800 | ef 41 | 1.87 | 2.05x | 3.19 | 3.78 | 499 | 6.68 | 5.24x | 11.45 | 14.81 | 1 147 |
| scan con 768 bits, rescore 200 | ef 30 | 1.01 | 1.10x | 2.34 | 2.83 | 865 | 1.91 | 1.50x | 3.95 | 5.97 | 3 727 |
| scan con 768 bits, rescore 400 | ef 29 | 1.26 | 1.38x | 2.56 | 3.04 | 715 | 3.39 | 2.66x | 6.68 | 9.78 | 2 163 |
| scan con 768 bits, rescore 800 | ef 29 | 1.78 | 1.95x | 3.08 | 3.59 | 527 | 6.63 | 5.19x | 11.24 | 14.23 | 1 172 |

**Recall 0.99, en frío, primera ronda:**

| curva | punto | 1 thread: p50, ms | frente a la actual | p95 | p99 | QPS | 8 threads: p50, ms | frente a la actual | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | ef 188 | 8.61 | 1.00x | 26.47 | 38.98 | 89 | 22.54 | 1.00x | 55.41 | 69.63 | 246 |
| scan con 512 bits, rescore 400 | ef 711 | 15.78 | 1.83x | 28.56 | 31.65 | 60 | 45.44 | 2.02x | 76.99 | 85.54 | 149 |
| scan con 512 bits, rescore 800 | ef 385 | 17.80 | 2.07x | 28.01 | 31.06 | 54 | 54.80 | 2.43x | 107.73 | 122.68 | 124 |
| scan con 768 bits, rescore 200 | ef 397 | 9.83 | 1.14x | 20.29 | 23.04 | 91 | 25.97 | 1.15x | 44.47 | 50.93 | 257 |
| scan con 768 bits, rescore 400 | ef 291 | 11.54 | 1.34x | 20.33 | 23.27 | 81 | 42.05 | 1.87x | 68.74 | 78.11 | 163 |
| scan con 768 bits, rescore 800 | ef 281 | 16.38 | 1.90x | 26.14 | 28.69 | 58 | 49.35 | 2.19x | 99.50 | 119.13 | 130 |

**Recall 0.99, en caliente, primera ronda:**

| curva | punto | 1 thread: p50, ms | frente a la actual | p95 | p99 | QPS | 8 threads: p50, ms | frente a la actual | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | ef 188 | 3.23 | 1.00x | 10.71 | 15.86 | 235 | 4.28 | 1.00x | 14.48 | 21.09 | 1 408 |
| scan con 512 bits, rescore 400 | ef 711 | 5.95 | 1.84x | 12.86 | 15.49 | 153 | 8.02 | 1.87x | 16.24 | 19.69 | 920 |
| scan con 512 bits, rescore 800 | ef 385 | 4.40 | 1.36x | 9.30 | 11.12 | 204 | 8.36 | 1.95x | 16.21 | 20.82 | 883 |
| scan con 768 bits, rescore 200 | ef 397 | 3.98 | 1.23x | 9.91 | 11.96 | 216 | 4.97 | 1.16x | 11.60 | 14.20 | 1 411 |
| scan con 768 bits, rescore 400 | ef 291 | 3.46 | 1.07x | 8.35 | 10.09 | 250 | 5.35 | 1.25x | 11.32 | 14.59 | 1 337 |
| scan con 768 bits, rescore 800 | ef 281 | 3.88 | 1.20x | 8.63 | 10.27 | 226 | 7.88 | 1.84x | 15.48 | 20.34 | 929 |

**Las partes de la latencia media, a 0.95 con 1 thread, en frío:**

| curva | media, ms | centroides | espera de los bloques | scan y ruteo | rescore | presupuesto por query |
|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | 4.06 | 0.07 | 1.87 | 0.65 | 1.41 | 20.0 |
| scan con 512 bits, rescore 200 | 5.57 | 0.07 | 1.22 | 0.77 | 3.45 | 20.0 |
| scan con 512 bits, rescore 400 | 7.97 | 0.08 | 1.15 | 0.73 | 5.94 | 20.0 |
| scan con 512 bits, rescore 800 | 11.76 | 0.08 | 1.13 | 0.75 | 9.70 | 20.0 |
| scan con 768 bits, rescore 200 | 5.18 | 0.07 | 0.97 | 0.65 | 3.42 | 20.0 |
| scan con 768 bits, rescore 400 | 7.50 | 0.07 | 0.95 | 0.66 | 5.74 | 20.0 |
| scan con 768 bits, rescore 800 | 11.16 | 0.08 | 0.97 | 0.70 | 9.31 | 20.0 |

**Las partes de la latencia media, a 0.95 con 1 thread, en caliente:**

| curva | media, ms | centroides | espera de los bloques | scan y ruteo | rescore | presupuesto por query |
|---|---|---|---|---|---|---|
| la actual: 512 bits en RAM, rescore 64 | 1.21 | 0.06 | 0.31 | 0.66 | 0.13 | 20.0 |
| scan con 512 bits, rescore 200 | 1.33 | 0.06 | 0.15 | 0.78 | 0.29 | 20.0 |
| scan con 512 bits, rescore 400 | 1.50 | 0.06 | 0.14 | 0.74 | 0.51 | 20.0 |
| scan con 512 bits, rescore 800 | 2.00 | 0.06 | 0.13 | 0.76 | 0.97 | 20.0 |
| scan con 768 bits, rescore 200 | 1.16 | 0.06 | 0.11 | 0.66 | 0.28 | 20.0 |
| scan con 768 bits, rescore 400 | 1.40 | 0.06 | 0.11 | 0.67 | 0.50 | 20.0 |
| scan con 768 bits, rescore 800 | 1.90 | 0.06 | 0.11 | 0.70 | 0.94 | 20.0 |

### La lectura

Una curva es una opción si a 0.95 en frío queda a 1.20 veces o menos de la celda actual, y llega a 0.99. Es el p50 con 1 thread de la primera ronda, sobre el de la celda actual en la misma pasada.

| curva | llega a 0.99 | en frío a 0.95, con presupuesto de 8 | frente a la actual | con el de un nodo | frente a la actual | con presupuesto de 8, que es como se lee | con el de un nodo |
|---|---|---|---|---|---|---|---|
| scan con 512 bits, rescore 200 | no, hasta 0.9822 | 5.78 | 1.60x | 5.38 | 1.57x | no lo es | no lo es |
| scan con 512 bits, rescore 400 | sí | 8.79 | 2.43x | 7.85 | 2.29x | no lo es | no lo es |
| scan con 512 bits, rescore 800 | sí | 13.35 | 3.68x | 11.83 | 3.45x | no lo es | no lo es |
| scan con 768 bits, rescore 200 | sí | 5.41 | 1.49x | 5.00 | 1.46x | no lo es | no lo es |
| scan con 768 bits, rescore 400 | sí | 8.27 | 2.28x | 7.57 | 2.21x | no lo es | no lo es |
| scan con 768 bits, rescore 800 | sí | 12.37 | 3.41x | 11.27 | 3.29x | no lo es | no lo es |

- **512 bits, con el presupuesto de 8: no es una opción** con ninguno de los tres rescores.
- **512 bits, con el presupuesto de un nodo: no es una opción** con ninguno de los tres rescores.
- **768 bits, con el presupuesto de 8: no es una opción** con ninguno de los tres rescores.
- **768 bits, con el presupuesto de un nodo: no es una opción** con ninguno de los tres rescores.

### Deriva entre rondas y actividad

- **Medianas que se movieron más de un 10 % entre rondas** (1 de 104):
  - scan con 768 bits, rescore 800, presupuesto de un nodo, a 0.99 con 8 threads, en frío: 49.35 → 60.29 ms (+22.2 %)
- **La sesión:** 31 tramos, de 2026-11-20 04:11:05 a 2026-11-20 23:32:00.
- **Tramos con teclado o trackpad:** r2 node-s512r800, r2 node-s768r200.
- **Tramos con eventos de energía o de actividad:** r2 node-s512r800 (2).
