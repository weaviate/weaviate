### El grafo de centroides de HFresh y su mejor configuración, sin cronometrar

- **El grafo, antes de limpiar:** 57 433 nodos, 28 967 de ellos tombstones, para 28 466 postings vivos.
- **Limpio:** 28 466 nodos y 0 tombstones, en 1 ronda de limpieza y 7.6 s.
- **Por defecto** (searchProbe 256, rescoreLimit 350): recall 0.9674 sin limpiar y 0.9636 limpio.
- **El mejor recall del barrido,** con el rescoreLimit por defecto: 0.9989 sin limpiar y 0.9989 limpio.

La configuración que menos lee para cada recall:

| recall | índice | rescoreLimit | searchProbe | recall medido | lecturas de postings | lecturas de rescore | lecturas por query | bytes por query |
|---|---|---|---|---|---|---|---|---|
| 0.95 | sin limpiar, v4.0 | 100 | 185 | 0.9500 | 210.2 | 100.0 | 310.2 | 8.14 MB |
| 0.95 | limpio | 100 | 214 | 0.9500 | 243.1 | 100.0 | 343.1 | 9.32 MB |
| 0.99 | sin limpiar, v4.0 | 350 | 928 | 0.9900 | 1 052.9 | 350.0 | 1 402.9 | 40.00 MB |
| 0.99 | limpio | 350 | 1 000 | 0.9900 | 1 134.1 | 350.0 | 1 484.1 | 42.94 MB |

Lo que leen las dos celdas, del manifiesto de v4.0:

| recall | celda | ef | cargas | lecturas de bloques | lecturas de rescore | lecturas por query | bytes por query |
|---|---|---|---|---|---|---|---|
| 0.95 | posting-edge, 512 bits en RAM | 21 | 18.59 | 37.2 | 64.0 | 101.2 | 1.56 MB |
| 0.95 | posting-edge, halo de 512 bits | 21 | 18.59 | 55.8 | 64.0 | 119.8 | 3.79 MB |
| 0.99 | posting-edge, 512 bits en RAM | 187 | 76.30 | 152.6 | 64.0 | 216.6 | 5.12 MB |
| 0.99 | posting-edge, halo de 512 bits | 187 | 76.30 | 228.9 | 64.0 | 292.9 | 14.25 MB |

**El presupuesto que dio el limitador:** 20.0 por query con 1 thread y 14.4 con 8.

### Recall 0.95, en frío, primera ronda

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| HFresh, postings uno a uno | searchProbe 214, rescoreLimit 100 | 51.96 | 61.83 | 64.66 | 19 | 83.95 | 106.57 | 108.26 | 87 |
| HFresh, postings de 8 en 8 | searchProbe 214, rescoreLimit 100 | 14.57 | 17.14 | 17.80 | 69 | 80.76 | 133.20 | 157.52 | 91 |
| posting-edge, 512 bits en RAM | ef 21 | 3.54 | 8.10 | 13.13 | 238 | 11.57 | 23.15 | 31.03 | 527 |
| posting-edge, halo de 512 bits | ef 21 | 4.76 | 14.39 | 24.57 | 160 | 15.74 | 37.41 | 50.23 | 355 |

### Recall 0.95, en caliente, primera ronda

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| HFresh, postings uno a uno | searchProbe 214, rescoreLimit 100 | 3.40 | 4.58 | 4.96 | 286 | 9.97 | 13.00 | 14.24 | 799 |
| HFresh, postings de 8 en 8 | searchProbe 214, rescoreLimit 100 | 2.75 | 3.91 | 4.48 | 350 | 9.59 | 13.47 | 15.19 | 817 |
| posting-edge, 512 bits en RAM | ef 21 | 0.90 | 2.94 | 4.39 | 843 | 1.27 | 4.03 | 6.12 | 4 764 |
| posting-edge, halo de 512 bits | ef 21 | 1.26 | 4.82 | 7.36 | 562 | 2.11 | 7.74 | 12.21 | 2 750 |

### Recall 0.99, en frío, primera ronda

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| HFresh, postings uno a uno | searchProbe 1000, rescoreLimit 350 | 182.11 | 259.94 | 271.64 | 6 | 246.71 | 361.63 | 368.74 | 30 |
| HFresh, postings de 8 en 8 | searchProbe 1000, rescoreLimit 350 | 47.07 | 67.56 | 69.10 | 21 | 234.58 | 361.10 | 472.38 | 31 |
| posting-edge, 512 bits en RAM | ef 187 | 9.03 | 27.41 | 39.54 | 87 | 23.73 | 55.11 | 72.43 | 237 |
| posting-edge, halo de 512 bits | ef 187 | 14.08 | 50.22 | 73.32 | 52 | 40.45 | 107.54 | 133.52 | 131 |

### Recall 0.99, en caliente, primera ronda

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| HFresh, postings uno a uno | searchProbe 1000, rescoreLimit 350 | 15.21 | 17.05 | 17.65 | 65 | 41.77 | 51.37 | 57.66 | 191 |
| HFresh, postings de 8 en 8 | searchProbe 1000, rescoreLimit 350 | 11.70 | 13.85 | 14.54 | 84 | 42.44 | 58.31 | 66.46 | 185 |
| posting-edge, 512 bits en RAM | ef 187 | 3.23 | 10.75 | 15.58 | 235 | 4.40 | 14.92 | 21.72 | 1 375 |
| posting-edge, halo de 512 bits | ef 187 | 4.87 | 17.53 | 25.55 | 151 | 7.98 | 27.76 | 40.87 | 754 |

### HFresh entre cada celda: cuántas veces el p50 de la celda

| recall | HFresh | celda | en frío, 1 thread | en frío, 8 threads | en caliente, 1 thread | en caliente, 8 threads |
|---|---|---|---|---|---|---|
| 0.95 | postings uno a uno | 512 bits en RAM | 14.69x | 7.26x | 3.79x | 7.83x |
| 0.95 | postings uno a uno | halo de 512 bits | 10.91x | 5.33x | 2.70x | 4.72x |
| 0.95 | postings de 8 en 8 | 512 bits en RAM | 4.12x | 6.98x | 3.06x | 7.53x |
| 0.95 | postings de 8 en 8 | halo de 512 bits | 3.06x | 5.13x | 2.18x | 4.54x |
| 0.99 | postings uno a uno | 512 bits en RAM | 20.17x | 10.40x | 4.71x | 9.49x |
| 0.99 | postings uno a uno | halo de 512 bits | 12.94x | 6.10x | 3.12x | 5.23x |
| 0.99 | postings de 8 en 8 | 512 bits en RAM | 5.21x | 9.88x | 3.63x | 9.65x |
| 0.99 | postings de 8 en 8 | halo de 512 bits | 3.34x | 5.80x | 2.40x | 5.32x |

### Los criterios de v4.0, con estos números

- **Posting-edge, 512 bits en RAM tiene menos latencia que HFresh, en frío a 0.95 con 1 thread:** 3.54 ms frente a 51.96, 14.69x. Frente a HFresh con sus postings de 8 en 8, que no es producto: 14.57 ms, 4.12x.
  - Llega a 0.99.
- **Posting-edge, halo de 512 bits tiene menos latencia que HFresh, en frío a 0.95 con 1 thread:** 4.76 ms frente a 51.96, 10.91x. Frente a HFresh con sus postings de 8 en 8, que no es producto: 14.57 ms, 3.06x.
  - Llega a 0.99.

### Junto a lo publicado en v4.0, que es otra sesión, con el grafo sin limpiar y presupuesto de 8

| índice | recall | | publicado: 1 thread, p50 ms | esta sesión | | publicado: 8 threads, p50 ms | esta sesión | |
|---|---|---|---|---|---|---|---|---|
| HFresh, postings uno a uno | 0.95 | en frío | 46.80 | 51.96 | +11.0 % | 77.84 | 83.95 | +7.9 % |
| HFresh, postings uno a uno | 0.95 | en caliente | 3.38 | 3.40 | +0.5 % | 8.97 | 9.97 | +11.2 % |
| HFresh, postings de 8 en 8 | 0.95 | en frío | 14.28 | 14.57 | +2.0 % | 73.63 | 80.76 | +9.7 % |
| HFresh, postings de 8 en 8 | 0.95 | en caliente | 2.85 | 2.75 | −3.7 % | 8.73 | 9.59 | +9.9 % |
| HFresh, postings uno a uno | 0.99 | en frío | 178.42 | 182.11 | +2.1 % | 242.75 | 246.71 | +1.6 % |
| HFresh, postings uno a uno | 0.99 | en caliente | 16.06 | 15.21 | −5.3 % | 42.14 | 41.77 | −0.9 % |
| HFresh, postings de 8 en 8 | 0.99 | en frío | 47.89 | 47.07 | −1.7 % | 228.50 | 234.58 | +2.7 % |
| HFresh, postings de 8 en 8 | 0.99 | en caliente | 12.73 | 11.70 | −8.1 % | 41.00 | 42.44 | +3.5 % |
| posting-edge, 512 bits en RAM | 0.95 | en frío | 3.54 | 3.54 | −0.1 % | 11.43 | 11.57 | +1.2 % |
| posting-edge, 512 bits en RAM | 0.95 | en caliente | 0.89 | 0.90 | +0.4 % | 1.27 | 1.27 | +0.6 % |
| posting-edge, 512 bits en RAM | 0.99 | en frío | 8.89 | 9.03 | +1.5 % | 23.55 | 23.73 | +0.8 % |
| posting-edge, 512 bits en RAM | 0.99 | en caliente | 3.16 | 3.23 | +2.0 % | 4.16 | 4.40 | +5.8 % |
| posting-edge, halo de 512 bits | 0.95 | en frío | 4.80 | 4.76 | −0.8 % | 16.03 | 15.74 | −1.8 % |
| posting-edge, halo de 512 bits | 0.95 | en caliente | 1.27 | 1.26 | −0.7 % | 2.06 | 2.11 | +2.7 % |
| posting-edge, halo de 512 bits | 0.99 | en frío | 14.02 | 14.08 | +0.4 % | 39.90 | 40.45 | +1.4 % |
| posting-edge, halo de 512 bits | 0.99 | en caliente | 4.86 | 4.87 | +0.2 % | 7.93 | 7.98 | +0.7 % |

- **No es la misma configuración de HFresh** cuando el searchProbe de arriba cambia entre las dos filas.

### Las partes de la latencia media de HFresh, con 1 thread

| índice | recall | | | media, ms | centroides | leer los postings | scan | rescore |
|---|---|---|---|---|---|---|---|---|
| HFresh, postings uno a uno | 0.95 | en frío | publicado | 46.44 | 0.80 | 40.48 | 1.30 | 3.79 |
| HFresh, postings uno a uno | 0.95 | en frío | esta sesión | 51.39 | 0.35 | 46.01 | 1.50 | 3.46 |
| HFresh, postings uno a uno | 0.95 | en caliente | publicado | 3.45 | 0.67 | 1.15 | 1.27 | 0.29 |
| HFresh, postings uno a uno | 0.95 | en caliente | esta sesión | 3.50 | 0.29 | 1.37 | 1.48 | 0.29 |
| HFresh, postings de 8 en 8 | 0.95 | en frío | publicado | 14.40 | 0.80 | 8.27 | 1.41 | 3.85 |
| HFresh, postings de 8 en 8 | 0.95 | en frío | esta sesión | 14.56 | 0.34 | 9.21 | 1.49 | 3.46 |
| HFresh, postings de 8 en 8 | 0.95 | en caliente | publicado | 2.92 | 0.69 | 0.51 | 1.36 | 0.30 |
| HFresh, postings de 8 en 8 | 0.95 | en caliente | esta sesión | 2.86 | 0.30 | 0.62 | 1.57 | 0.30 |
| HFresh, postings uno a uno | 0.99 | en frío | publicado | 174.97 | 3.48 | 153.34 | 7.13 | 10.87 |
| HFresh, postings uno a uno | 0.99 | en frío | esta sesión | 180.72 | 1.29 | 161.73 | 7.62 | 9.91 |
| HFresh, postings uno a uno | 0.99 | en caliente | publicado | 16.16 | 3.11 | 5.97 | 6.25 | 0.68 |
| HFresh, postings uno a uno | 0.99 | en caliente | esta sesión | 15.38 | 1.16 | 6.69 | 6.68 | 0.68 |
| HFresh, postings de 8 en 8 | 0.99 | en frío | publicado | 49.28 | 3.50 | 28.37 | 6.20 | 11.05 |
| HFresh, postings de 8 en 8 | 0.99 | en frío | esta sesión | 47.67 | 1.30 | 29.73 | 6.66 | 9.83 |
| HFresh, postings de 8 en 8 | 0.99 | en caliente | publicado | 12.94 | 3.09 | 2.51 | 6.50 | 0.69 |
| HFresh, postings de 8 en 8 | 0.99 | en caliente | esta sesión | 11.95 | 1.15 | 2.95 | 7.00 | 0.69 |

### Deriva entre rondas y actividad

- **Ninguna mediana se movió más de un 10 % entre rondas,** de 32.
- **La sesión:** 12 tramos, de 2026-11-19 23:00:58 a 2026-11-20 00:14:06.
- **Tramos con teclado o trackpad:** n1 hfresh-best95-par, n1 hfresh-best99.
- **Tramos con eventos de energía o de actividad:** n1 hfresh-best95-par (2), n1 hfresh-best99 (1), n1 hfresh-best99-par (1).
