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

La sesión cronometrada no ha corrido todavía.
