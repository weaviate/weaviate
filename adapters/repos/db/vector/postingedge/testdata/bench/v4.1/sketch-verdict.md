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
| la actual: 512 bits en RAM | 243.0 MB | 6 083 MB | 105.7 MB | 6 431 MB | — | — |
| scan con 512 bits | 36.8 MB | 6 083 MB | 105.7 MB | 6 225 MB | — | — |
| scan con 768 bits | 36.8 MB | 6 083 MB | 137.4 MB | 6 257 MB | — | — |

- **El heap lleva unos 40 MB del arnés y del proceso,** como en v4.0 («El heap a 1M, por componente»).

La sesión cronometrada no ha corrido todavía.
