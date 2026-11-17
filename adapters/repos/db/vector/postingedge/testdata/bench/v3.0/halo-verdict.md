### Estado

| S | ruteo de endpoints | bytes del código | recall máximo, con rescoreLimit 64 | gate de 0.99 | cargas a 0.95 | cargas a 0.99 | frente a su referencia a 0.99 | cruza 0.99 entre | recall en esos ef | endpoints puntuados a 0.99 |
|---|---|---|---|---|---|---|---|---|---|---|
| 236 | referencia: código completo | 200 | 0.9973 | pasa | 10.11 | 52.77 | 1.00x | ef 160 y 320 | 0.9859 y 0.9908 | 26630 |
| 236 | sketch de 256 bits | 40 | 0.9988 | pasa | 37.63 | 150.49 | 2.85x | ef 160 y 320 | 0.9886 y 0.9937 | 36493 |
| 236 | sketch de 384 bits | 56 | 0.9986 | pasa | 23.71 | 90.42 | 1.71x | ef 160 y 320 | 0.9899 y 0.9940 | 28174 |
| 236 | sketch de 512 bits | 72 | 0.9983 | pasa | 18.26 | 78.31 | 1.48x | ef 160 y 320 | 0.9886 y 0.9938 | 28491 |
| 1024 | referencia: código completo | 200 | 0.9985 | pasa | 6.16 | 20.54 | 1.00x | ef 80 y 160 | 0.9869 y 0.9913 | 23174 |
| 1024 | sketch de 256 bits | 40 | 0.9993 | pasa | 15.92 | 54.23 | 2.64x | ef 80 y 160 | 0.9886 y 0.9939 | 27779 |
| 1024 | sketch de 384 bits | 56 | 0.9992 | pasa | 11.42 | 37.56 | 1.83x | ef 80 y 160 | 0.9890 y 0.9935 | 24940 |
| 1024 | sketch de 512 bits | 72 | 0.9991 | pasa | 9.47 | 29.35 | 1.43x | ef 80 y 160 | 0.9891 y 0.9933 | 23202 |

### El criterio

**El halo es viable si alguna celda alcanza 0.99 con 3× o menos las cargas de la referencia.**

- **S = 236:** la referencia carga 52.77 postings a 0.99, y 3× son 158.30.
  - **Sketch de 256 bits:** 150.49 cargas a 0.99, 2.85x las de la referencia: **cumple**.
  - **Sketch de 384 bits:** 90.42 cargas a 0.99, 1.71x las de la referencia: **cumple**.
  - **Sketch de 512 bits:** 78.31 cargas a 0.99, 1.48x las de la referencia: **cumple**.
- **S = 1024:** la referencia carga 20.54 postings a 0.99, y 3× son 61.62.
  - **Sketch de 256 bits:** 54.23 cargas a 0.99, 2.64x las de la referencia: **cumple**.
  - **Sketch de 384 bits:** 37.56 cargas a 0.99, 1.83x las de la referencia: **cumple**.
  - **Sketch de 512 bits:** 29.35 cargas a 0.99, 1.43x las de la referencia: **cumple**.

**Veredicto: el halo es viable.** Cumplen S = 236 con 256 bits (2.85x), S = 236 con 384 bits (1.71x), S = 236 con 512 bits (1.48x), S = 1024 con 256 bits (2.64x), S = 1024 con 384 bits (1.83x), S = 1024 con 512 bits (1.43x).

### El posting, bloque a bloque

**S = 236:** 4195 postings. 8.98 aristas por miembro, y 0.622 endpoints distintos por arista.

| bloque | unidades por posting: media | p95 | bytes por unidad | bytes por posting: media | p95 |
|---|---|---|---|---|---|
| miembros | 236.0 | 380 | 208 | 49.1 KB | 79.0 KB |
| aristas | 2118.7 | 3665 | 4 | 8.5 KB | 14.7 KB |
| halo, sketch de 256 bits | 1318.0 endpoints distintos | 2557 | 44 | 58.0 KB | 112.5 KB |
| halo, sketch de 384 bits | 1318.0 endpoints distintos | 2557 | 60 | 79.1 KB | 153.4 KB |
| halo, sketch de 512 bits | 1318.0 endpoints distintos | 2557 | 76 | 100.2 KB | 194.3 KB |
| halo con el código completo, informativo | 1318.0 endpoints distintos | 2557 | 204 | 268.9 KB | 521.6 KB |

| el posting entero | media | p95 | frente al posting sin halo |
|---|---|---|---|
| miembros y aristas, con sidecar residente | 57.6 KB | 90.6 KB | 1.00x |
| con halo de 256 bits | 115.6 KB | 189.3 KB | 2.01x |
| con halo de 384 bits | 136.6 KB | 229.0 KB | 2.37x |
| con halo de 512 bits | 157.7 KB | 268.7 KB | 2.74x |

**S = 1024:** 967 postings. 7.39 aristas por miembro, y 0.564 endpoints distintos por arista.

| bloque | unidades por posting: media | p95 | bytes por unidad | bytes por posting: media | p95 |
|---|---|---|---|---|---|
| miembros | 1023.8 | 1569 | 208 | 212.9 KB | 326.4 KB |
| aristas | 7566.2 | 13276 | 4 | 30.3 KB | 53.1 KB |
| halo, sketch de 256 bits | 4265.4 endpoints distintos | 8183 | 44 | 187.7 KB | 360.1 KB |
| halo, sketch de 384 bits | 4265.4 endpoints distintos | 8183 | 60 | 255.9 KB | 491.0 KB |
| halo, sketch de 512 bits | 4265.4 endpoints distintos | 8183 | 76 | 324.2 KB | 621.9 KB |
| halo con el código completo, informativo | 4265.4 endpoints distintos | 8183 | 204 | 870.1 KB | 1669.3 KB |

| el posting entero | media | p95 | frente al posting sin halo |
|---|---|---|---|
| miembros y aristas, con sidecar residente | 243.2 KB | 366.4 KB | 1.00x |
| con halo de 256 bits | 430.9 KB | 691.8 KB | 1.77x |
| con halo de 384 bits | 499.1 KB | 808.7 KB | 2.05x |
| con halo de 512 bits | 567.4 KB | 940.3 KB | 2.33x |

### Resultado principal: bytes leídos por query

De los postings que carga cada query. El rescore va aparte: 64 lecturas de 6.1 KB en los dos modos.

**A recall 0.95:**

| S | celda | modo | cargas | miembros | aristas | halo | total | frente al sidecar residente de su S | frente al de S = 236 |
|---|---|---|---|---|---|---|---|---|---|
| 236 | referencia: código completo | sidecar residente | 10.11 | 0.54 MB | 0.10 MB | — | 0.63 MB | 1.00x | 1.00x |
| 236 | sketch de 256 bits | todo en disco | 37.63 | 1.96 MB | 0.37 MB | 2.67 MB | 5.00 MB | 7.89x | 7.89x |
| 236 | sketch de 384 bits | todo en disco | 23.71 | 1.24 MB | 0.23 MB | 2.25 MB | 3.72 MB | 5.86x | 5.86x |
| 236 | sketch de 512 bits | todo en disco | 18.26 | 0.96 MB | 0.18 MB | 2.19 MB | 3.33 MB | 5.25x | 5.25x |
| 1024 | referencia: código completo | sidecar residente | 6.16 | 1.39 MB | 0.21 MB | — | 1.60 MB | 1.00x | 2.53x |
| 1024 | sketch de 256 bits | todo en disco | 15.92 | 3.56 MB | 0.56 MB | 3.58 MB | 7.70 MB | 4.80x | 12.14x |
| 1024 | sketch de 384 bits | todo en disco | 11.42 | 2.55 MB | 0.40 MB | 3.45 MB | 6.40 MB | 3.99x | 10.09x |
| 1024 | sketch de 512 bits | todo en disco | 9.47 | 2.12 MB | 0.33 MB | 3.61 MB | 6.07 MB | 3.78x | 9.57x |

**A recall 0.99:**

| S | celda | modo | cargas | miembros | aristas | halo | total | frente al sidecar residente de su S | frente al de S = 236 |
|---|---|---|---|---|---|---|---|---|---|
| 236 | referencia: código completo | sidecar residente | 52.77 | 2.75 MB | 0.51 MB | — | 3.26 MB | 1.00x | 1.00x |
| 236 | sketch de 256 bits | todo en disco | 150.49 | 7.80 MB | 1.46 MB | 10.35 MB | 19.61 MB | 6.01x | 6.01x |
| 236 | sketch de 384 bits | todo en disco | 90.42 | 4.70 MB | 0.88 MB | 8.49 MB | 14.07 MB | 4.31x | 4.31x |
| 236 | sketch de 512 bits | todo en disco | 78.31 | 4.07 MB | 0.77 MB | 9.36 MB | 14.20 MB | 4.35x | 4.35x |
| 1024 | referencia: código completo | sidecar residente | 20.54 | 4.58 MB | 0.71 MB | — | 5.30 MB | 1.00x | 1.62x |
| 1024 | sketch de 256 bits | todo en disco | 54.23 | 12.02 MB | 1.87 MB | 11.91 MB | 25.80 MB | 4.87x | 7.90x |
| 1024 | sketch de 384 bits | todo en disco | 37.56 | 8.34 MB | 1.30 MB | 11.26 MB | 20.90 MB | 3.95x | 6.40x |
| 1024 | sketch de 512 bits | todo en disco | 29.35 | 6.53 MB | 1.02 MB | 11.22 MB | 18.78 MB | 3.55x | 5.75x |

### Recall y cargas a igual ef, frente a la referencia

**S = 236,** con rescoreLimit 64:

| ef | referencia: recall | cargas | 256 bits: recall | cargas | 384 bits: recall | cargas | 512 bits: recall | cargas |
|---|---|---|---|---|---|---|---|---|
| 10 | 0.9224 | 7.60 | 0.9141 (-0.0083) | 21.93 (2.89x) | 0.9176 (-0.0047) | 15.36 (2.02x) | 0.9175 (-0.0049) | 12.30 (1.62x) |
| 20 | 0.9504 | 10.16 | 0.9443 (-0.0062) | 33.65 (3.31x) | 0.9485 (-0.0019) | 22.97 (2.26x) | 0.9495 (-0.0009) | 18.07 (1.78x) |
| 40 | 0.9665 | 14.58 | 0.9671 (+0.0006) | 52.54 (3.60x) | 0.9698 (+0.0033) | 35.72 (2.45x) | 0.9699 (+0.0033) | 27.61 (1.89x) |
| 80 | 0.9783 | 22.14 | 0.9804 (+0.0021) | 82.67 (3.73x) | 0.9827 (+0.0044) | 56.06 (2.53x) | 0.9818 (+0.0034) | 42.99 (1.94x) |
| 160 | 0.9859 | 35.20 | 0.9886 (+0.0027) | 132.11 (3.75x) | 0.9899 (+0.0041) | 89.57 (2.54x) | 0.9886 (+0.0027) | 68.56 (1.95x) |
| 320 | 0.9908 | 57.28 | 0.9937 (+0.0029) | 212.71 (3.71x) | 0.9940 (+0.0031) | 144.90 (2.53x) | 0.9938 (+0.0029) | 111.03 (1.94x) |
| 640 | 0.9940 | 95.54 | 0.9966 (+0.0026) | 345.85 (3.62x) | 0.9965 (+0.0025) | 236.99 (2.48x) | 0.9962 (+0.0022) | 182.47 (1.91x) |
| 1024 | 0.9960 | 136.39 | 0.9978 (+0.0018) | 480.77 (3.53x) | 0.9977 (+0.0017) | 332.30 (2.44x) | 0.9973 (+0.0013) | 256.60 (1.88x) |
| 2048 | 0.9973 | 233.97 | 0.9988 (+0.0014) | 772.16 (3.30x) | 0.9986 (+0.0012) | 546.57 (2.34x) | 0.9983 (+0.0010) | 427.94 (1.83x) |

**S = 1024,** con rescoreLimit 64:

| ef | referencia: recall | cargas | 256 bits: recall | cargas | 384 bits: recall | cargas | 512 bits: recall | cargas |
|---|---|---|---|---|---|---|---|---|
| 10 | 0.9512 | 6.16 | 0.9458 (-0.0053) | 14.72 (2.39x) | 0.9470 (-0.0042) | 10.86 (1.76x) | 0.9465 (-0.0046) | 8.99 (1.46x) |
| 20 | 0.9700 | 7.99 | 0.9663 (-0.0038) | 21.65 (2.71x) | 0.9681 (-0.0019) | 15.46 (1.94x) | 0.9693 (-0.0008) | 12.61 (1.58x) |
| 40 | 0.9797 | 10.84 | 0.9805 (+0.0008) | 32.19 (2.97x) | 0.9825 (+0.0028) | 22.77 (2.10x) | 0.9817 (+0.0019) | 18.13 (1.67x) |
| 80 | 0.9869 | 15.48 | 0.9886 (+0.0017) | 48.47 (3.13x) | 0.9890 (+0.0021) | 34.15 (2.21x) | 0.9891 (+0.0022) | 26.83 (1.73x) |
| 160 | 0.9913 | 23.09 | 0.9939 (+0.0026) | 73.90 (3.20x) | 0.9935 (+0.0022) | 51.98 (2.25x) | 0.9933 (+0.0020) | 40.77 (1.77x) |
| 320 | 0.9942 | 35.39 | 0.9969 (+0.0027) | 113.17 (3.20x) | 0.9963 (+0.0021) | 80.03 (2.26x) | 0.9961 (+0.0019) | 62.75 (1.77x) |
| 640 | 0.9963 | 55.87 | 0.9980 (+0.0017) | 172.93 (3.10x) | 0.9980 (+0.0017) | 123.93 (2.22x) | 0.9977 (+0.0014) | 97.73 (1.75x) |
| 1024 | 0.9975 | 76.94 | 0.9987 (+0.0012) | 228.02 (2.96x) | 0.9988 (+0.0013) | 166.32 (2.16x) | 0.9985 (+0.0010) | 132.38 (1.72x) |
| 2048 | 0.9985 | 124.05 | 0.9993 (+0.0008) | 335.78 (2.71x) | 0.9992 (+0.0007) | 253.19 (2.04x) | 0.9991 (+0.0006) | 205.97 (1.66x) |

### Lo que ocupa cada modo

| S | modo | disco: miembros | aristas | halo | total | RAM: sidecar | mapa vector→posting |
|---|---|---|---|---|---|---|---|
| 236 | sidecar residente | 205.92 MB | 35.55 MB | — | 241.47 MB | 198.00 MB | 3.96 MB |
| 236 | todo en disco, sketch de 256 bits | 205.92 MB | 35.55 MB | 243.27 MB | 484.74 MB | — | 3.96 MB |
| 236 | todo en disco, sketch de 384 bits | 205.92 MB | 35.55 MB | 331.73 MB | 573.20 MB | — | 3.96 MB |
| 236 | todo en disco, sketch de 512 bits | 205.92 MB | 35.55 MB | 420.19 MB | 661.67 MB | — | 3.96 MB |
| 1024 | sidecar residente | 205.92 MB | 29.27 MB | — | 235.19 MB | 198.00 MB | 3.96 MB |
| 1024 | todo en disco, sketch de 256 bits | 205.92 MB | 29.27 MB | 181.48 MB | 416.67 MB | — | 3.96 MB |
| 1024 | todo en disco, sketch de 384 bits | 205.92 MB | 29.27 MB | 247.48 MB | 482.66 MB | — | 3.96 MB |
| 1024 | todo en disco, sketch de 512 bits | 205.92 MB | 29.27 MB | 313.47 MB | 548.66 MB | — | 3.96 MB |

Los centroides quedan residentes en los dos modos, y no se cuentan.

### La regla de ruido

- **Cada celda se compara con la referencia de su build,** dentro de la corrida, sobre el mismo índice y con las mismas queries.
- **La columna frente a S = 236 compara dos particiones distintas.** Lleva lo que se separan dos builds, sin medir aquí.
- **Nada a 0.99 se compara con otra corrida.**
