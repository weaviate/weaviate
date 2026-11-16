### Estado de las celdas

| ef de inserción | estado | splits (fallidos) | tamaño medio (p50 / p95 / máx) | cobertura | no-nearest | µs por insert, indicativo |
|---|---|---|---|---|---|---|
| 320, ancla | completa | 2070 (0) | 237.5 (239 / 344 / 360) | complete | 0.0242 | 7442 |
| 320, ancla repetida | completa | 2053 (0) | 238.5 (240 / 345 / 360) | complete | 0.0248 | 3929 |
| 160 | completa | 2044 (0) | 239.0 (241 / 345 / 360) | complete | 0.0243 | 6216 |
| 80 | completa | 2070 (0) | 237.5 (239 / 344 / 360) | complete | 0.0254 | 5007 |
| 40 | completa | 2067 (0) | 237.7 (240 / 343 / 360) | complete | 0.0253 | 3971 |
| 20 | cortada: edge recall at insert 0.8992 in the stretch to 80% | 1248 (0) | — | complete | 0.0250 | 3402 |

### Recall a igual ef de búsqueda al 100 %, diferencia frente al lote

| ef de búsqueda | lote | 320, ancla | 320, ancla repetida | 160 | 80 | 40 |
|---|---|---|---|---|---|---|
| 10 | 0.9229 | +0.0008 | +0.0008 | +0.0008 | -0.0024 | -0.0057 |
| 20 | 0.9506 | +0.0022 | +0.0020 | +0.0010 | -0.0013 | -0.0044 |
| 40 | 0.9664 | +0.0013 | +0.0022 | +0.0003 | -0.0019 | -0.0055 |
| 80 | 0.9782 | +0.0008 | +0.0014 | -0.0001 | -0.0018 | -0.0050 |
| 160 | 0.9858 | +0.0009 | +0.0013 | +0.0003 | -0.0015 | -0.0037 |
| 320 | 0.9908 | +0.0011 | +0.0013 | +0.0008 | -0.0006 | -0.0017 |
| 640 | 0.9940 | +0.0012 | +0.0013 | +0.0010 | +0.0002 | -0.0007 |
| 1024 | 0.9960 | +0.0007 | +0.0006 | +0.0007 | -0.0002 | -0.0007 |
| peor | | +0.0007 | +0.0006 | -0.0001 | -0.0024 | -0.0057 |

### Criterio

| ef de inserción | peor diferencia de recall (umbral −0.005) | cargas a 0.95 (umbral +15 %) | cargas a 0.99 (umbral +10 %) | guardia: peor tramo (umbral 0.90) | cumple |
|---|---|---|---|---|---|
| 320, ancla | +0.0007: cumple | 10.3 (+2.3 %): cumple | 49.8 (-5.7 %): cumple | 0.9846 | **sí** |
| 320, ancla repetida | +0.0006: cumple | 10.2 (+1.8 %): cumple | 48.3 (-8.5 %): cumple | 0.9849 | **sí** |
| 160 | -0.0001: cumple | 10.4 (+3.0 %): cumple | 51.0 (-3.3 %): cumple | 0.9765 | **sí** |
| 80 | -0.0024: cumple | 10.7 (+6.2 %): cumple | 58.0 (+9.8 %): cumple, dentro del ruido | 0.9623 | **sí** |
| 40 | -0.0057: no cumple, dentro del ruido | 11.5 (+14.4 %): cumple | 64.9 (+23.0 %): no cumple | 0.9400 | **no** |
| 20 | — | — | — | 0.8992 | **no:** cortada: edge recall at insert 0.8992 in the stretch to 80% |

- **Elegido: ef 80,** el menor que cumple.
- **Suelo de ruido, de las dos corridas del ancla:** hasta 0.0009 de recall (ef 10: 0.0001, ef 20: 0.0002, ef 40: 0.0009, ef 80: 0.0007, ef 160: 0.0004, ef 320: 0.0003, ef 640: 0.0002, ef 1024: 0.0001), un 0.5 % en cargas a 0.95 y un 3.0 % a 0.99.

### La búsqueda de un insert: lo que lee en disco

Conteo directo de la búsqueda de cada insert, por insert, hasta donde llegó la celda. La aproximación es el trabajo de las queries de un checkpoint a un ef de búsqueda igual al de inserción, que es lo único que hay para el ancla.

| ef de inserción | cargas: media | p50 | p95 | máximo | rondas | vectores escaneados | endpoints puntuados | rescoreados | aproximación, cargas al 50 % | al 100 % | conteo / media de la aproximación |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 320, ancla | — | — | — | — | — | — | — | — | 53.5 | 59.4 | — |
| 320, ancla repetida | — | — | — | — | — | — | — | — | 53.5 | 59.3 | — |
| 160 | 34.6 | 33 | 69 | 157 | 8.4 | 8642 | 17411 | 64.0 | 32.7 | 36.4 | 1.003 |
| 80 | 21.9 | 21 | 45 | 109 | 5.2 | 5474 | 11034 | 64.0 | 20.7 | 22.8 | 1.003 |
| 40 | 14.4 | 13 | 29 | 69 | 3.3 | 3606 | 7085 | 64.0 | 13.7 | 15.0 | 1.003 |
| 20 | 9.9 | 9 | 21 | 57 | 2.2 | 2477 | 4477 | 64.0 | 9.6 | — | — |

**Por tramo:** cargas de posting por insert, y entre paréntesis las escrituras de aristas inversas por insert.

| ef de inserción | hasta el 60 % | hasta el 70 % | hasta el 80 % | hasta el 90 % | hasta el 100 % |
|---|---|---|---|---|---|
| 320, ancla | — (4.38) | — (4.54) | — (4.64) | — (4.74) | — (4.79) |
| 320, ancla repetida | — | — | — | — | — (4.62) |
| 160 | 33.4 (4.34) | 33.8 (4.50) | 34.6 (4.61) | 35.3 (4.69) | 35.9 (4.74) |
| 80 | 21.1 (4.30) | 21.4 (4.46) | 21.9 (4.57) | 22.3 (4.65) | 22.7 (4.72) |
| 40 | 13.9 (4.23) | 14.1 (4.38) | 14.4 (4.49) | 14.7 (4.58) | 14.9 (4.64) |
| 20 | 9.8 (4.12) | 9.9 (4.27) | 10.1 (4.37) | — | — |

### Recall de arista al insertar, por tramo

A nivel de vector y, entre paréntesis, de posting. Es la guardia del constructor: por debajo de 0.90 la celda se corta.

| ef de inserción | hasta el 60 % | hasta el 70 % | hasta el 80 % | hasta el 90 % | hasta el 100 % |
|---|---|---|---|---|---|
| 320, ancla | 0.9867 (0.9814) | 0.9853 (0.9789) | 0.9848 (0.9783) | 0.9861 (0.9810) | 0.9846 (0.9790) |
| 320, ancla repetida | — | — | — | — | 0.9849 (0.9785) |
| 160 | 0.9784 (0.9688) | 0.9765 (0.9646) | 0.9770 (0.9661) | 0.9772 (0.9665) | 0.9768 (0.9659) |
| 80 | 0.9653 (0.9488) | 0.9635 (0.9446) | 0.9624 (0.9437) | 0.9623 (0.9442) | 0.9633 (0.9448) |
| 40 | 0.9464 (0.9189) | 0.9412 (0.9106) | 0.9423 (0.9118) | 0.9400 (0.9103) | 0.9418 (0.9129) |
| 20 | 0.9107 (0.8648) | 0.9015 (0.8485) | 0.8992 (0.8493) | — | — |

### Grado y recall de arista contra el corpus actual, al 100 %

| ef de inserción | grado medio: insertados | lote | insertados ahora: arista (posting) | lote ahora: arista (posting) |
|---|---|---|---|---|
| 320, ancla | 9.32 | 8.57 | 0.8245 (0.8730) | 0.6715 (0.7673) |
| 320, ancla repetida | 9.29 | 8.57 | 0.8264 (0.8725) | 0.6700 (0.7670) |
| 160 | 9.28 | 8.57 | 0.8169 (0.8615) | 0.6705 (0.7628) |
| 80 | 9.23 | 8.57 | 0.8007 (0.8400) | 0.6673 (0.7606) |
| 40 | 9.13 | 8.57 | 0.7862 (0.8186) | 0.6614 (0.7539) |

### Escrituras de aristas inversas por insert

| ef de inserción | listas leídas | escrituras: media | p50 | p95 | máximo | inserts sin ninguna | postings distintos: media | p50 | p95 | máximo |
|---|---|---|---|---|---|---|---|---|---|---|
| 320, ancla | — | 4.62 | — | — | — | — | — | — | — | — |
| 320, ancla repetida | 8.97 | 4.62 | 4 | 11 | 16 | 10.5 % | 3.28 | 3 | 8 | 16 |
| 160 | 8.94 | 4.58 | 4 | 11 | 16 | 10.9 % | 3.24 | 3 | 8 | 16 |
| 80 | 8.91 | 4.54 | 4 | 11 | 16 | 11.4 % | 3.19 | 3 | 8 | 16 |
| 40 | 8.83 | 4.46 | 4 | 11 | 16 | 12.3 % | 3.11 | 3 | 8 | 16 |
| 20 | 8.59 | 4.26 | 4 | 11 | 16 | 14.1 % | 2.90 | 2 | 7 | 16 |

### Trabajo de una query a igual recall, al 100 %

| ef de inserción | cargas a 0.95 | endpoints a 0.95 | cargas a 0.99 | endpoints a 0.99 | frente al lote: cargas a 0.95 / a 0.99 | frente al ancla: cargas a 0.95 / a 0.99 | frente al ancla: endpoints a 0.95 / a 0.99 |
|---|---|---|---|---|---|---|---|
| lote | 10.1 | 4953 | 52.8 | 26634 | | | |
| 320, ancla | 10.3 | 5090 | 49.8 | 26183 | +2.3 % / -5.7 % | +0.0 % / +0.0 % | +0.0 % / +0.0 % |
| 320, ancla repetida | 10.2 | 5090 | 48.3 | 25549 | +1.8 % / -8.5 % | -0.5 % / -3.0 % | +0.0 % / -2.4 % |
| 160 | 10.4 | 5140 | 51.0 | 26760 | +3.0 % / -3.3 % | +0.7 % / +2.6 % | +1.0 % / +2.2 % |
| 80 | 10.7 | 5300 | 58.0 | 29760 | +6.2 % / +9.8 % | +3.9 % / +16.5 % | +4.1 % / +13.7 % |
| 40 | 11.5 | 5705 | 64.9 | 32598 | +14.4 % / +23.0 % | +11.9 % / +30.4 % | +12.1 % / +24.5 % |

### Fases de un insert, µs

Indicativo: las celdas de v2.1 corrieron a la vez; el ancla, en paralelo con otras dos celdas, y su repetición, sola.

| ef de inserción | asignación | centroide exacto | búsqueda de aristas | escritura | aristas inversas | split | total |
|---|---|---|---|---|---|---|---|
| 320, ancla | 140 | 1821 | 5340 | 20 | 79 | 35 | 7442 |
| 320, ancla repetida | 71 | 866 | 2901 | 15 | 51 | 21 | 3929 |
| 160 | 148 | 1874 | 3947 | 23 | 84 | 39 | 6216 |
| 80 | 147 | 1975 | 2662 | 23 | 87 | 43 | 5007 |
| 40 | 140 | 1885 | 1755 | 22 | 81 | 39 | 3971 |
| 20 | 150 | 1692 | 1362 | 23 | 86 | 47 | 3402 |
