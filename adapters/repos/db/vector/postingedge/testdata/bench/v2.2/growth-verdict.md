### Estado de las celdas

El lote del 100 %: 4195 postings de 236.0 vectores de media (p50 234, p95 380, máximo 786). La banda de ±15 % va de 200.6 a 271.4.

| celda | estado | umbral | splits (fallidos) | tamaño medio final (p50 / p95 / máx) | frente al lote | cobertura | no-nearest | µs por insert, indicativo |
|---|---|---|---|---|---|---|---|---|
| 160, desde el 10 % | completa | 360 | 3674 (0) | 241.8 (244 / 343 / 360) | +2.5 %: dentro de la banda | complete | 0.0263 | 4020 |
| 80, desde el 10 % | completa | 360 | 3657 (0) | 242.8 (244 / 345 / 360) | +2.9 %: dentro de la banda | complete | 0.0268 | 3088 |

### Checkpoints

El recall y las cargas de los checkpoints intermedios son informativos: no hay lote de ese tamaño con el que compararlos.

**ef de inserción 160:**

| checkpoint | vivos | postings | tamaño medio (p50 / p95 / máx) | al insertar, en el tramo: arista / posting | insertados ahora: arista / posting (grado) | lote ahora: arista / posting (grado) | cobertura | recall a ef 10 | cargas a 0.95 | a 0.99 | truth por fuerza bruta |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 % | 99000 | 420 | 235.7 (228 / 362 / 568) | — | — | 0.9935 / 0.9915 (7.54) | complete | 0.9530 | 6.4 | 16.3 | 2202 |
| 20 % | 198000 | 844 | 234.6 (232 / 337 / 360) | 0.9873 / 0.9806 | 0.8070 / 0.8643 (8.20) | 0.6629 / 0.7728 (7.54) | complete | 0.9428 | 7.5 | 22.6 | 14 |
| 40 % | 396000 | 1659 | 238.7 (239 / 343 / 360) | 0.9789 / 0.9699 | 0.7621 / 0.8315 (8.45) | 0.5508 / 0.6769 (7.54) | complete | 0.9344 | 8.8 | 34.4 | 0 |
| 60 % | 594000 | 2465 | 241.0 (242 / 342 / 360) | 0.9803 / 0.9717 | 0.7349 / 0.8116 (8.62) | 0.5205 / 0.6470 (7.54) | complete | 0.9297 | 9.5 | 41.6 | 0 |
| 80 % | 792000 | 3274 | 241.9 (243 / 343 / 360) | 0.9738 / 0.9655 | 0.7253 / 0.8040 (8.88) | 0.4984 / 0.6151 (7.54) | complete | 0.9260 | 10.1 | 52.6 | 0 |
| 100 % | 990000 | 4094 | 241.8 (244 / 343 / 360) | 0.9743 / 0.9653 | 0.7220 / 0.7975 (8.95) | 0.4870 / 0.5998 (7.54) | complete | 0.9203 | 10.7 | 63.1 | 0 |

**ef de inserción 80:**

| checkpoint | vivos | postings | tamaño medio (p50 / p95 / máx) | al insertar, en el tramo: arista / posting | insertados ahora: arista / posting (grado) | lote ahora: arista / posting (grado) | cobertura | recall a ef 10 | cargas a 0.95 | a 0.99 | truth por fuerza bruta |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 % | 99000 | 420 | 235.7 (228 / 362 / 568) | — | — | 0.9935 / 0.9915 (7.54) | complete | 0.9531 | 6.4 | 16.3 | 2202 |
| 20 % | 198000 | 831 | 238.3 (238 / 345 / 360) | 0.9787 / 0.9655 | 0.8002 / 0.8546 (8.10) | 0.6641 / 0.7720 (7.54) | complete | 0.9426 | 7.5 | 23.8 | 14 |
| 40 % | 396000 | 1637 | 241.9 (245 / 346 / 360) | 0.9642 / 0.9482 | 0.7520 / 0.8167 (8.44) | 0.5486 / 0.6774 (7.54) | complete | 0.9326 | 8.9 | 37.6 | 0 |
| 60 % | 594000 | 2468 | 240.7 (241 / 343 / 360) | 0.9651 / 0.9484 | 0.7270 / 0.7965 (8.58) | 0.5156 / 0.6355 (7.54) | complete | 0.9282 | 9.7 | 44.4 | 0 |
| 80 % | 792000 | 3270 | 242.2 (243 / 346 / 360) | 0.9626 / 0.9453 | 0.7168 / 0.7894 (8.86) | 0.4948 / 0.6111 (7.54) | complete | 0.9217 | 10.4 | 55.8 | 0 |
| 100 % | 990000 | 4077 | 242.8 (244 / 345 / 360) | 0.9626 / 0.9465 | 0.7061 / 0.7800 (8.86) | 0.4855 / 0.5959 (7.54) | complete | 0.9192 | 11.0 | 62.2 | 0 |

### Recall a igual ef de búsqueda al 100 %, diferencia frente al lote

| ef de búsqueda | lote | 160, desde el 10 % | 160, desde el 50 % (v2.1) | 80, desde el 10 % | 80, desde el 50 % (v2.1) |
|---|---|---|---|---|---|
| 10 | 0.9229 | -0.0027 | +0.0008 | -0.0037 | -0.0024 |
| 20 | 0.9506 | +0.0002 | +0.0010 | -0.0021 | -0.0013 |
| 40 | 0.9664 | -0.0002 | +0.0003 | -0.0023 | -0.0019 |
| 80 | 0.9782 | -0.0016 | -0.0001 | -0.0021 | -0.0018 |
| 160 | 0.9858 | -0.0015 | +0.0003 | -0.0015 | -0.0015 |
| 320 | 0.9908 | -0.0010 | +0.0008 | -0.0010 | -0.0006 |
| 640 | 0.9940 | -0.0010 | +0.0010 | -0.0008 | +0.0002 |
| 1024 | 0.9960 | -0.0009 | +0.0007 | -0.0005 | -0.0002 |
| peor | | -0.0027 | -0.0001 | -0.0037 | -0.0024 |

### Criterio

| celda | peor diferencia de recall (umbral −0.005) | cargas a 0.95 (umbral +15 %) | cargas a 0.99 (umbral +10 %) | guardia: peor tramo (umbral 0.90) | cumple |
|---|---|---|---|---|---|
| 160, desde el 10 % | -0.0027: cumple | 10.7 (+6.3 %): cumple | 63.1 (+19.6 %): no cumple | 0.9738 | **no** |
| 80, desde el 10 % | -0.0037: cumple | 11.0 (+9.2 %): cumple | 62.2 (+17.9 %): no cumple | 0.9626 | **no** |
| 160, desde el 50 % (v2.1) | -0.0001: cumple | 10.4 (+3.0 %): cumple | 51.0 (-3.3 %): cumple | 0.9765 | **sí** |
| 80, desde el 50 % (v2.1) | -0.0024: cumple | 10.7 (+6.2 %): cumple | 58.0 (+9.8 %): cumple, dentro del ruido | 0.9623 | **sí** |

- **El tamaño medio final queda dentro de la banda** en las celdas que acabaron. El umbral no se recalibra.
- **Suelo de ruido, de las dos corridas de H1 con ef 320 de v2.0:** hasta 0.0009 de recall, un 0.5 % en cargas a 0.95 y un 3.0 % a 0.99.

### Por tramo

Lo que hicieron los inserts de cada tramo. Las cargas son las de la búsqueda de aristas de cada insert; el grado al insertar y el recall de arista, de la muestra de la guardia.

**ef de inserción 160:**

| tramo | inserts | splits | no-nearest | cargas por insert: media (p50 / p95) | rondas | vectores escaneados | endpoints | recall de arista al insertar (posting) | grado al insertar | escrituras inversas: media (p95) | postings distintos: media (p95) | listas leídas |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 → 20 % | 99000 | 424 | 0.0161 | 29.7 (29 / 57) | 7.2 | 7488 | 11983 | 0.9873 (0.9806) | 8.07 | 4.11 (10) | 2.76 (7) | 7.77 |
| 20 → 40 % | 198000 | 815 | 0.0209 | 32.2 (29 / 61) | 7.8 | 8146 | 14324 | 0.9789 (0.9699) | 8.74 | 4.44 (11) | 3.08 (7) | 8.34 |
| 40 → 60 % | 198000 | 806 | 0.0291 | 34.4 (33 / 65) | 8.3 | 8709 | 16453 | 0.9803 (0.9717) | 9.05 | 4.59 (11) | 3.26 (8) | 8.75 |
| 60 → 80 % | 198000 | 809 | 0.0293 | 35.7 (33 / 69) | 8.7 | 9114 | 17957 | 0.9738 (0.9655) | 9.19 | 4.70 (11) | 3.37 (8) | 8.98 |
| 80 → 100 % | 198000 | 820 | 0.0312 | 36.8 (33 / 73) | 9.0 | 9376 | 19109 | 0.9743 (0.9653) | 9.36 | 4.79 (11) | 3.46 (8) | 9.18 |
| todo | 891000 | 3674 | 0.0263 | 34.2 (33 / 69) | 8.3 | 8686 | 16407 | | | 4.57 (11) | 3.23 (8) | 8.70 |

**ef de inserción 80:**

| tramo | inserts | splits | no-nearest | cargas por insert: media (p50 / p95) | rondas | vectores escaneados | endpoints | recall de arista al insertar (posting) | grado al insertar | escrituras inversas: media (p95) | postings distintos: media (p95) | listas leídas |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 → 20 % | 99000 | 411 | 0.0153 | 18.7 (17 / 37) | 4.4 | 4731 | 7519 | 0.9787 (0.9655) | 8.06 | 4.07 (10) | 2.72 (7) | 7.71 |
| 20 → 40 % | 198000 | 806 | 0.0242 | 20.3 (21 / 41) | 4.8 | 5149 | 8932 | 0.9642 (0.9482) | 8.76 | 4.39 (11) | 3.03 (7) | 8.29 |
| 40 → 60 % | 198000 | 831 | 0.0281 | 21.6 (21 / 41) | 5.2 | 5500 | 10261 | 0.9651 (0.9484) | 8.98 | 4.54 (11) | 3.20 (8) | 8.67 |
| 60 → 80 % | 198000 | 802 | 0.0299 | 22.4 (21 / 45) | 5.4 | 5717 | 11142 | 0.9626 (0.9453) | 9.18 | 4.65 (11) | 3.32 (8) | 8.92 |
| 80 → 100 % | 198000 | 807 | 0.0308 | 23.1 (21 / 45) | 5.5 | 5894 | 11920 | 0.9626 (0.9465) | 9.41 | 4.73 (11) | 3.40 (8) | 9.13 |
| todo | 891000 | 3657 | 0.0268 | 21.5 (21 / 41) | 5.1 | 5472 | 10225 | | | 4.52 (11) | 3.18 (8) | 8.64 |

### Grado por cohorte

El grado medio de todos los vectores de cada cohorte, sin muestra, en cada checkpoint: los del lote y los inserts de cada tramo.

**ef de inserción 160:**

| checkpoint | lote | 10 → 20 % | 20 → 40 % | 40 → 60 % | 60 → 80 % | 80 → 100 % | todos |
|---|---|---|---|---|---|---|---|
| 10 % | 7.63 |  |  |  |  |  | 7.63 |
| 20 % | 7.63 | 8.10 |  |  |  |  | 7.87 |
| 40 % | 7.63 | 8.10 | 8.69 |  |  |  | 8.28 |
| 60 % | 7.63 | 8.10 | 8.69 | 9.08 |  |  | 8.54 |
| 80 % | 7.63 | 8.10 | 8.69 | 9.08 | 9.30 |  | 8.73 |
| 100 % | 7.63 | 8.10 | 8.69 | 9.08 | 9.30 | 9.47 | 8.88 |

**ef de inserción 80:**

| checkpoint | lote | 10 → 20 % | 20 → 40 % | 40 → 60 % | 60 → 80 % | 80 → 100 % | todos |
|---|---|---|---|---|---|---|---|
| 10 % | 7.63 |  |  |  |  |  | 7.63 |
| 20 % | 7.63 | 8.05 |  |  |  |  | 7.84 |
| 40 % | 7.63 | 8.05 | 8.65 |  |  |  | 8.24 |
| 60 % | 7.63 | 8.05 | 8.65 | 9.01 |  |  | 8.50 |
| 80 % | 7.63 | 8.05 | 8.65 | 9.01 | 9.25 |  | 8.69 |
| 100 % | 7.63 | 8.05 | 8.65 | 9.01 | 9.25 | 9.43 | 8.84 |

### Frente a la H1 de 50 → 100 %, al mismo ef de inserción

Al 100 %. La deriva crece con el tramo insertado si la celda que crece desde el 10 % queda peor que la que crece desde el 50 % por más que el suelo de ruido.

**ef de inserción 160:**

| al 100 % | desde el 50 % (v2.1) | desde el 10 % (v2.2) | diferencia | suelo de ruido |
|---|---|---|---|---|
| recall a ef 10 | 0.9237 | 0.9203 | -0.0034 | 0.0009 |
| recall a ef 20 | 0.9516 | 0.9508 | -0.0008, dentro del ruido | 0.0009 |
| recall a ef 40 | 0.9667 | 0.9662 | -0.0005, dentro del ruido | 0.0009 |
| recall a ef 80 | 0.9781 | 0.9766 | -0.0016 | 0.0009 |
| recall a ef 160 | 0.9862 | 0.9843 | -0.0019 | 0.0009 |
| recall a ef 320 | 0.9917 | 0.9898 | -0.0019 | 0.0009 |
| recall a ef 640 | 0.9950 | 0.9930 | -0.0020 | 0.0009 |
| recall a ef 1024 | 0.9966 | 0.9951 | -0.0016 | 0.0009 |
| cargas a 0.95 | 10.4 | 10.7 | +3.2 % | 0.5 % |
| rondas a 0.95 | 2.34 | 2.42 | +3.5 % |  |
| endpoints a 0.95 | 5140.0 | 5245.6 | +2.1 % |  |
| cargas a 0.99 | 51.0 | 63.1 | +23.7 % | 3.0 % |
| rondas a 0.99 | 12.50 | 15.53 | +24.2 % |  |
| endpoints a 0.99 | 26760.4 | 32627.6 | +21.9 % |  |
| recall de arista, insertados ahora | 0.8169 | 0.7220 | -0.0949 | |
| recall de posting, insertados ahora | 0.8615 | 0.7975 | -0.0639 | |
| grado, insertados ahora (muestra) | 9.28 | 8.95 | -0.33 | |
| recall de arista, lote ahora | 0.6705 | 0.4870 | -0.1836 | |
| recall de posting, lote ahora | 0.7628 | 0.5998 | -0.1629 | |
| grado, lote ahora (muestra) | 8.57 | 7.54 | -1.03 | |
| escrituras inversas por insert, acumulado | 4.5789 | 4.5724 | -0.0065 | |
| postings distintos por insert, acumulado | 3.2370 | 3.2315 | -0.0055 | |
| no-nearest, acumulado | 0.0243 | 0.0263 | +0.0020 | |
| cargas por insert, en el tramo del 80 al 100 % | 35.6162 | 36.7994 | +3.3 % | |
| rondas por insert, en el tramo del 80 al 100 % | 8.6549 | 8.9501 | +3.4 % | |
| escrituras inversas por insert, en el tramo del 80 al 100 % | 4.7188 | 4.7851 | +1.4 % | |
| no-nearest, en el tramo del 80 al 100 % | 0.0248 | 0.0312 | +25.8 % | |
| postings al final | 4142 | 4094 | -1.2 % | |
| tamaño medio de posting | 239.0 | 241.8 | +1.2 % | |

**ef de inserción 80:**

| al 100 % | desde el 50 % (v2.1) | desde el 10 % (v2.2) | diferencia | suelo de ruido |
|---|---|---|---|---|
| recall a ef 10 | 0.9206 | 0.9192 | -0.0013 | 0.0009 |
| recall a ef 20 | 0.9494 | 0.9485 | -0.0009, dentro del ruido | 0.0009 |
| recall a ef 40 | 0.9645 | 0.9641 | -0.0004, dentro del ruido | 0.0009 |
| recall a ef 80 | 0.9764 | 0.9761 | -0.0004, dentro del ruido | 0.0009 |
| recall a ef 160 | 0.9843 | 0.9843 | +0.0000, dentro del ruido | 0.0009 |
| recall a ef 320 | 0.9902 | 0.9898 | -0.0004, dentro del ruido | 0.0009 |
| recall a ef 640 | 0.9942 | 0.9932 | -0.0010 | 0.0009 |
| recall a ef 1024 | 0.9957 | 0.9954 | -0.0003, dentro del ruido | 0.0009 |
| cargas a 0.95 | 10.7 | 11.0 | +2.8 % | 0.5 % |
| rondas a 0.95 | 2.42 | 2.50 | +3.0 % |  |
| endpoints a 0.95 | 5299.6 | 5427.5 | +2.4 % |  |
| cargas a 0.99 | 58.0 | 62.2 | +7.3 % | 3.0 % |
| rondas a 0.99 | 14.24 | 15.30 | +7.5 % |  |
| endpoints a 0.99 | 29759.8 | 31806.4 | +6.9 % |  |
| recall de arista, insertados ahora | 0.8007 | 0.7061 | -0.0946 | |
| recall de posting, insertados ahora | 0.8400 | 0.7800 | -0.0600 | |
| grado, insertados ahora (muestra) | 9.23 | 8.86 | -0.37 | |
| recall de arista, lote ahora | 0.6673 | 0.4855 | -0.1819 | |
| recall de posting, lote ahora | 0.7606 | 0.5959 | -0.1647 | |
| grado, lote ahora (muestra) | 8.57 | 7.54 | -1.03 | |
| escrituras inversas por insert, acumulado | 4.5419 | 4.5206 | -0.0214 | |
| postings distintos por insert, acumulado | 3.1856 | 3.1805 | -0.0051 | |
| no-nearest, acumulado | 0.0254 | 0.0268 | +0.0014 | |
| cargas por insert, en el tramo del 80 al 100 % | 22.4755 | 23.0853 | +2.7 % | |
| rondas por insert, en el tramo del 80 al 100 % | 5.3695 | 5.5219 | +2.8 % | |
| escrituras inversas por insert, en el tramo del 80 al 100 % | 4.6868 | 4.7330 | +1.0 % | |
| no-nearest, en el tramo del 80 al 100 % | 0.0260 | 0.0308 | +18.3 % | |
| postings al final | 4168 | 4077 | -2.2 % | |
| tamaño medio de posting | 237.5 | 242.8 | +2.2 % | |

### El trabajo del build, en conteos

En millones. El incremental es el lote del 10 % más los inserts hasta donde llegó la celda. Las cargas del lote son visitas a postings, una cota superior de lo que leería.

| | lote del 100 % | ef 160: lote del 10 % | ef 160: inserts | ef 160: total | ef 160: frente al lote | ef 80: lote del 10 % | ef 80: inserts | ef 80: total | ef 80: frente al lote |
|---|---|---|---|---|---|---|---|---|---|
| cargas de posting | 253.4 M | 25.3 M | 30.5 M | 55.8 M | 0.22x | 25.3 M | 19.2 M | 44.5 M | 0.18x |
| distancias por código | 51008 M | 5907.7 M | 22359 M | 28266 M | 0.55x | 5907.7 M | 13986 M | 19894 M | 0.39x |
| distancias exactas | 4411.6 M | 60.7 M | 396.5 M | 457.2 M | 0.10x | 60.7 M | 390.8 M | 451.5 M | 0.10x |

**El detalle del lote del 100 %** (990000 vectores, 4195 postings):

| parte | millones | por vector |
|---|---|---|
| cargas de posting | 253.4 M | 256.0 |
| distancias por código | 51008 M | 51523.4 |
| exactas: 2-means de la partición | 191.6 M | 193.5 |
| exactas: asignación | 4153.1 M | 4195.0 |
| exactas: rescore de aristas | 63.4 M | 64.0 |
| exactas: construir el grafo de centroides | 3.6 M | 3.7 |

**El detalle de la celda de ef 160** (lote de 99000 vectores en 420 postings, y 891000 inserts):

| parte | millones | por vector del lote, o por insert |
|---|---|---|
| lote del 10 %: cargas de posting | 25.3 M | 256.0 |
| lote del 10 %: distancias por código | 5907.7 M | 59673.3 |
| lote del 10 %: exactas: 2-means de la partición | 12.6 M | 127.3 |
| lote del 10 %: exactas: asignación | 41.6 M | 420.0 |
| lote del 10 %: exactas: rescore de aristas | 6.3 M | 64.0 |
| lote del 10 %: exactas: construir el grafo de centroides | 0.2 M | 1.6 |
| inserts: cargas de posting | 30.5 M | 34.2 |
| inserts: distancias por código | 22359 M | 25093.9 |
| inserts: exactas: asignación por el grafo | 111.1 M | 124.7 |
| inserts: exactas: entrada de la búsqueda, por el grafo | 111.1 M | 124.7 |
| inserts: exactas: rescore de la búsqueda | 57.0 M | 64.0 |
| inserts: exactas: comprobar las aristas inversas | 75.0 M | 84.2 |
| inserts: exactas: 2-means y medias de los splits | 21.8 M | 24.5 |
| inserts: exactas: altas y bajas de centroides en el grafo | 6.8 M | 7.6 |
| inserts: exactas: limpieza de lápidas | 13.5 M | 15.1 |

**El detalle de la celda de ef 80** (lote de 99000 vectores en 420 postings, y 891000 inserts):

| parte | millones | por vector del lote, o por insert |
|---|---|---|
| lote del 10 %: cargas de posting | 25.3 M | 256.0 |
| lote del 10 %: distancias por código | 5907.7 M | 59673.3 |
| lote del 10 %: exactas: 2-means de la partición | 12.6 M | 127.3 |
| lote del 10 %: exactas: asignación | 41.6 M | 420.0 |
| lote del 10 %: exactas: rescore de aristas | 6.3 M | 64.0 |
| lote del 10 %: exactas: construir el grafo de centroides | 0.2 M | 1.6 |
| inserts: cargas de posting | 19.2 M | 21.5 |
| inserts: distancias por código | 13986 M | 15697.5 |
| inserts: exactas: asignación por el grafo | 108.9 M | 122.2 |
| inserts: exactas: entrada de la búsqueda, por el grafo | 108.9 M | 122.2 |
| inserts: exactas: rescore de la búsqueda | 57.0 M | 64.0 |
| inserts: exactas: comprobar las aristas inversas | 74.4 M | 83.5 |
| inserts: exactas: 2-means y medias de los splits | 21.7 M | 24.4 |
| inserts: exactas: altas y bajas de centroides en el grafo | 6.7 M | 7.5 |
| inserts: exactas: limpieza de lápidas | 13.2 M | 14.8 |

### Fases de un insert, µs

Indicativo: las dos celdas y el build de conteos corrieron a la vez.

| celda | asignación | centroide exacto | búsqueda de aristas | escritura | aristas inversas | split | total |
|---|---|---|---|---|---|---|---|
| 160, desde el 10 % | 101 | 1084 | 2653 | 18 | 62 | 27 | 4020 |
| 80, desde el 10 % | 99 | 1092 | 1741 | 18 | 61 | 27 | 3088 |
