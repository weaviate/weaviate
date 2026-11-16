### Calibración del umbral

| umbral | media del lote al 50 % | postings | tamaño medio final | p50 | p95 | máximo | splits (fallidos) | no-nearest | s |
|---|---|---|---|---|---|---|---|---|---|
| 280 | 235.9 | 5317 | 186.2 (fuera) | 187 | 269 | 280 | 3219 (0) | 0.0253 | 575 |
| 320 | 235.9 | 4649 | 212.9 | 214 | 307 | 320 | 2551 (0) | 0.0258 | 519 |
| 360 | 235.9 | 4170 | 237.4 | 237 | 345 | 360 | 2072 (0) | 0.0247 | 483 |
| 400 | 235.9 | 3744 | 264.4 | 265 | 384 | 400 | 1646 (0) | 0.0248 | 351 |
| 472 | 235.9 | 3176 | 311.7 (fuera) | 315 | 452 | 472 | 1078 (0) | 0.0244 | 286 |

### principal (umbral 360, ef de inserción 320, orden shuffled)

| checkpoint | vivos | postings | tamaño medio (p50 / p95 / máx) | al insertar: arista / posting | insertados ahora: arista / posting (grado) | lote ahora: arista / posting (grado) | cobertura | cargas a 0.95 | a 0.99 | ef para 0.99 | no-nearest | splits | µs por insert |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 50 % | 495000 | 2098 | 235.9 (231 / 383 / 700) | — | — | 0.9743 / 0.9613 (8.57) | complete | 8.7 | 38.7 | 320 | — | 0 | — |
| 60 % | 594000 | 2519 | 235.8 (239 / 342 / 360) | 0.9799 / 0.9701 | 0.8905 / 0.9096 (8.71) | 0.7809 / 0.8363 (8.57) | complete | 11.5 | 67.9 | 640 | 0.0256 | 421 | 3794 |
| 70 % | 693000 | 2930 | 236.5 (241 / 343 / 360) | 0.9749 / 0.9613 | 0.8199 / 0.8623 (9.11) | 0.6495 / 0.7496 (8.57) | complete | 13.3 | 82.7 | 640 | 0.0253 | 832 | 3761 |
| 80 % | 792000 | 3347 | 236.6 (238 / 345 / 360) | 0.9715 / 0.9569 | 0.7672 / 0.8271 (9.09) | 0.5515 / 0.6855 (8.57) | complete | 14.7 | 95.1 | 1024 | 0.0248 | 1249 | 3856 |
| 90 % | 891000 | 3730 | 238.9 (240 / 345 / 360) | 0.9697 / 0.9545 | 0.7284 / 0.7980 (9.18) | 0.4805 / 0.6330 (8.57) | complete | 16.6 | 111.3 | 1024 | 0.0247 | 1632 | 3952 |
| 100 % | 990000 | 4165 | 237.7 (238 / 344 / 360) | 0.9641 / 0.9465 | 0.6975 / 0.7773 (9.23) | 0.4245 / 0.5921 (8.57) | complete | 17.8 | 122.5 | 1024 | 0.0244 | 2067 | 4031 |

Fases por insert, µs (indicativo): asignación 83, centroide exacto 935, búsqueda de aristas 2970, escritura 16, inversas 0, split 22, reparación 0.

Al 100 %, frente a la referencia en lote (recall a igual ef):

| ef | referencia | principal | diferencia |
|---|---|---|---|
| 10 | 0.9229 | 0.8888 | -0.0341 |
| 20 | 0.9506 | 0.9199 | -0.0307 |
| 40 | 0.9664 | 0.9407 | -0.0257 |
| 80 | 0.9782 | 0.9580 | -0.0202 |
| 160 | 0.9858 | 0.9704 | -0.0155 |
| 320 | 0.9908 | 0.9798 | -0.0111 |
| 640 | 0.9940 | 0.9870 | -0.0070 |
| 1024 | 0.9960 | 0.9906 | -0.0054 |

Peor diferencia: -0.0341. Cargas a 0.95: 17.8 frente a 10.1 (+77.0 %); a 0.99: 122.5 frente a 52.8 (+132.2 %).


### control H1 (aristas inversas) (umbral 360, ef de inserción 320, orden shuffled)

| checkpoint | vivos | postings | tamaño medio (p50 / p95 / máx) | al insertar: arista / posting | insertados ahora: arista / posting (grado) | lote ahora: arista / posting (grado) | cobertura | cargas a 0.95 | a 0.99 | ef para 0.99 | no-nearest | splits | µs por insert |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 50 % | 495000 | 2098 | 235.9 (231 / 383 / 700) | — | — | 0.9743 / 0.9613 (8.57) | complete | 8.8 | 38.8 | 320 | — | 0 | — |
| 60 % | 594000 | 2518 | 235.9 (239 / 341 / 360) | 0.9867 / 0.9814 | 0.9297 / 0.9420 (8.75) | 0.8515 / 0.8876 (8.57) | complete | 9.4 | 39.1 | 320 | 0.0239 | 420 | 5325 |
| 70 % | 693000 | 2935 | 236.1 (240 / 342 / 360) | 0.9853 / 0.9789 | 0.8839 / 0.9100 (9.11) | 0.7793 / 0.8389 (8.57) | complete | 9.6 | 40.9 | 320 | 0.0246 | 837 | 6335 |
| 80 % | 792000 | 3338 | 237.3 (237 / 344 / 360) | 0.9848 / 0.9783 | 0.8588 / 0.8961 (9.13) | 0.7301 / 0.8103 (8.57) | complete | 10.0 | 45.9 | 320 | 0.0245 | 1240 | 6480 |
| 90 % | 891000 | 3737 | 238.4 (238 / 345 / 360) | 0.9861 / 0.9810 | 0.8459 / 0.8838 (9.22) | 0.6961 / 0.7855 (8.57) | complete | 10.0 | 48.4 | 320 | 0.0245 | 1639 | 7082 |
| 100 % | 990000 | 4168 | 237.5 (239 / 344 / 360) | 0.9846 / 0.9790 | 0.8245 / 0.8730 (9.32) | 0.6715 / 0.7673 (8.57) | complete | 10.3 | 49.8 | 320 | 0.0242 | 2070 | 7442 |

Fases por insert, µs (indicativo): asignación 140, centroide exacto 1821, búsqueda de aristas 5340, escritura 20, inversas 79, split 35, reparación 0.

Al 100 %, frente a la referencia en lote (recall a igual ef):

| ef | referencia | h1 | diferencia |
|---|---|---|---|
| 10 | 0.9229 | 0.9238 | +0.0008 |
| 20 | 0.9506 | 0.9528 | +0.0022 |
| 40 | 0.9664 | 0.9677 | +0.0013 |
| 80 | 0.9782 | 0.9789 | +0.0008 |
| 160 | 0.9858 | 0.9867 | +0.0009 |
| 320 | 0.9908 | 0.9919 | +0.0011 |
| 640 | 0.9940 | 0.9952 | +0.0012 |
| 1024 | 0.9960 | 0.9966 | +0.0007 |

Peor diferencia: +0.0007. Cargas a 0.95: 10.3 frente a 10.1 (+2.3 %); a 0.99: 49.8 frente a 52.8 (-5.7 %).


### control H2 (reparación) (umbral 360, ef de inserción 320, orden shuffled)

| checkpoint | vivos | postings | tamaño medio (p50 / p95 / máx) | al insertar: arista / posting | insertados ahora: arista / posting (grado) | lote ahora: arista / posting (grado) | cobertura | cargas a 0.95 | a 0.99 | ef para 0.99 | no-nearest | splits | µs por insert |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 50 % | 495000 | 2098 | 235.9 (231 / 383 / 700) | — | — | 0.9743 / 0.9613 (8.57) | complete | 8.8 | 38.8 | 320 | — | 0 | — |
| 60 % | 594000 | 2522 | 235.5 (237 / 342 / 360) | 0.9809 / 0.9720 | 0.9073 / 0.9239 (8.82) | 0.8383 / 0.8789 (9.03) | complete | 10.9 | 61.5 | 640 | 0.0240 | 424 | 12063 |
| 70 % | 693000 | 2934 | 236.2 (238 / 343 / 360) | 0.9778 / 0.9664 | 0.8607 / 0.8921 (9.34) | 0.7673 / 0.8366 (9.29) | complete | 11.7 | 69.2 | 640 | 0.0231 | 836 | 13206 |
| 80 % | 792000 | 3332 | 237.7 (239 / 343 / 360) | 0.9758 / 0.9645 | 0.8390 / 0.8804 (9.46) | 0.7223 / 0.8112 (9.45) | complete | 12.5 | 75.4 | 640 | 0.0229 | 1234 | 14319 |
| 90 % | 891000 | 3718 | 239.6 (242 / 345 / 360) | 0.9759 / 0.9645 | 0.8175 / 0.8614 (9.55) | 0.6919 / 0.7938 (9.58) | complete | 12.9 | 81.5 | 640 | 0.0232 | 1620 | 13207 |
| 100 % | 990000 | 4142 | 239.0 (241 / 344 / 360) | 0.9730 / 0.9599 | 0.8037 / 0.8635 (9.81) | 0.6825 / 0.7848 (9.73) | complete | 13.1 | 91.8 | 640 | 0.0235 | 2044 | 12259 |

Fases por insert, µs (indicativo): asignación 122, centroide exacto 1422, búsqueda de aristas 4510, escritura 19, inversas 0, split 30, reparación 6150.

Al 100 %, frente a la referencia en lote (recall a igual ef):

| ef | referencia | h2 | diferencia |
|---|---|---|---|
| 10 | 0.9229 | 0.9094 | -0.0135 |
| 20 | 0.9506 | 0.9374 | -0.0132 |
| 40 | 0.9664 | 0.9559 | -0.0105 |
| 80 | 0.9782 | 0.9689 | -0.0093 |
| 160 | 0.9858 | 0.9784 | -0.0074 |
| 320 | 0.9908 | 0.9848 | -0.0060 |
| 640 | 0.9940 | 0.9903 | -0.0037 |
| 1024 | 0.9960 | 0.9928 | -0.0031 |

Peor diferencia: -0.0135. Cargas a 0.95: 13.1 frente a 10.1 (+29.9 %); a 0.99: 91.8 frente a 52.8 (+73.8 %).


### principal, orden natural (umbral 360, ef de inserción 320, orden natural)

| checkpoint | vivos | postings | tamaño medio (p50 / p95 / máx) | al insertar: arista / posting | insertados ahora: arista / posting (grado) | lote ahora: arista / posting (grado) | cobertura | cargas a 0.95 | a 0.99 | ef para 0.99 | no-nearest | splits | µs por insert |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 50 % | 495000 | 2098 | 235.9 (232 / 375 / 626) | — | — | 0.9744 / 0.9613 (8.51) | complete | 8.6 | 34.4 | 320 | — | 0 | — |
| 60 % | 594000 | 2546 | 233.3 (236 / 345 / 360) | 0.9809 / 0.9714 | 0.8893 / 0.9109 (8.89) | 0.7704 / 0.8273 (8.51) | complete | 11.1 | 65.7 | 640 | 0.0252 | 448 | 4772 |
| 70 % | 693000 | 2954 | 234.6 (237 / 341 / 360) | 0.9743 / 0.9627 | 0.8138 / 0.8614 (8.78) | 0.6404 / 0.7398 (8.51) | complete | 13.2 | 84.3 | 640 | 0.0245 | 856 | 5338 |
| 80 % | 792000 | 3329 | 237.9 (239 / 345 / 360) | 0.9715 / 0.9568 | 0.7651 / 0.8152 (8.95) | 0.5480 / 0.6771 (8.51) | complete | 14.6 | 95.2 | 1024 | 0.0238 | 1231 | 5700 |
| 90 % | 891000 | 3729 | 238.9 (240 / 345 / 360) | 0.9675 / 0.9522 | 0.7229 / 0.7957 (9.09) | 0.4725 / 0.6240 (8.51) | complete | 16.3 | 108.2 | 1024 | 0.0235 | 1631 | 6300 |
| 100 % | 990000 | 4155 | 238.3 (241 / 344 / 360) | 0.9653 / 0.9481 | 0.6876 / 0.7671 (9.15) | 0.4199 / 0.5860 (8.51) | complete | 18.1 | 123.6 | 1024 | 0.0233 | 2057 | 6733 |

Fases por insert, µs (indicativo): asignación 146, centroide exacto 1838, búsqueda de aristas 4687, escritura 21, inversas 0, split 35, reparación 0.

Al 100 %, frente a la referencia en lote (recall a igual ef):

| ef | referencia | natural | diferencia |
|---|---|---|---|
| 10 | 0.9229 | 0.8870 | -0.0360 |
| 20 | 0.9506 | 0.9186 | -0.0320 |
| 40 | 0.9664 | 0.9402 | -0.0262 |
| 80 | 0.9782 | 0.9570 | -0.0211 |
| 160 | 0.9858 | 0.9696 | -0.0163 |
| 320 | 0.9908 | 0.9795 | -0.0113 |
| 640 | 0.9940 | 0.9868 | -0.0072 |
| 1024 | 0.9960 | 0.9905 | -0.0055 |

Peor diferencia: -0.0360. Cargas a 0.95: 18.1 frente a 10.1 (+80.3 %); a 0.99: 123.6 frente a 52.8 (+134.1 %).


### celda 5 (reparación por hermanos) (umbral 360, ef de inserción 320, orden shuffled)

| checkpoint | vivos | postings | tamaño medio (p50 / p95 / máx) | al insertar: arista / posting | insertados ahora: arista / posting (grado) | lote ahora: arista / posting (grado) | cobertura | cargas a 0.95 | a 0.99 | ef para 0.99 | no-nearest | splits | µs por insert |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 50 % | 495000 | 2098 | 235.9 (231 / 383 / 700) | — | — | 0.9743 / 0.9613 (8.57) | complete | 8.8 | 38.8 | 320 | — | 0 | — |
| 60 % | 594000 | 2518 | 235.9 (239 / 342 / 360) | 0.9797 / 0.9699 | 0.9036 / 0.9200 (9.67) | 0.8222 / 0.8656 (11.15) | complete | 11.4 | 66.0 | 640 | 0.0244 | 420 | 3273 |
| 70 % | 693000 | 2929 | 236.6 (241 / 343 / 360) | 0.9757 / 0.9627 | 0.8414 / 0.8810 (10.75) | 0.7120 / 0.7967 (12.85) | complete | 12.9 | 79.1 | 640 | 0.0243 | 831 | 3428 |
| 80 % | 792000 | 3330 | 237.8 (239 / 346 / 360) | 0.9729 / 0.9593 | 0.8024 / 0.8524 (11.51) | 0.6272 / 0.7474 (13.86) | complete | 14.0 | 92.0 | 1024 | 0.0237 | 1232 | 3593 |
| 90 % | 891000 | 3735 | 238.6 (240 / 346 / 360) | 0.9709 / 0.9563 | 0.7660 / 0.8299 (11.80) | 0.5600 / 0.7059 (14.58) | complete | 16.0 | 109.7 | 1024 | 0.0239 | 1637 | 3807 |
| 100 % | 990000 | 4159 | 238.0 (239 / 345 / 360) | 0.9661 / 0.9498 | 0.7367 / 0.8165 (12.39) | 0.5106 / 0.6764 (15.00) | complete | 17.1 | 118.2 | 1024 | 0.0240 | 2061 | 3958 |

Fases por insert, µs (indicativo): asignación 74, centroide exacto 874, búsqueda de aristas 2940, escritura 15, inversas 0, split 21, reparación 0.

Al 100 %, frente a la referencia en lote (recall a igual ef):

| ef | referencia | siblings | diferencia |
|---|---|---|---|
| 10 | 0.9229 | 0.8945 | -0.0285 |
| 20 | 0.9506 | 0.9247 | -0.0259 |
| 40 | 0.9664 | 0.9430 | -0.0234 |
| 80 | 0.9782 | 0.9594 | -0.0188 |
| 160 | 0.9858 | 0.9721 | -0.0137 |
| 320 | 0.9908 | 0.9809 | -0.0099 |
| 640 | 0.9940 | 0.9873 | -0.0067 |
| 1024 | 0.9960 | 0.9911 | -0.0049 |

Peor diferencia: -0.0285. Cargas a 0.95: 17.1 frente a 10.1 (+69.5 %); a 0.99: 118.2 frente a 52.8 (+123.9 %).


### celda 6 (aristas inversas y reparación por hermanos) (umbral 360, ef de inserción 320, orden shuffled)

| checkpoint | vivos | postings | tamaño medio (p50 / p95 / máx) | al insertar: arista / posting | insertados ahora: arista / posting (grado) | lote ahora: arista / posting (grado) | cobertura | cargas a 0.95 | a 0.99 | ef para 0.99 | no-nearest | splits | µs por insert |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 50 % | 495000 | 2098 | 235.9 (231 / 383 / 700) | — | — | 0.9743 / 0.9613 (8.57) | complete | 8.8 | 38.8 | 320 | — | 0 | — |
| 60 % | 594000 | 2513 | 236.4 (239 / 342 / 360) | 0.9869 / 0.9820 | 0.9428 / 0.9528 (9.62) | 0.8996 / 0.9162 (11.15) | complete | 9.1 | 37.6 | 320 | 0.0251 | 415 | 3474 |
| 70 % | 693000 | 2929 | 236.6 (239 / 343 / 360) | 0.9863 / 0.9799 | 0.9110 / 0.9306 (10.75) | 0.8511 / 0.8845 (12.86) | complete | 9.3 | 38.3 | 320 | 0.0250 | 831 | 3656 |
| 80 % | 792000 | 3340 | 237.1 (238 / 345 / 360) | 0.9863 / 0.9813 | 0.9004 / 0.9244 (11.52) | 0.8204 / 0.8689 (13.89) | complete | 9.5 | 41.9 | 320 | 0.0246 | 1242 | 3869 |
| 90 % | 891000 | 3733 | 238.7 (240 / 345 / 360) | 0.9871 / 0.9820 | 0.8837 / 0.9156 (11.78) | 0.7954 / 0.8538 (14.59) | complete | 9.6 | 43.0 | 320 | 0.0246 | 1635 | 4070 |
| 100 % | 990000 | 4149 | 238.6 (241 / 345 / 360) | 0.9867 / 0.9813 | 0.8707 / 0.9084 (12.43) | 0.7816 / 0.8460 (14.99) | complete | 9.6 | 43.6 | 320 | 0.0246 | 2051 | 4270 |

Fases por insert, µs (indicativo): asignación 74, centroide exacto 883, búsqueda de aristas 3176, escritura 15, inversas 68, split 21, reparación 0.

Al 100 %, frente a la referencia en lote (recall a igual ef):

| ef | referencia | inverse-siblings | diferencia |
|---|---|---|---|
| 10 | 0.9229 | 0.9304 | +0.0075 |
| 20 | 0.9506 | 0.9578 | +0.0072 |
| 40 | 0.9664 | 0.9712 | +0.0048 |
| 80 | 0.9782 | 0.9820 | +0.0038 |
| 160 | 0.9858 | 0.9883 | +0.0025 |
| 320 | 0.9908 | 0.9932 | +0.0024 |
| 640 | 0.9940 | 0.9957 | +0.0017 |
| 1024 | 0.9960 | 0.9969 | +0.0009 |

Peor diferencia: +0.0009. Cargas a 0.95: 9.6 frente a 10.1 (-4.1 %); a 0.99: 43.6 frente a 52.8 (-17.5 %).


### H1, repetición con el histograma de escrituras (umbral 360, ef de inserción 320, orden shuffled)

| checkpoint | vivos | postings | tamaño medio (p50 / p95 / máx) | al insertar: arista / posting | insertados ahora: arista / posting (grado) | lote ahora: arista / posting (grado) | cobertura | cargas a 0.95 | a 0.99 | ef para 0.99 | no-nearest | splits | µs por insert |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 50 % | 495000 | 2098 | 235.9 (231 / 383 / 700) | — | — | 0.9743 / 0.9613 (8.57) | complete | 8.8 | 38.8 | 320 | — | 0 | — |
| 100 % | 990000 | 4151 | 238.5 (240 / 345 / 360) | 0.9849 / 0.9785 | 0.8264 / 0.8725 (9.29) | 0.6700 / 0.7670 (8.57) | complete | 10.2 | 48.3 | 320 | 0.0248 | 2053 | 3929 |

Fases por insert, µs (indicativo): asignación 71, centroide exacto 866, búsqueda de aristas 2901, escritura 15, inversas 51, split 21, reparación 0.

Al 100 %, frente a la referencia en lote (recall a igual ef):

| ef | referencia | h1-writes | diferencia |
|---|---|---|---|
| 10 | 0.9229 | 0.9237 | +0.0008 |
| 20 | 0.9506 | 0.9526 | +0.0020 |
| 40 | 0.9664 | 0.9686 | +0.0022 |
| 80 | 0.9782 | 0.9796 | +0.0014 |
| 160 | 0.9858 | 0.9871 | +0.0013 |
| 320 | 0.9908 | 0.9922 | +0.0013 |
| 640 | 0.9940 | 0.9954 | +0.0013 |
| 1024 | 0.9960 | 0.9966 | +0.0006 |

Peor diferencia: +0.0006. Cargas a 0.95: 10.2 frente a 10.1 (+1.8 %); a 0.99: 48.3 frente a 52.8 (-8.5 %).


### Frontera cronometrada, mismo proceso (QPS a igual recall, primera ronda; entre paréntesis, la segunda)

| recall | threads | referencia | referencia por docID | principal | principal / referencia | por docID / referencia |
|---|---|---|---|---|---|---|
| 0.95 | 1 | 1859 (1770) | 1551 (1735) | 990 (1002) | 0.533x | 0.834x |
| 0.95 | 8 | 12295 (12174) | 10389 (10372) | 5605 (6098) | 0.456x | 0.845x |
| 0.99 | 1 | 420 (411) | 382 (381) | 169 (170) | 0.402x | 0.910x |
| 0.99 | 8 | 2690 (2678) | 2273 (2266) | 883 (977) | 0.328x | 0.845x |

| curva | threads | deriva entre rondas: mediana | mínimo | máximo |
|---|---|---|---|---|
| reference | 1 | -0.9 % | -4.9 % | +0.5 % |
| reference | 8 | -0.5 % | -2.2 % | +0.8 % |
| reference-docid | 1 | -0.3 % | -1.8 % | +12.2 % |
| reference-docid | 8 | -0.2 % | -9.2 % | +4.5 % |
| cell | 1 | +0.1 % | -5.5 % | +3.7 % |
| cell | 8 | +9.1 % | +6.7 % | +18.3 % |

### Veredicto

- **Celda principal al 100 %:** peor diferencia de recall a igual ef -0.0341 (umbral −0.005); cargas a 0.95 +77.0 % (umbral +15 %).
- **H1 y H2 no se sostienen.**
- **control H1 (aristas inversas):** peor diferencia +0.0007 (principal -0.0341), cargas a 0.95 +2.3 % (principal +77.0 %).
- **control H2 (reparación):** peor diferencia -0.0135 (principal -0.0341), cargas a 0.95 +29.9 % (principal +77.0 %).
- **celda 5 (reparación por hermanos):** peor diferencia -0.0285 (principal -0.0341), cargas a 0.95 +69.5 % (principal +77.0 %).
- **celda 6 (aristas inversas y reparación por hermanos):** peor diferencia +0.0009 (principal -0.0341), cargas a 0.95 -4.1 % (principal +77.0 %).
- **La celda 4, «las dos», no corrió.**

### Suelo de ruido entre corridas

El checkpoint del 50 % es el mismo build en lote en 6 corridas (principal, h1, h2, siblings, inverse-siblings, h1-writes).

| ef | recall al 50 %: mínimo | máximo | diferencia | cargas: diferencia relativa | H1 frente a su repetición al 100 % |
|---|---|---|---|---|---|
| 10 | 0.9326 | 0.9335 | 0.0009 | 0.37 % | 0.0001 |
| 20 | 0.9578 | 0.9585 | 0.0007 | 0.15 % | 0.0002 |
| 40 | 0.9719 | 0.9722 | 0.0003 | 0.12 % | 0.0009 |
| 80 | 0.9820 | 0.9823 | 0.0004 | 0.13 % | 0.0007 |
| 160 | 0.9884 | 0.9884 | 0.0000 | 0.03 % | 0.0004 |
| 320 | 0.9930 | 0.9930 | 0.0000 | 0.02 % | 0.0003 |
| 640 | 0.9961 | 0.9961 | 0.0000 | 0.01 % | 0.0002 |
| 1024 | 0.9972 | 0.9972 | 0.0000 | 0.00 % | 0.0001 |

Cargas a 0.95 de H1 al 100 %: 10.29 y 10.24 en las dos corridas (0.5 % de diferencia).

### Trabajo a igual recall y grado medio, al 100 %

| celda | cargas a 0.95 | endpoints puntuados a 0.95 | cargas a 0.99 | endpoints puntuados a 0.99 | grado medio: insertados | lote |
|---|---|---|---|---|---|---|
| referencia en lote | 10.1 | 4953 | 52.8 | 26634 | — | — |
| principal | 17.8 | 8951 | 122.5 | 55312 | 9.23 | 8.57 |
| control H1 (aristas inversas) | 10.3 | 5090 | 49.8 | 26183 | 9.32 | 8.57 |
| control H2 (reparación) | 13.1 | 6723 | 91.8 | 45269 | 9.81 | 9.73 |
| principal, orden natural | 18.1 | 9098 | 123.6 | 55847 | 9.15 | 8.51 |
| celda 5 (reparación por hermanos) | 17.1 | 9115 | 118.2 | 56643 | 12.39 | 15.00 |
| celda 6 (aristas inversas y reparación por hermanos) | 9.6 | 5337 | 43.6 | 25607 | 12.43 | 14.99 |
| H1, repetición con el histograma de escrituras | 10.2 | 5090 | 48.3 | 25549 | 9.29 | 8.57 |

### Escrituras de aristas inversas por insert

Cada arista inversa es una escritura a la lista de otro vector. «Postings» cuenta los postings distintos donde viven esas listas: lo que escribe un insert si las aristas se guardan con su posting.

| celda | tramo | inserts | listas con aristas leídas por insert | distancias exactas por insert | escrituras: media | p50 | p95 | máximo | inserts sin ninguna | postings distintos: media | p50 | p95 | máximo | inserts sin ninguno |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| control H1 (aristas inversas) | hasta el 100 % | 495000 | — | — | 4.62 | — | — | — | — | — | — | — | — | — |
| control H1 (aristas inversas) | 50 a 60 % | 99000 | — | — | 4.38 | — | — | — | — | — | — | — | — | — |
| control H1 (aristas inversas) | 60 a 70 % | 99000 | — | — | 4.54 | — | — | — | — | — | — | — | — | — |
| control H1 (aristas inversas) | 70 a 80 % | 99000 | — | — | 4.64 | — | — | — | — | — | — | — | — | — |
| control H1 (aristas inversas) | 80 a 90 % | 99000 | — | — | 4.74 | — | — | — | — | — | — | — | — | — |
| control H1 (aristas inversas) | 90 a 100 % | 99000 | — | — | 4.79 | — | — | — | — | — | — | — | — | — |
| celda 6 (aristas inversas y reparación por hermanos) | hasta el 100 % | 495000 | 9.13 | 118.0 | 5.73 | 5 | 12 | 16 | 7.5 % | 3.83 | 3 | 9 | 16 | 7.5 % |
| celda 6 (aristas inversas y reparación por hermanos) | 50 a 60 % | 99000 | 8.81 | 100.1 | 4.97 | 5 | 11 | 16 | 9.7 % | 3.35 | 3 | 8 | 16 | 9.7 % |
| celda 6 (aristas inversas y reparación por hermanos) | 60 a 70 % | 99000 | 8.98 | 111.0 | 5.49 | 5 | 12 | 16 | 8.1 % | 3.67 | 3 | 8 | 16 | 8.1 % |
| celda 6 (aristas inversas y reparación por hermanos) | 70 a 80 % | 99000 | 9.17 | 120.7 | 5.87 | 6 | 12 | 16 | 7.0 % | 3.91 | 4 | 9 | 16 | 7.0 % |
| celda 6 (aristas inversas y reparación por hermanos) | 80 a 90 % | 99000 | 9.30 | 127.0 | 6.11 | 6 | 13 | 16 | 6.4 % | 4.06 | 4 | 9 | 16 | 6.4 % |
| celda 6 (aristas inversas y reparación por hermanos) | 90 a 100 % | 99000 | 9.39 | 131.0 | 6.20 | 6 | 13 | 16 | 6.1 % | 4.15 | 4 | 9 | 16 | 6.1 % |
| H1, repetición con el histograma de escrituras | hasta el 100 % | 495000 | 8.97 | 89.9 | 4.62 | 4 | 11 | 16 | 10.5 % | 3.28 | 3 | 8 | 16 | 10.5 % |

La repetición de H1 frente a la corrida original, al 100 %: 4.619 escrituras por insert frente a 4.619, 2053 splits frente a 2070.

### Celda 5 frente a H2, al 100 %

| ef | H2 | celda 5 | diferencia |
|---|---|---|---|
| 10 | 0.9094 | 0.8945 | -0.0150 |
| 20 | 0.9374 | 0.9247 | -0.0127 |
| 40 | 0.9559 | 0.9430 | -0.0129 |
| 80 | 0.9689 | 0.9594 | -0.0095 |
| 160 | 0.9784 | 0.9721 | -0.0063 |
| 320 | 0.9848 | 0.9809 | -0.0039 |
| 640 | 0.9903 | 0.9873 | -0.0030 |
| 1024 | 0.9928 | 0.9911 | -0.0018 |

- **Recall:** peor diferencia frente a H2 -0.0150 (umbral −0.001). **Cargas a 0.95:** 17.1 frente a 13.1 (+30.5 %; umbral +2 %).
- **Coste por split, µs (indicativo; H2 corrió en paralelo con otras dos celdas, la 5 sola):** celda 5 12186, H2 1496645.
- **Trabajo por split, sin depender de la máquina:** celda 5, 34820 distancias exactas; H2, 365.5 búsquedas.
- **Celda 5 no iguala a H2 sin búsqueda: no es la candidata.**
- **Frente al suelo de ruido:** ef 10: -0.0150, con un suelo de 0.0009; ef 20: -0.0127, con un suelo de 0.0007; ef 40: -0.0129, con un suelo de 0.0009; ef 80: -0.0095, con un suelo de 0.0007; ef 160: -0.0063, con un suelo de 0.0004; ef 320: -0.0039, con un suelo de 0.0003; ef 640: -0.0030, con un suelo de 0.0002; ef 1024: -0.0018, con un suelo de 0.0001.

### Celda 6 frente a H1, al 100 %

| ef | H1 | H1, repetición | celda 6 | diferencia frente a H1 | frente a la repetición | suelo de ruido |
|---|---|---|---|---|---|---|
| 10 | 0.9238 | 0.9237 | 0.9304 | +0.0067 | +0.0067 | 0.0009 |
| 20 | 0.9528 | 0.9526 | 0.9578 | +0.0051 | +0.0053 | 0.0007 |
| 40 | 0.9677 | 0.9686 | 0.9712 | +0.0035 | +0.0026 | 0.0009 |
| 80 | 0.9789 | 0.9796 | 0.9820 | +0.0031 | +0.0024 | 0.0007 |
| 160 | 0.9867 | 0.9871 | 0.9883 | +0.0016 | +0.0013 | 0.0004 |
| 320 | 0.9919 | 0.9922 | 0.9932 | +0.0013 | +0.0011 | 0.0003 |
| 640 | 0.9952 | 0.9954 | 0.9957 | +0.0005 | +0.0004 | 0.0002 |
| 1024 | 0.9966 | 0.9966 | 0.9969 | +0.0002 | +0.0003 | 0.0001 |

- **Recall:** mejor diferencia frente a H1 +0.0067, peor +0.0002 (umbral ±0.001). **Cargas a 0.95:** 9.6 frente a 10.3 (-6.2 %; umbral ∓2 %).
- **Celda 6 frente a H1: los hermanos aportan sobre las aristas inversas.**
- **Frente al suelo de ruido:** ef 10: +0.0067, con un suelo de 0.0009; ef 20: +0.0051, con un suelo de 0.0007; ef 40: +0.0035, con un suelo de 0.0009; ef 80: +0.0031, con un suelo de 0.0007; ef 160: +0.0016, con un suelo de 0.0004; ef 320: +0.0013, con un suelo de 0.0003.
- **Cargas frente al suelo de ruido:** -6.2 %, con 0.5 % entre las dos corridas de H1.
- **Trabajo por split, sin depender de la máquina:** 34762 distancias exactas, 344.4 listas de aristas que cambian y 2628.1 aristas hacia la mitad hermana. Repartido entre los inserts: 1.43 listas cambiadas por insert.
- **Trabajo por insert, sin depender de la máquina:** una búsqueda (k=16, ef=320); 9.13 listas con aristas leídas y 5.73 escrituras de aristas inversas (H1: 4.62 escrituras).
- **µs, indicativos (H1 corrió en paralelo con otras dos celdas, la 6 sola):** por insert, celda 6 4270 y H1 7442; por split con su reparación, celda 6 12254.
