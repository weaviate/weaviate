### La corrida

- **Dataset:** dbpedia-openai-1000k-angular.bin, 990 000 vectores, 5 000 queries, k = 10. Malla de ef: 10, 20, 40, 80, 160, 320, 640, 1 024.
- **Índice:** S = 236, P = 256, R = 64, split a T = 360, ef de inserción 160. Limpieza: se reescribe por encima del 30 % de borrados y se fusiona por debajo de 118 vivos.
- **El lote del 100 %:** 4 195 postings de 236.0 miembros de media.
- **Ningún stop:** ningún vector borrado devuelto y cobertura completa en todo lo medido.

### El criterio: con el 30 % borrado al azar

Los deletes se sostienen si alguna celda queda a 0.005 de recall o menos de su referencia a igual ef, con cargas a 0.95 un 15 % o menos por encima y a 0.99 un 10 % o menos.

- **Corpus:** 990 000 vectores, 297 000 borrados. Quedan 693 000.
- **Referencia, el lote de los que quedan:** 2 937 postings de 236.0 miembros. Cargas a 0.95: 9.28. A 0.99: 44.46, entre ef 160 y 320.

| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | cruza 0.99 entre ef | las tres |
|---|---|---|---|---|---|---|---|---|
| 1, borrado lógico | −0.0148 en ef 10 | no | 13.70, +47.6 % | no | 74.32, +67.2 % | no | 320 y 640 | no |
| 2, endpoints borrados sin puntuar | −0.0227 en ef 10 | no | 13.48, +45.2 % | no | 74.75, +68.1 % | no | 320 y 640 | no |
| 3, más limpieza física | −0.0214 en ef 10 | no | 12.73, +37.1 % | no | 66.99, +50.7 % | no | 320 y 640 | no |
| 4, más refresco | −0.0173 en ef 10 | no | 11.95, +28.7 % | no | 60.62, +36.3 % | no | 320 y 640 | no |

Recall y cargas a igual ef:

| ef | referencia: recall | cargas | celda 1: recall | cargas | celda 2: recall | cargas | celda 3: recall | cargas | celda 4: recall | cargas |
|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 0.9296 | 7.40 | 0.9148 | 8.73 | 0.9069 | 7.82 | 0.9082 | 7.67 | 0.9123 | 7.68 |
| 20 | 0.9560 | 9.92 | 0.9437 | 12.01 | 0.9363 | 10.47 | 0.9379 | 10.25 | 0.9420 | 10.27 |
| 40 | 0.9711 | 14.26 | 0.9624 | 17.72 | 0.9562 | 15.11 | 0.9580 | 14.71 | 0.9611 | 14.71 |
| 80 | 0.9799 | 21.50 | 0.9753 | 27.31 | 0.9711 | 22.85 | 0.9727 | 22.22 | 0.9747 | 22.33 |
| 160 | 0.9872 | 34.11 | 0.9840 | 43.75 | 0.9807 | 36.56 | 0.9823 | 35.22 | 0.9836 | 35.44 |
| 320 | 0.9923 | 55.56 | 0.9898 | 72.19 | 0.9881 | 60.25 | 0.9886 | 57.49 | 0.9896 | 57.87 |
| 640 | 0.9953 | 92.89 | 0.9940 | 121.01 | 0.9927 | 101.44 | 0.9933 | 96.28 | 0.9939 | 97.02 |
| 1024 | 0.9968 | 133.03 | 0.9960 | 173.84 | 0.9949 | 146.32 | 0.9953 | 138.07 | 0.9957 | 139.32 |

| celda | postings | miembros vivos por posting | miembros que ocupan sitio | bajo el mínimo | vacíos | aristas muertas | vectores con la mitad o más |
|---|---|---|---|---|---|---|---|
| 1, borrado lógico | 4 195 | 165.2 | 236.0 | 880 | 0 | 0.300 | 99 641 |
| 2, endpoints borrados sin puntuar | 4 195 | 165.2 | 236.0 | 880 | 0 | 0.300 | 99 641 |
| 3, más limpieza física | 3 513 | 197.3 | 230.2 | 57 | 0 | 0.300 | 99 641 |
| 4, más refresco | 3 513 | 197.3 | 230.2 | 57 | 0 | 0.238 | 0 |

- **Limpieza:** 2 076 postings reescritos, con 331 305 miembros vivos copiados; 181 165 borrados soltados; 743 fusiones, que movieron 63 045 miembros vivos; 0 postings retirados sin ningún vivo; 61 splits tras una fusión.
- **Refresco:** 99 641 vectores, el 14.4 % de los vivos. Por vector: 30.9 cargas, 10 329 endpoints puntuados y 64.0 distancias exactas. Entraron 6 746 aristas inversas.

**Ninguna celda cumple el criterio: por la regla como está escrita, los deletes no se sostienen con estas cuatro celdas.**

### El 10 % al azar

- **Corpus:** 990 000 vectores, 99 000 borrados. Quedan 891 000.
- **Referencia, el lote de los que quedan:** 3 776 postings de 236.0 miembros. Cargas a 0.95: 9.72. A 0.99: 51.97, entre ef 160 y 320.

| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | cruza 0.99 entre ef | las tres |
|---|---|---|---|---|---|---|---|---|
| 1, borrado lógico | −0.0066 en ef 10 | no | 10.92, +12.4 % | sí | 58.32, +12.2 % | no, a menos del suelo de ruido | 160 y 320 | no |
| 2, endpoints borrados sin puntuar | −0.0083 en ef 10 | no | 11.03, +13.5 % | sí | 60.15, +15.7 % | no | 320 y 640 | no |
| 3, más limpieza física | −0.0076 en ef 10 | no | 10.76, +10.7 % | sí | 58.08, +11.7 % | no, a menos del suelo de ruido | 320 y 640 | no |
| 4, más refresco | −0.0074 en ef 10 | no | 10.73, +10.4 % | sí | 57.93, +11.5 % | no, a menos del suelo de ruido | 320 y 640 | no |

Recall y cargas a igual ef:

| ef | referencia: recall | cargas | celda 1: recall | cargas | celda 2: recall | cargas | celda 3: recall | cargas | celda 4: recall | cargas |
|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 0.9264 | 7.52 | 0.9198 | 7.88 | 0.9181 | 7.61 | 0.9188 | 7.59 | 0.9190 | 7.59 |
| 20 | 0.9536 | 10.10 | 0.9491 | 10.70 | 0.9466 | 10.25 | 0.9478 | 10.25 | 0.9479 | 10.24 |
| 40 | 0.9682 | 14.47 | 0.9654 | 15.48 | 0.9633 | 14.76 | 0.9639 | 14.69 | 0.9640 | 14.69 |
| 80 | 0.9781 | 21.97 | 0.9764 | 23.64 | 0.9747 | 22.35 | 0.9753 | 22.22 | 0.9754 | 22.22 |
| 160 | 0.9858 | 34.80 | 0.9851 | 37.55 | 0.9835 | 35.59 | 0.9842 | 35.26 | 0.9843 | 35.27 |
| 320 | 0.9909 | 56.75 | 0.9906 | 61.34 | 0.9897 | 58.13 | 0.9899 | 57.45 | 0.9899 | 57.46 |
| 640 | 0.9944 | 94.75 | 0.9938 | 102.41 | 0.9937 | 97.25 | 0.9937 | 95.66 | 0.9937 | 95.68 |
| 1024 | 0.9962 | 135.73 | 0.9958 | 146.57 | 0.9956 | 139.24 | 0.9957 | 136.68 | 0.9957 | 136.73 |

| celda | postings | miembros vivos por posting | miembros que ocupan sitio | bajo el mínimo | vacíos | aristas muertas | vectores con la mitad o más |
|---|---|---|---|---|---|---|---|
| 1, borrado lógico | 4 195 | 212.4 | 236.0 | 430 | 0 | 0.100 | 14 616 |
| 2, endpoints borrados sin puntuar | 4 195 | 212.4 | 236.0 | 430 | 0 | 0.100 | 14 616 |
| 3, más limpieza física | 3 895 | 228.8 | 252.6 | 36 | 0 | 0.100 | 14 616 |
| 4, más refresco | 3 895 | 228.8 | 252.6 | 36 | 0 | 0.097 | 0 |

- **Limpieza:** 1 postings reescritos, con 3 miembros vivos copiados; 6 134 borrados soltados; 364 fusiones, que movieron 28 778 miembros vivos; 0 postings retirados sin ningún vivo; 64 splits tras una fusión.
- **Refresco:** 14 616 vectores, el 1.6 % de los vivos. Por vector: 22.0 cargas, 8 033 endpoints puntuados y 64.0 distancias exactas. Entraron 579 aristas inversas.

### El 50 % al azar

- **Corpus:** 990 000 vectores, 495 000 borrados. Quedan 495 000.
- **Referencia, el lote de los que quedan:** 2 098 postings de 235.9 miembros. Cargas a 0.95: 8.73. A 0.99: 36.25, entre ef 160 y 320.

| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | cruza 0.99 entre ef | las tres |
|---|---|---|---|---|---|---|---|---|
| 1, borrado lógico | −0.0263 en ef 10 | no | 18.14, +107.9 % | no | 103.74, +186.2 % | no | 320 y 640 | no |
| 2, endpoints borrados sin puntuar | −0.0475 en ef 10 | no | 18.63, +113.5 % | no | 106.96, +195.1 % | no | 320 y 640 | no |
| 3, más limpieza física | −0.0342 en ef 10 | no | 14.19, +62.6 % | no | 79.61, +119.6 % | no | 320 y 640 | no |
| 4, más refresco | −0.0133 en ef 10 | no | 10.37, +18.8 % | no | 51.32, +41.6 % | no | 160 y 320 | no |

Recall y cargas a igual ef:

| ef | referencia: recall | cargas | celda 1: recall | cargas | celda 2: recall | cargas | celda 3: recall | cargas | celda 4: recall | cargas |
|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 0.9323 | 7.21 | 0.9060 | 9.99 | 0.8848 | 8.03 | 0.8981 | 7.53 | 0.9190 | 7.51 |
| 20 | 0.9594 | 9.66 | 0.9372 | 14.15 | 0.9186 | 10.81 | 0.9291 | 10.07 | 0.9486 | 10.06 |
| 40 | 0.9733 | 13.77 | 0.9577 | 21.05 | 0.9423 | 15.51 | 0.9495 | 14.02 | 0.9650 | 14.31 |
| 80 | 0.9827 | 20.76 | 0.9711 | 32.95 | 0.9597 | 23.50 | 0.9652 | 20.89 | 0.9771 | 21.67 |
| 160 | 0.9891 | 32.69 | 0.9806 | 53.35 | 0.9730 | 37.69 | 0.9766 | 32.81 | 0.9856 | 34.52 |
| 320 | 0.9933 | 53.43 | 0.9885 | 88.76 | 0.9839 | 62.78 | 0.9855 | 53.74 | 0.9911 | 56.82 |
| 640 | 0.9962 | 89.70 | 0.9937 | 150.29 | 0.9900 | 106.96 | 0.9914 | 89.96 | 0.9946 | 95.46 |
| 1024 | 0.9972 | 128.79 | 0.9951 | 217.36 | 0.9932 | 155.92 | 0.9942 | 129.83 | 0.9963 | 137.94 |

| celda | postings | miembros vivos por posting | miembros que ocupan sitio | bajo el mínimo | vacíos | aristas muertas | vectores con la mitad o más |
|---|---|---|---|---|---|---|---|
| 1, borrado lógico | 4 195 | 118.0 | 236.0 | 2 108 | 0 | 0.499 | 262 665 |
| 2, endpoints borrados sin puntuar | 4 195 | 118.0 | 236.0 | 2 108 | 0 | 0.499 | 262 665 |
| 3, más limpieza física | 2 616 | 189.2 | 189.2 | 71 | 0 | 0.499 | 262 665 |
| 4, más refresco | 2 616 | 189.2 | 189.2 | 71 | 0 | 0.162 | 0 |

- **Limpieza:** 4 194 postings reescritos, con 494 992 miembros vivos copiados; 495 000 borrados soltados; 1 646 fusiones, que movieron 138 588 miembros vivos; 0 postings retirados sin ningún vivo; 67 splits tras una fusión.
- **Refresco:** 262 665 vectores, el 53.1 % de los vivos. Por vector: 34.4 cargas, 10 891 endpoints puntuados y 64.0 distancias exactas. Entraron 22 427 aristas inversas.

### El 30 % en regiones enteras

- **Corpus:** 990 000 vectores, 297 259 borrados. Quedan 692 741.
- **Regiones:** 22, con 1 380 postings en total.
- **Referencia, el lote de los que quedan:** 2 936 postings de 235.9 miembros. Cargas a 0.95: 18.87. A 0.99: 90.70, entre ef 320 y 640.
- **Ground truth:** 384 queries con menos de 10 vivos entre sus 100 vecinos del dataset, resueltas por fuerza bruta.

| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | cruza 0.99 entre ef | las tres |
|---|---|---|---|---|---|---|---|---|
| 1, borrado lógico | −0.3158 en ef 1024 | no | no llega a 0.95 | no | no llega a 0.99 | no | — | no |
| 2, endpoints borrados sin puntuar | −0.3158 en ef 1024 | no | no llega a 0.95 | no | no llega a 0.99 | no | — | no |
| 3, más limpieza física | −0.0230 en ef 10 | no | 23.63, +25.2 % | no | 117.30, +29.3 % | no | 640 y 1024 | no |
| 4, más refresco | −0.0090 en ef 10 | no | 19.95, +5.7 % | sí | 99.32, +9.5 % | sí, a menos del suelo de ruido | 640 y 1024 | no |

Recall y cargas a igual ef:

| ef | referencia: recall | cargas | celda 1: recall | cargas | celda 2: recall | cargas | celda 3: recall | cargas | celda 4: recall | cargas |
|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 0.8828 | 8.14 | 0.6354 | 5.30 | 0.6373 | 4.87 | 0.8597 | 7.81 | 0.8738 | 7.90 |
| 20 | 0.9167 | 10.83 | 0.6514 | 7.11 | 0.6526 | 6.32 | 0.8973 | 10.47 | 0.9084 | 10.51 |
| 40 | 0.9396 | 15.17 | 0.6604 | 10.17 | 0.6615 | 8.77 | 0.9243 | 14.71 | 0.9346 | 14.80 |
| 80 | 0.9584 | 22.53 | 0.6673 | 15.27 | 0.6680 | 12.78 | 0.9464 | 21.69 | 0.9547 | 21.86 |
| 160 | 0.9740 | 35.11 | 0.6714 | 24.09 | 0.6719 | 19.45 | 0.9641 | 33.24 | 0.9711 | 33.73 |
| 320 | 0.9841 | 56.25 | 0.6744 | 39.23 | 0.6747 | 30.64 | 0.9763 | 52.84 | 0.9812 | 53.65 |
| 640 | 0.9902 | 92.18 | 0.6767 | 66.03 | 0.6768 | 49.83 | 0.9862 | 86.10 | 0.9887 | 87.59 |
| 1024 | 0.9936 | 130.49 | 0.6778 | 95.67 | 0.6778 | 70.62 | 0.9904 | 121.60 | 0.9922 | 123.70 |

| celda | postings | miembros vivos por posting | miembros que ocupan sitio | bajo el mínimo | vacíos | aristas muertas | vectores con la mitad o más |
|---|---|---|---|---|---|---|---|
| 1, borrado lógico | 4 195 | 165.1 | 236.0 | 1 515 | 1 380 | 0.164 | 83 012 |
| 2, endpoints borrados sin puntuar | 4 195 | 165.1 | 236.0 | 1 515 | 1 380 | 0.164 | 83 012 |
| 3, más limpieza física | 2 750 | 251.9 | 251.9 | 42 | 0 | 0.164 | 83 012 |
| 4, más refresco | 2 750 | 251.9 | 251.9 | 42 | 0 | 0.086 | 0 |

- **Limpieza:** 1 380 postings reescritos, con 0 miembros vivos copiados; 297 259 borrados soltados; 129 fusiones, que movieron 11 204 miembros vivos; 1 360 postings retirados sin ningún vivo; 44 splits tras una fusión.
- **Refresco:** 83 012 vectores, el 12.0 % de los vivos. Por vector: 29.2 cargas, 11 760 endpoints puntuados y 64.0 distancias exactas. Entraron 7 648 aristas inversas.

### Churn, con la celda 4, más refresco

- **Por qué esa celda:** ninguna cumple el criterio al 30 % al azar, y es la de menos cargas a 0.99 sobre su referencia, quedándose la más simple entre dos a menos del suelo de ruido.

| ronda | borrados | insertados | postings | vivos por posting | miembros que ocupan sitio | aristas muertas | splits de la ronda | cargas por insert | reescritos | fusiones | refrescados |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 22 800 | 22 800 | 3 299 | 230.4 | 235.9 | 0.028 | 317 | 34.9 | 0 | 239 | 2 766 |
| 2 | 45 600 | 45 600 | 3 289 | 231.1 | 242.9 | 0.055 | 42 | 34.5 | 0 | 291 | 3 400 |
| 3 | 68 400 | 68 400 | 3 289 | 231.1 | 248.8 | 0.079 | 19 | 34.7 | 0 | 310 | 4 097 |
| 4 | 91 200 | 91 200 | 3 291 | 230.9 | 254.2 | 0.102 | 19 | 34.9 | 0 | 327 | 4 701 |
| 5 | 114 000 | 114 000 | 3 295 | 230.7 | 259.3 | 0.122 | 20 | 34.5 | 0 | 343 | 5 811 |
| 6 | 136 800 | 136 800 | 3 296 | 230.6 | 264.1 | 0.140 | 18 | 34.5 | 0 | 360 | 7 207 |
| 7 | 159 600 | 159 600 | 3 293 | 230.8 | 269.0 | 0.157 | 8 | 34.6 | 0 | 371 | 8 619 |
| 8 | 182 400 | 182 400 | 3 299 | 230.4 | 272.9 | 0.171 | 16 | 34.5 | 0 | 381 | 10 324 |
| 9 | 205 200 | 205 200 | 3 300 | 230.3 | 276.8 | 0.182 | 10 | 34.8 | 0 | 390 | 12 142 |
| 10 | 228 000 | 228 000 | 3 306 | 229.9 | 279.7 | 0.191 | 15 | 34.6 | 12 | 399 | 13 892 |

- **Reescritos y fusiones** son los acumulados; **refrescados**, los de la ronda.

- **Corpus:** 760 000 vectores, 228 000 borrados y 228 000 insertados. Quedan 760 000.
- **Referencia, el lote de los que quedan:** 3 221 postings de 236.0 miembros. Cargas a 0.95: 9.41. A 0.99: 44.43, entre ef 160 y 320.

| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | cruza 0.99 entre ef | las tres |
|---|---|---|---|---|---|---|---|---|
| 4, más refresco | −0.0081 en ef 20 | no | 10.84, +15.1 % | no, a menos del suelo de ruido | 53.71, +20.9 % | no | 160 y 320 | no |

Recall y cargas a igual ef:

| ef | referencia: recall | cargas | celda 4: recall | cargas |
|---|---|---|---|---|
| 10 | 0.9274 | 7.39 | 0.9206 | 7.66 |
| 20 | 0.9553 | 9.96 | 0.9472 | 10.22 |
| 40 | 0.9708 | 14.28 | 0.9641 | 14.54 |
| 80 | 0.9807 | 21.72 | 0.9765 | 22.06 |
| 160 | 0.9876 | 34.36 | 0.9851 | 35.00 |
| 320 | 0.9922 | 56.01 | 0.9906 | 56.70 |
| 640 | 0.9946 | 93.51 | 0.9945 | 94.48 |
| 1024 | 0.9962 | 133.90 | 0.9964 | 135.41 |

| celda | postings | miembros vivos por posting | miembros que ocupan sitio | bajo el mínimo | vacíos | aristas muertas | vectores con la mitad o más |
|---|---|---|---|---|---|---|---|
| 4, más refresco | 3 306 | 229.9 | 279.7 | 4 | 0 | 0.191 | 0 |

- **Limpieza:** 12 postings reescritos, con 1 992 miembros vivos copiados; 63 225 borrados soltados; 399 fusiones, que movieron 36 401 miembros vivos; 0 postings retirados sin ningún vivo; 184 splits tras una fusión.

## La segunda corrida: la celda 1b, las referencias de llenado y el ruido

- **Dataset:** dbpedia-openai-1000k-angular.bin, 990 000 vectores, 5 000 queries, k = 10. Malla de ef: 10, 20, 40, 80, 160, 320, 640, 1 024.
- **Índice:** S = 236, P = 256, R = 64, split a T = 360, ef de inserción 160. Limpieza: se reescribe por encima del 30 % de borrados y se fusiona por debajo de 118 vivos.
- **El lote del 100 %:** 4 195 postings de 236.0 miembros de media.
- **Ningún stop:** ningún vector borrado devuelto y cobertura completa en todo lo medido.
- **De dónde sale cada veredicto:** el del criterio original para las celdas 1 a 4, de la primera corrida; el de la celda 1b y la lectura de llenado, de esta, donde cada celda tiene sus referencias en la misma corrida.

### La celda 1b: un miembro borrado sigue siendo fuente de ruteo

| punto | celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | las tres |
|---|---|---|---|---|---|---|---|---|
| el 30 % al azar | 1 | −0.0154 en ef 10 | no | 13.82, +48.7 % | no | 74.32, +67.2 % | no | no |
| el 30 % al azar | 1b | +0.0001 en ef 10 | sí | 11.08, +19.1 % | no | 50.56, +13.7 % | no | no |
| el 30 % al azar | 2 | −0.0235 en ef 10 | no | 13.62, +46.6 % | no | 74.75, +68.1 % | no | no |
| el 10 % al azar | 1 | −0.0058 en ef 10 | no, a menos del suelo de ruido | 10.96, +12.1 % | sí | 58.35, +12.3 % | no, a menos del suelo de ruido | no |
| el 10 % al azar | 1b | −0.0018 en ef 10 | sí | 10.37, +6.1 % | sí | 53.82, +3.6 % | sí | **sí** |
| el 10 % al azar | 2 | −0.0078 en ef 10 | no | 11.04, +12.9 % | sí | 60.16, +15.8 % | no | no |
| el 50 % al azar | 1 | −0.0257 en ef 10 | no | 18.14, +107.6 % | no | 103.75, +185.4 % | no | no |
| el 50 % al azar | 1b | +0.0001 en ef 1024 | sí | 12.46, +42.6 % | no | 52.75, +45.1 % | no | no |
| el 50 % al azar | 2 | −0.0471 en ef 10 | no | 18.65, +113.4 % | no | 106.97, +194.3 % | no | no |
| el 30 % en regiones enteras | 1 | −0.3138 en ef 1024 | no | no llega a 0.95 | no | no llega a 0.99 | no | no |
| el 30 % en regiones enteras | 1b | +0.0016 en ef 1024 | sí | 17.54, −7.6 % | sí | 86.88, −4.1 % | sí | **sí** |
| el 30 % en regiones enteras | 2 | −0.3138 en ef 1024 | no | no llega a 0.95 | no | no llega a 0.99 | no | no |

El trabajo de una búsqueda a igual ef, en ef 160: un borrado que la celda 1b puntúa cuenta como un vector puntuado más en el scan.

| punto | celda | recall | cargas | vectores puntuados en el scan | endpoints puntuados |
|---|---|---|---|---|---|
| el 30 % al azar | 1 | 0.9839 | 43.77 | 7 675 | 18 144 |
| el 30 % al azar | 1b | 0.9891 | 45.10 | 11 319 | 22 969 |
| el 30 % al azar | 2 | 0.9807 | 36.56 | 6 427 | 11 424 |
| el 10 % al azar | 1 | 0.9851 | 37.56 | 8 487 | 18 170 |
| el 10 % al azar | 1b | 0.9867 | 37.85 | 9 508 | 19 452 |
| el 10 % al azar | 2 | 0.9835 | 35.61 | 8 050 | 15 876 |
| el 50 % al azar | 1 | 0.9806 | 53.37 | 6 666 | 17 953 |
| el 50 % al azar | 1b | 0.9908 | 57.17 | 14 322 | 28 731 |
| el 50 % al azar | 2 | 0.9731 | 37.71 | 4 737 | 7 263 |
| el 30 % en regiones enteras | 1 | 0.6733 | 24.23 | 4 796 | 9 904 |
| el 30 % en regiones enteras | 1b | 0.9852 | 55.96 | 14 057 | 28 620 |
| el 30 % en regiones enteras | 2 | 0.6737 | 19.53 | 4 908 | 8 018 |

**La celda 1b no cumple el criterio con el 30 % borrado al azar: falla en las cargas a 0.95 y en las cargas a 0.99.**

### La lectura de llenado: cada celda contra un lote con el tamaño vivo de sus postings

Si una celda cumple el recall a igual ef y falla solo en cargas, y contra su referencia de llenado las cargas quedan dentro de los dos umbrales (+15 % a 0.95 y +10 % a 0.99), el fallo es de llenado y se atribuye a la política de fusión, no a las aristas. El criterio original no cambia.

**El 30 % al azar:**

| referencia en lote | postings | miembros por posting | cargas a 0.95 | sobre la de S = 236 | cargas a 0.99 | sobre la de S = 236 |
|---|---|---|---|---|---|---|
| S = 236, la del criterio | 2 937 | 236.0 | 9.30 | | 44.46 | |
| S = 165, de llenado | 4 200 | 165.0 | 10.54 | +13.4 % | 59.36 | +33.5 % |
| S = 197, de llenado | 3 518 | 197.0 | 9.85 | +6.0 % | 50.39 | +13.3 % |

| celda | peor recall a igual ef contra S = 236 | cargas contra S = 236: a 0.95 | a 0.99 | su referencia | contra ella: a 0.95 | dentro | a 0.99 | dentro | peor recall a igual ef contra ella | lectura |
|---|---|---|---|---|---|---|---|---|---|---|
| 1, borrado lógico | −0.0154 | +48.7 % | +67.2 % | S = 165 | +31.2 % | no | +25.2 % | no | −0.0077 | falla el recall a igual ef: la lectura no aplica |
| 1b, borrado lógico con los borrados como fuentes | +0.0001 | +19.1 % | +13.7 % | S = 165 | +5.1 % | sí | −14.8 % | sí | +0.0008 | **fallo de llenado:** de la política de fusión, no de las aristas |
| 2, endpoints borrados sin puntuar | −0.0235 | +46.6 % | +68.1 % | S = 165 | +29.3 % | no | +25.9 % | no | −0.0158 | falla el recall a igual ef: la lectura no aplica |
| 3, más limpieza física | −0.0214 | +36.6 % | +50.4 % | S = 197 | +28.9 % | no | +32.7 % | no | −0.0182 | falla el recall a igual ef: la lectura no aplica |
| 4, más refresco | −0.0173 | +28.1 % | +36.0 % | S = 197 | +20.9 % | no | +20.0 % | no | −0.0140 | falla el recall a igual ef: la lectura no aplica |

**El 10 % al azar:**

| referencia en lote | postings | miembros por posting | cargas a 0.95 | sobre la de S = 236 | cargas a 0.99 | sobre la de S = 236 |
|---|---|---|---|---|---|---|
| S = 236, la del criterio | 3 776 | 236.0 | 9.78 | | 51.94 | |
| S = 212, de llenado | 4 203 | 212.0 | 10.01 | +2.4 % | 55.33 | +6.5 % |
| S = 229, de llenado | 3 891 | 229.0 | 9.79 | +0.1 % | 52.85 | +1.8 % |

| celda | peor recall a igual ef contra S = 236 | cargas contra S = 236: a 0.95 | a 0.99 | su referencia | contra ella: a 0.95 | dentro | a 0.99 | dentro | peor recall a igual ef contra ella | lectura |
|---|---|---|---|---|---|---|---|---|---|---|
| 1, borrado lógico | −0.0058 | +12.1 % | +12.3 % | S = 212 | +9.5 % | sí | +5.4 % | sí | −0.0051 | falla el recall a igual ef: la lectura no aplica |
| 1b, borrado lógico con los borrados como fuentes | −0.0018 | +6.1 % | +3.6 % | S = 212 | +3.6 % | sí | −2.7 % | sí | −0.0011 | cumple el criterio: no hay fallo que leer |
| 2, endpoints borrados sin puntuar | −0.0078 | +12.9 % | +15.8 % | S = 212 | +10.3 % | sí | +8.7 % | sí, a menos del suelo de ruido | −0.0071 | falla el recall a igual ef: la lectura no aplica |
| 3, más limpieza física | −0.0066 | +9.9 % | +11.8 % | S = 229 | +9.8 % | sí | +9.9 % | sí, a menos del suelo de ruido | −0.0076 | falla el recall a igual ef: la lectura no aplica |
| 4, más refresco | −0.0064 | +9.6 % | +11.5 % | S = 229 | +9.5 % | sí | +9.6 % | sí, a menos del suelo de ruido | −0.0074 | falla el recall a igual ef: la lectura no aplica |

**El 50 % al azar:**

| referencia en lote | postings | miembros por posting | cargas a 0.95 | sobre la de S = 236 | cargas a 0.99 | sobre la de S = 236 |
|---|---|---|---|---|---|---|
| S = 236, la del criterio | 2 098 | 235.9 | 8.74 | | 36.35 | |
| S = 118, de llenado | 4 194 | 118.0 | 12.14 | +38.9 % | 63.56 | +74.8 % |
| S = 189, de llenado | 2 620 | 188.9 | 9.44 | +8.0 % | 45.21 | +24.4 % |

| celda | peor recall a igual ef contra S = 236 | cargas contra S = 236: a 0.95 | a 0.99 | su referencia | contra ella: a 0.95 | dentro | a 0.99 | dentro | peor recall a igual ef contra ella | lectura |
|---|---|---|---|---|---|---|---|---|---|---|
| 1, borrado lógico | −0.0257 | +107.6 % | +185.4 % | S = 118 | +49.4 % | no | +63.2 % | no | −0.0073 | falla el recall a igual ef: la lectura no aplica |
| 1b, borrado lógico con los borrados como fuentes | +0.0001 | +42.6 % | +45.1 % | S = 118 | +2.7 % | sí | −17.0 % | sí | +0.0014 | **fallo de llenado:** de la política de fusión, no de las aristas |
| 2, endpoints borrados sin puntuar | −0.0471 | +113.4 % | +194.3 % | S = 118 | +53.6 % | no | +68.3 % | no | −0.0283 | falla el recall a igual ef: la lectura no aplica |
| 3, más limpieza física | −0.0332 | +62.0 % | +119.1 % | S = 189 | +50.1 % | no | +76.2 % | no | −0.0297 | falla el recall a igual ef: la lectura no aplica |
| 4, más refresco | −0.0127 | +18.1 % | +40.8 % | S = 189 | +9.4 % | sí | +13.2 % | no | −0.0091 | falla el recall a igual ef: la lectura no aplica |

**El 30 % en regiones enteras:**

| referencia en lote | postings | miembros por posting | cargas a 0.95 | sobre la de S = 236 | cargas a 0.99 | sobre la de S = 236 |
|---|---|---|---|---|---|---|
| S = 236, la del criterio | 2 936 | 235.9 | 18.98 | | 90.56 | |
| S = 165, de llenado | 4 198 | 165.0 | 23.03 | +21.4 % | 119.06 | +31.5 % |
| S = 252, de llenado | 2 749 | 252.0 | 17.89 | −5.7 % | 84.85 | −6.3 % |

| celda | peor recall a igual ef contra S = 236 | cargas contra S = 236: a 0.95 | a 0.99 | su referencia | contra ella: a 0.95 | dentro | a 0.99 | dentro | peor recall a igual ef contra ella | lectura |
|---|---|---|---|---|---|---|---|---|---|---|
| 1, borrado lógico | −0.3138 | no llega | no llega | S = 165 | no llega | no | no llega | no | −0.3124 | falla el recall a igual ef: la lectura no aplica |
| 1b, borrado lógico con los borrados como fuentes | +0.0016 | −7.6 % | −4.1 % | S = 165 | −23.8 % | sí | −27.0 % | sí | +0.0030 | cumple el criterio: no hay fallo que leer |
| 2, endpoints borrados sin puntuar | −0.3138 | no llega | no llega | S = 165 | no llega | no | no llega | no | −0.3124 | falla el recall a igual ef: la lectura no aplica |
| 3, más limpieza física | −0.0241 | +25.5 % | +29.3 % | S = 252 | +33.1 % | no | +38.0 % | no | −0.0274 | falla el recall a igual ef: la lectura no aplica |
| 4, más refresco | −0.0102 | +5.4 % | +9.4 % | S = 252 | +11.8 % | sí | +16.8 % | no | −0.0134 | falla el recall a igual ef: la lectura no aplica |

**Churn: el 30 % borrado y otro tanto insertado:**

| referencia en lote | postings | miembros por posting | cargas a 0.95 | sobre la de S = 236 | cargas a 0.99 | sobre la de S = 236 |
|---|---|---|---|---|---|---|
| S = 236, la del criterio | 3 221 | 236.0 | 9.40 | | 44.52 | |
| S = 230, de llenado | 3 304 | 230.0 | 9.49 | +1.0 % | 44.78 | +0.6 % |

| celda | peor recall a igual ef contra S = 236 | cargas contra S = 236: a 0.95 | a 0.99 | su referencia | contra ella: a 0.95 | dentro | a 0.99 | dentro | peor recall a igual ef contra ella | lectura |
|---|---|---|---|---|---|---|---|---|---|---|
| 4, más refresco | −0.0074 | +13.0 % | +22.2 % | S = 230 | +11.9 % | sí | +21.5 % | no | −0.0073 | falla el recall a igual ef: la lectura no aplica |

### El ruido entre las dos corridas: las celdas 1 a 4, medidas dos veces

| punto | índice | cargas a 0.95: primera | segunda | diferencia | cargas a 0.99: primera | segunda | diferencia |
|---|---|---|---|---|---|---|---|
| el 30 % al azar | referencia | 9.28 | 9.30 | +0.1 % | 44.46 | 44.46 | −0.0 % |
| el 30 % al azar | 1 | 13.70 | 13.82 | +0.9 % | 74.32 | 74.32 | −0.0 % |
| el 30 % al azar | 2 | 13.48 | 13.62 | +1.1 % | 74.75 | 74.75 | −0.0 % |
| el 30 % al azar | 3 | 12.73 | 12.70 | −0.2 % | 66.99 | 66.88 | −0.2 % |
| el 30 % al azar | 4 | 11.95 | 11.91 | −0.3 % | 60.62 | 60.48 | −0.2 % |
| el 10 % al azar | referencia | 9.72 | 9.78 | +0.6 % | 51.97 | 51.94 | −0.1 % |
| el 10 % al azar | 1 | 10.92 | 10.96 | +0.4 % | 58.32 | 58.35 | +0.0 % |
| el 10 % al azar | 2 | 11.03 | 11.04 | +0.1 % | 60.15 | 60.16 | +0.0 % |
| el 10 % al azar | 3 | 10.76 | 10.74 | −0.2 % | 58.08 | 58.07 | −0.0 % |
| el 10 % al azar | 4 | 10.73 | 10.71 | −0.2 % | 57.93 | 57.93 | −0.0 % |
| el 50 % al azar | referencia | 8.73 | 8.74 | +0.1 % | 36.25 | 36.35 | +0.3 % |
| el 50 % al azar | 1 | 18.14 | 18.14 | +0.0 % | 103.74 | 103.75 | +0.0 % |
| el 50 % al azar | 2 | 18.63 | 18.65 | +0.1 % | 106.96 | 106.97 | +0.0 % |
| el 50 % al azar | 3 | 14.19 | 14.16 | −0.2 % | 79.61 | 79.64 | +0.0 % |
| el 50 % al azar | 4 | 10.37 | 10.32 | −0.5 % | 51.32 | 51.17 | −0.3 % |
| el 30 % en regiones enteras | referencia | 18.87 | 18.98 | +0.6 % | 90.70 | 90.56 | −0.2 % |
| el 30 % en regiones enteras | 1 | no llega | no llega | — | no llega | no llega | — |
| el 30 % en regiones enteras | 2 | no llega | no llega | — | no llega | no llega | — |
| el 30 % en regiones enteras | 3 | 23.63 | 23.81 | +0.8 % | 117.30 | 117.13 | −0.2 % |
| el 30 % en regiones enteras | 4 | 19.95 | 20.01 | +0.3 % | 99.32 | 99.07 | −0.3 % |
| churn: el 30 % borrado y otro tanto insertado | referencia | 9.41 | 9.40 | −0.1 % | 44.43 | 44.52 | +0.2 % |
| churn: el 30 % borrado y otro tanto insertado | 4 | 10.84 | 10.62 | −2.0 % | 53.71 | 54.39 | +1.3 % |

- **La mayor diferencia entre las dos corridas,** sin el churn: 1.1 % a 0.95 y 0.3 % a 0.99. El suelo de ruido de v2.0, que es el que marca las condiciones: 0.5 % y 3.0 %.

Las cantidades del criterio, cada celda contra la referencia de S de su corrida:

| punto | celda | peor recall a igual ef: primera | segunda | cargas a 0.95 sobre la referencia: primera | segunda | a 0.99: primera | segunda | las tres: primera | segunda |
|---|---|---|---|---|---|---|---|---|---|
| el 30 % al azar | 1 | −0.0148 | −0.0154 | +47.6 % | +48.7 % | +67.2 % | +67.2 % | no | no |
| el 30 % al azar | 2 | −0.0227 | −0.0235 | +45.2 % | +46.6 % | +68.1 % | +68.1 % | no | no |
| el 30 % al azar | 3 | −0.0214 | −0.0214 | +37.1 % | +36.6 % | +50.7 % | +50.4 % | no | no |
| el 30 % al azar | 4 | −0.0173 | −0.0173 | +28.7 % | +28.1 % | +36.3 % | +36.0 % | no | no |
| el 10 % al azar | 1 | −0.0066 | −0.0058 | +12.4 % | +12.1 % | +12.2 % | +12.3 % | no | no |
| el 10 % al azar | 2 | −0.0083 | −0.0078 | +13.5 % | +12.9 % | +15.7 % | +15.8 % | no | no |
| el 10 % al azar | 3 | −0.0076 | −0.0066 | +10.7 % | +9.9 % | +11.7 % | +11.8 % | no | no |
| el 10 % al azar | 4 | −0.0074 | −0.0064 | +10.4 % | +9.6 % | +11.5 % | +11.5 % | no | no |
| el 50 % al azar | 1 | −0.0263 | −0.0257 | +107.9 % | +107.6 % | +186.2 % | +185.4 % | no | no |
| el 50 % al azar | 2 | −0.0475 | −0.0471 | +113.5 % | +113.4 % | +195.1 % | +194.3 % | no | no |
| el 50 % al azar | 3 | −0.0342 | −0.0332 | +62.6 % | +62.0 % | +119.6 % | +119.1 % | no | no |
| el 50 % al azar | 4 | −0.0133 | −0.0127 | +18.8 % | +18.1 % | +41.6 % | +40.8 % | no | no |
| el 30 % en regiones enteras | 1 | −0.3158 | −0.3138 | no llega | no llega | no llega | no llega | no | no |
| el 30 % en regiones enteras | 2 | −0.3158 | −0.3138 | no llega | no llega | no llega | no llega | no | no |
| el 30 % en regiones enteras | 3 | −0.0230 | −0.0241 | +25.2 % | +25.5 % | +29.3 % | +29.3 % | no | no |
| el 30 % en regiones enteras | 4 | −0.0090 | −0.0102 | +5.7 % | +5.4 % | +9.5 % | +9.4 % | no | no |
| churn: el 30 % borrado y otro tanto insertado | 4 | −0.0081 | −0.0074 | +15.1 % | +13.0 % | +20.9 % | +22.2 % | no | no |

- **Ningún veredicto cambia de una corrida a otra.**

### Segunda corrida, el 30 % al azar

- **Corpus:** 990 000 vectores, 297 000 borrados. Quedan 693 000.
- **Referencia, el lote de los que quedan:** 2 937 postings de 236.0 miembros. Cargas a 0.95: 9.30. A 0.99: 44.46, entre ef 160 y 320.

| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | cruza 0.99 entre ef | las tres |
|---|---|---|---|---|---|---|---|---|
| 1, borrado lógico | −0.0154 en ef 10 | no | 13.82, +48.7 % | no | 74.32, +67.2 % | no | 320 y 640 | no |
| 1b, borrado lógico con los borrados como fuentes | +0.0001 en ef 10 | sí | 11.08, +19.1 % | no | 50.56, +13.7 % | no | 160 y 320 | no |
| 2, endpoints borrados sin puntuar | −0.0235 en ef 10 | no | 13.62, +46.6 % | no | 74.75, +68.1 % | no | 320 y 640 | no |
| 3, más limpieza física | −0.0214 en ef 10 | no | 12.70, +36.6 % | no | 66.88, +50.4 % | no | 320 y 640 | no |
| 4, más refresco | −0.0173 en ef 10 | no | 11.91, +28.1 % | no | 60.48, +36.0 % | no | 320 y 640 | no |

Recall y cargas a igual ef:

| ef | referencia: recall | cargas | celda 1: recall | cargas | celda 1b: recall | cargas | celda 2: recall | cargas | celda 3: recall | cargas | celda 4: recall | cargas |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 0.9295 | 7.40 | 0.9141 | 8.73 | 0.9296 | 8.75 | 0.9060 | 7.83 | 0.9080 | 7.68 | 0.9122 | 7.69 |
| 20 | 0.9558 | 9.92 | 0.9432 | 12.02 | 0.9580 | 12.15 | 0.9356 | 10.48 | 0.9382 | 10.27 | 0.9423 | 10.29 |
| 40 | 0.9709 | 14.26 | 0.9621 | 17.73 | 0.9731 | 18.00 | 0.9557 | 15.13 | 0.9582 | 14.71 | 0.9612 | 14.71 |
| 80 | 0.9799 | 21.50 | 0.9752 | 27.31 | 0.9830 | 28.03 | 0.9707 | 22.85 | 0.9727 | 22.23 | 0.9748 | 22.33 |
| 160 | 0.9872 | 34.10 | 0.9839 | 43.77 | 0.9891 | 45.10 | 0.9807 | 36.56 | 0.9824 | 35.25 | 0.9837 | 35.43 |
| 320 | 0.9923 | 55.57 | 0.9898 | 72.19 | 0.9929 | 74.37 | 0.9881 | 60.26 | 0.9886 | 57.48 | 0.9896 | 57.87 |
| 640 | 0.9953 | 92.89 | 0.9940 | 121.00 | 0.9956 | 125.02 | 0.9927 | 101.43 | 0.9933 | 96.27 | 0.9939 | 97.02 |
| 1024 | 0.9968 | 133.03 | 0.9960 | 173.84 | 0.9969 | 179.57 | 0.9949 | 146.32 | 0.9953 | 138.08 | 0.9957 | 139.33 |

| celda | postings | miembros vivos por posting | miembros que ocupan sitio | bajo el mínimo | vacíos | aristas muertas | vectores con la mitad o más |
|---|---|---|---|---|---|---|---|
| 1, borrado lógico | 4 195 | 165.2 | 236.0 | 880 | 0 | 0.300 | 99 641 |
| 1b, borrado lógico con los borrados como fuentes | 4 195 | 165.2 | 236.0 | 880 | 0 | 0.300 | 99 641 |
| 2, endpoints borrados sin puntuar | 4 195 | 165.2 | 236.0 | 880 | 0 | 0.300 | 99 641 |
| 3, más limpieza física | 3 513 | 197.3 | 230.2 | 57 | 0 | 0.300 | 99 641 |
| 4, más refresco | 3 513 | 197.3 | 230.2 | 57 | 0 | 0.238 | 0 |

- **Limpieza:** 2 076 postings reescritos, con 331 305 miembros vivos copiados; 181 165 borrados soltados; 743 fusiones, que movieron 63 045 miembros vivos; 0 postings retirados sin ningún vivo; 61 splits tras una fusión.
- **Refresco:** 99 641 vectores, el 14.4 % de los vivos. Por vector: 30.9 cargas, 10 329 endpoints puntuados y 64.0 distancias exactas. Entraron 6 746 aristas inversas.

### Segunda corrida, el 10 % al azar

- **Corpus:** 990 000 vectores, 99 000 borrados. Quedan 891 000.
- **Referencia, el lote de los que quedan:** 3 776 postings de 236.0 miembros. Cargas a 0.95: 9.78. A 0.99: 51.94, entre ef 160 y 320.

| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | cruza 0.99 entre ef | las tres |
|---|---|---|---|---|---|---|---|---|
| 1, borrado lógico | −0.0058 en ef 10 | no, a menos del suelo de ruido | 10.96, +12.1 % | sí | 58.35, +12.3 % | no, a menos del suelo de ruido | 160 y 320 | no |
| 1b, borrado lógico con los borrados como fuentes | −0.0018 en ef 10 | sí | 10.37, +6.1 % | sí | 53.82, +3.6 % | sí | 160 y 320 | **sí** |
| 2, endpoints borrados sin puntuar | −0.0078 en ef 10 | no | 11.04, +12.9 % | sí | 60.16, +15.8 % | no | 320 y 640 | no |
| 3, más limpieza física | −0.0066 en ef 10 | no | 10.74, +9.9 % | sí | 58.07, +11.8 % | no, a menos del suelo de ruido | 320 y 640 | no |
| 4, más refresco | −0.0064 en ef 10 | no | 10.71, +9.6 % | sí | 57.93, +11.5 % | no, a menos del suelo de ruido | 320 y 640 | no |

Recall y cargas a igual ef:

| ef | referencia: recall | cargas | celda 1: recall | cargas | celda 1b: recall | cargas | celda 2: recall | cargas | celda 3: recall | cargas | celda 4: recall | cargas |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 0.9259 | 7.52 | 0.9201 | 7.89 | 0.9241 | 7.88 | 0.9181 | 7.62 | 0.9193 | 7.59 | 0.9195 | 7.59 |
| 20 | 0.9529 | 10.09 | 0.9489 | 10.70 | 0.9532 | 10.73 | 0.9466 | 10.25 | 0.9479 | 10.24 | 0.9479 | 10.23 |
| 40 | 0.9680 | 14.48 | 0.9655 | 15.49 | 0.9685 | 15.51 | 0.9634 | 14.76 | 0.9639 | 14.68 | 0.9641 | 14.69 |
| 80 | 0.9780 | 21.98 | 0.9766 | 23.65 | 0.9794 | 23.75 | 0.9747 | 22.36 | 0.9752 | 22.21 | 0.9754 | 22.22 |
| 160 | 0.9858 | 34.80 | 0.9851 | 37.56 | 0.9867 | 37.85 | 0.9835 | 35.61 | 0.9842 | 35.25 | 0.9843 | 35.26 |
| 320 | 0.9909 | 56.75 | 0.9906 | 61.34 | 0.9913 | 61.77 | 0.9897 | 58.15 | 0.9899 | 57.44 | 0.9899 | 57.45 |
| 640 | 0.9944 | 94.76 | 0.9938 | 102.41 | 0.9945 | 103.39 | 0.9937 | 97.25 | 0.9937 | 95.65 | 0.9937 | 95.68 |
| 1024 | 0.9962 | 135.74 | 0.9958 | 146.57 | 0.9962 | 147.77 | 0.9956 | 139.25 | 0.9957 | 136.67 | 0.9957 | 136.72 |

| celda | postings | miembros vivos por posting | miembros que ocupan sitio | bajo el mínimo | vacíos | aristas muertas | vectores con la mitad o más |
|---|---|---|---|---|---|---|---|
| 1, borrado lógico | 4 195 | 212.4 | 236.0 | 430 | 0 | 0.100 | 14 616 |
| 1b, borrado lógico con los borrados como fuentes | 4 195 | 212.4 | 236.0 | 430 | 0 | 0.100 | 14 616 |
| 2, endpoints borrados sin puntuar | 4 195 | 212.4 | 236.0 | 430 | 0 | 0.100 | 14 616 |
| 3, más limpieza física | 3 895 | 228.8 | 252.6 | 36 | 0 | 0.100 | 14 616 |
| 4, más refresco | 3 895 | 228.8 | 252.6 | 36 | 0 | 0.097 | 0 |

- **Limpieza:** 1 postings reescritos, con 3 miembros vivos copiados; 6 134 borrados soltados; 364 fusiones, que movieron 28 778 miembros vivos; 0 postings retirados sin ningún vivo; 64 splits tras una fusión.
- **Refresco:** 14 616 vectores, el 1.6 % de los vivos. Por vector: 22.0 cargas, 8 033 endpoints puntuados y 64.0 distancias exactas. Entraron 579 aristas inversas.

### Segunda corrida, el 50 % al azar

- **Corpus:** 990 000 vectores, 495 000 borrados. Quedan 495 000.
- **Referencia, el lote de los que quedan:** 2 098 postings de 235.9 miembros. Cargas a 0.95: 8.74. A 0.99: 36.35, entre ef 160 y 320.

| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | cruza 0.99 entre ef | las tres |
|---|---|---|---|---|---|---|---|---|
| 1, borrado lógico | −0.0257 en ef 10 | no | 18.14, +107.6 % | no | 103.75, +185.4 % | no | 320 y 640 | no |
| 1b, borrado lógico con los borrados como fuentes | +0.0001 en ef 1024 | sí | 12.46, +42.6 % | no | 52.75, +45.1 % | no | 80 y 160 | no |
| 2, endpoints borrados sin puntuar | −0.0471 en ef 10 | no | 18.65, +113.4 % | no | 106.97, +194.3 % | no | 320 y 640 | no |
| 3, más limpieza física | −0.0332 en ef 10 | no | 14.16, +62.0 % | no | 79.64, +119.1 % | no | 320 y 640 | no |
| 4, más refresco | −0.0127 en ef 10 | no | 10.32, +18.1 % | no | 51.17, +40.8 % | no | 160 y 320 | no |

Recall y cargas a igual ef:

| ef | referencia: recall | cargas | celda 1: recall | cargas | celda 1b: recall | cargas | celda 2: recall | cargas | celda 3: recall | cargas | celda 4: recall | cargas |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 0.9320 | 7.20 | 0.9063 | 10.00 | 0.9330 | 10.15 | 0.8848 | 8.03 | 0.8987 | 7.54 | 0.9193 | 7.52 |
| 20 | 0.9592 | 9.65 | 0.9375 | 14.18 | 0.9630 | 14.58 | 0.9186 | 10.83 | 0.9293 | 10.06 | 0.9488 | 10.06 |
| 40 | 0.9733 | 13.77 | 0.9576 | 21.06 | 0.9777 | 22.11 | 0.9424 | 15.52 | 0.9496 | 14.03 | 0.9652 | 14.31 |
| 80 | 0.9827 | 20.76 | 0.9712 | 32.98 | 0.9857 | 35.08 | 0.9597 | 23.52 | 0.9651 | 20.88 | 0.9770 | 21.68 |
| 160 | 0.9891 | 32.69 | 0.9806 | 53.37 | 0.9908 | 57.17 | 0.9731 | 37.71 | 0.9765 | 32.82 | 0.9856 | 34.52 |
| 320 | 0.9933 | 53.43 | 0.9885 | 88.77 | 0.9943 | 95.49 | 0.9838 | 62.78 | 0.9855 | 53.74 | 0.9912 | 56.82 |
| 640 | 0.9962 | 89.70 | 0.9937 | 150.31 | 0.9965 | 161.86 | 0.9900 | 106.97 | 0.9914 | 89.96 | 0.9946 | 95.44 |
| 1024 | 0.9972 | 128.79 | 0.9951 | 217.38 | 0.9973 | 233.74 | 0.9932 | 155.95 | 0.9942 | 129.83 | 0.9963 | 137.91 |

| celda | postings | miembros vivos por posting | miembros que ocupan sitio | bajo el mínimo | vacíos | aristas muertas | vectores con la mitad o más |
|---|---|---|---|---|---|---|---|
| 1, borrado lógico | 4 195 | 118.0 | 236.0 | 2 108 | 0 | 0.499 | 262 665 |
| 1b, borrado lógico con los borrados como fuentes | 4 195 | 118.0 | 236.0 | 2 108 | 0 | 0.499 | 262 665 |
| 2, endpoints borrados sin puntuar | 4 195 | 118.0 | 236.0 | 2 108 | 0 | 0.499 | 262 665 |
| 3, más limpieza física | 2 615 | 189.3 | 189.3 | 71 | 0 | 0.499 | 262 665 |
| 4, más refresco | 2 615 | 189.3 | 189.3 | 71 | 0 | 0.162 | 0 |

- **Limpieza:** 4 194 postings reescritos, con 494 992 miembros vivos copiados; 495 000 borrados soltados; 1 647 fusiones, que movieron 138 646 miembros vivos; 0 postings retirados sin ningún vivo; 67 splits tras una fusión.
- **Refresco:** 262 665 vectores, el 53.1 % de los vivos. Por vector: 34.4 cargas, 10 892 endpoints puntuados y 64.0 distancias exactas. Entraron 22 425 aristas inversas.

### Segunda corrida, el 30 % en regiones enteras

- **Corpus:** 990 000 vectores, 297 259 borrados. Quedan 692 741.
- **Regiones:** 22, con 1 380 postings en total.
- **Referencia, el lote de los que quedan:** 2 936 postings de 235.9 miembros. Cargas a 0.95: 18.98. A 0.99: 90.56, entre ef 320 y 640.
- **Ground truth:** 384 queries con menos de 10 vivos entre sus 100 vecinos del dataset, resueltas por fuerza bruta.

| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | cruza 0.99 entre ef | las tres |
|---|---|---|---|---|---|---|---|---|
| 1, borrado lógico | −0.3138 en ef 1024 | no | no llega a 0.95 | no | no llega a 0.99 | no | — | no |
| 1b, borrado lógico con los borrados como fuentes | +0.0016 en ef 1024 | sí | 17.54, −7.6 % | sí | 86.88, −4.1 % | sí | 160 y 320 | **sí** |
| 2, endpoints borrados sin puntuar | −0.3138 en ef 1024 | no | no llega a 0.95 | no | no llega a 0.99 | no | — | no |
| 3, más limpieza física | −0.0241 en ef 10 | no | 23.81, +25.5 % | no | 117.13, +29.3 % | no | 640 y 1024 | no |
| 4, más refresco | −0.0102 en ef 10 | no | 20.01, +5.4 % | sí | 99.07, +9.4 % | sí, a menos del suelo de ruido | 640 y 1024 | no |

Recall y cargas a igual ef:

| ef | referencia: recall | cargas | celda 1: recall | cargas | celda 1b: recall | cargas | celda 2: recall | cargas | celda 3: recall | cargas | celda 4: recall | cargas |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 0.8827 | 8.14 | 0.6375 | 5.32 | 0.9120 | 11.71 | 0.6394 | 4.88 | 0.8586 | 7.77 | 0.8726 | 7.85 |
| 20 | 0.9162 | 10.82 | 0.6532 | 7.14 | 0.9465 | 16.30 | 0.6545 | 6.34 | 0.8961 | 10.45 | 0.9080 | 10.51 |
| 40 | 0.9391 | 15.16 | 0.6620 | 10.22 | 0.9645 | 23.76 | 0.6632 | 8.80 | 0.9232 | 14.67 | 0.9339 | 14.80 |
| 80 | 0.9584 | 22.54 | 0.6692 | 15.36 | 0.9772 | 35.89 | 0.6699 | 12.84 | 0.9460 | 21.67 | 0.9547 | 21.86 |
| 160 | 0.9742 | 35.15 | 0.6733 | 24.23 | 0.9852 | 55.96 | 0.6737 | 19.53 | 0.9641 | 33.22 | 0.9709 | 33.71 |
| 320 | 0.9841 | 56.26 | 0.6763 | 39.46 | 0.9903 | 88.95 | 0.6766 | 30.77 | 0.9760 | 52.81 | 0.9811 | 53.64 |
| 640 | 0.9902 | 92.18 | 0.6787 | 66.41 | 0.9938 | 144.48 | 0.6788 | 50.07 | 0.9862 | 86.11 | 0.9888 | 87.61 |
| 1024 | 0.9936 | 130.49 | 0.6798 | 96.22 | 0.9952 | 202.90 | 0.6798 | 70.97 | 0.9905 | 121.62 | 0.9922 | 123.71 |

| celda | postings | miembros vivos por posting | miembros que ocupan sitio | bajo el mínimo | vacíos | aristas muertas | vectores con la mitad o más |
|---|---|---|---|---|---|---|---|
| 1, borrado lógico | 4 195 | 165.1 | 236.0 | 1 515 | 1 380 | 0.164 | 83 012 |
| 1b, borrado lógico con los borrados como fuentes | 4 195 | 165.1 | 236.0 | 1 515 | 1 380 | 0.164 | 83 012 |
| 2, endpoints borrados sin puntuar | 4 195 | 165.1 | 236.0 | 1 515 | 1 380 | 0.164 | 83 012 |
| 3, más limpieza física | 2 750 | 251.9 | 251.9 | 42 | 0 | 0.164 | 83 012 |
| 4, más refresco | 2 750 | 251.9 | 251.9 | 42 | 0 | 0.086 | 0 |

- **Limpieza:** 1 380 postings reescritos, con 0 miembros vivos copiados; 297 259 borrados soltados; 129 fusiones, que movieron 11 204 miembros vivos; 1 360 postings retirados sin ningún vivo; 44 splits tras una fusión.
- **Refresco:** 83 012 vectores, el 12.0 % de los vivos. Por vector: 29.2 cargas, 11 760 endpoints puntuados y 64.0 distancias exactas. Entraron 7 648 aristas inversas.

### Segunda corrida, churn, con la celda 4, más refresco

- **Por qué esa celda:** es la que eligió la primera corrida.

| ronda | borrados | insertados | postings | vivos por posting | miembros que ocupan sitio | aristas muertas | splits de la ronda | cargas por insert | reescritos | fusiones | refrescados |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 22 800 | 22 800 | 3 299 | 230.4 | 235.9 | 0.028 | 320 | 34.8 | 0 | 242 | 2 764 |
| 2 | 45 600 | 45 600 | 3 287 | 231.2 | 243.0 | 0.055 | 43 | 34.5 | 0 | 297 | 3 393 |
| 3 | 68 400 | 68 400 | 3 288 | 231.1 | 248.9 | 0.079 | 25 | 34.7 | 0 | 321 | 4 114 |
| 4 | 91 200 | 91 200 | 3 289 | 231.1 | 254.3 | 0.102 | 21 | 34.9 | 0 | 341 | 4 696 |
| 5 | 114 000 | 114 000 | 3 295 | 230.7 | 259.2 | 0.122 | 20 | 34.5 | 0 | 355 | 5 805 |
| 6 | 136 800 | 136 800 | 3 296 | 230.6 | 264.1 | 0.140 | 19 | 34.5 | 0 | 373 | 7 159 |
| 7 | 159 600 | 159 600 | 3 292 | 230.9 | 269.1 | 0.157 | 9 | 34.6 | 0 | 386 | 8 611 |
| 8 | 182 400 | 182 400 | 3 293 | 230.8 | 273.1 | 0.171 | 12 | 34.5 | 0 | 397 | 10 339 |
| 9 | 205 200 | 205 200 | 3 294 | 230.7 | 277.0 | 0.182 | 12 | 34.8 | 0 | 408 | 12 200 |
| 10 | 228 000 | 228 000 | 3 298 | 230.4 | 279.9 | 0.191 | 19 | 34.6 | 11 | 423 | 13 940 |

- **Reescritos y fusiones** son los acumulados; **refrescados**, los de la ronda.

- **Corpus:** 760 000 vectores, 228 000 borrados y 228 000 insertados. Quedan 760 000.
- **Referencia, el lote de los que quedan:** 3 221 postings de 236.0 miembros. Cargas a 0.95: 9.40. A 0.99: 44.52, entre ef 160 y 320.

| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | cruza 0.99 entre ef | las tres |
|---|---|---|---|---|---|---|---|---|
| 4, más refresco | −0.0074 en ef 20 | no | 10.62, +13.0 % | sí | 54.39, +22.2 % | no | 160 y 320 | no |

Recall y cargas a igual ef:

| ef | referencia: recall | cargas | celda 4: recall | cargas |
|---|---|---|---|---|
| 10 | 0.9275 | 7.39 | 0.9224 | 7.63 |
| 20 | 0.9553 | 9.95 | 0.9479 | 10.15 |
| 40 | 0.9708 | 14.27 | 0.9643 | 14.51 |
| 80 | 0.9805 | 21.71 | 0.9764 | 21.96 |
| 160 | 0.9875 | 34.36 | 0.9850 | 34.88 |
| 320 | 0.9922 | 56.03 | 0.9904 | 56.54 |
| 640 | 0.9946 | 93.52 | 0.9944 | 94.19 |
| 1024 | 0.9962 | 133.91 | 0.9962 | 135.13 |

| celda | postings | miembros vivos por posting | miembros que ocupan sitio | bajo el mínimo | vacíos | aristas muertas | vectores con la mitad o más |
|---|---|---|---|---|---|---|---|
| 4, más refresco | 3 298 | 230.4 | 279.9 | 8 | 0 | 0.191 | 0 |

- **Limpieza:** 11 postings reescritos, con 1 858 miembros vivos copiados; 64 736 borrados soltados; 423 fusiones, que movieron 39 419 miembros vivos; 0 postings retirados sin ningún vivo; 202 splits tras una fusión.

