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

