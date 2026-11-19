### La corrida

- **Dataset:** dbpedia-openai-1000k-angular.bin, 990 000 vectores, 5 000 queries, k = 10. Malla de ef: 10, 20, 40, 80, 160, 320, 640, 1 024.
- **Índice:** S = 236, P = 256, R = 64, split a T = 360, ef de inserción 160.
- **El lote del 100 %:** 4 195 postings de 236.0 miembros de media.
- **Ningún stop:** ningún vector borrado devuelto, cobertura completa, ninguna lista reparada hacia un borrado y ningún borrado tras la segunda fase, en todo lo medido.

### El criterio: con el 30 % borrado al azar

- **Corpus:** 990 000 vectores, 297 000 borrados. Quedan 693 000.
- **Referencia, el lote de los que quedan con S = 236:** 2 937 postings. Cargas a 0.95: 9.28. A 0.99: 44.29.

| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | las tres |
|---|---|---|---|---|---|---|---|
| referencia: la celda 4 de v6.0 | −0.0178 en ef 10 | no | 11.98, +29.1 % | no | 60.88, +37.5 % | no | no |
| la mitad o más, fusión de 118 (mía) | −0.0175 en ef 10 | no | 11.86, +27.8 % | no | 59.25, +33.8 % | no | no |
| el 25 % o más, fusión de 118 | −0.0086 en ef 10 | no | 10.18, +9.7 % | sí | 44.85, +1.3 % | sí | no |
| el 25 % o más, fusión de 177 | −0.0023 en ef 10 | sí | 9.53, +2.7 % | sí | 39.54, −10.7 % | sí | **sí** |
| una o más, fusión de 118 | −0.0060 en ef 10 | no, a menos del suelo de ruido | 9.97, +7.4 % | sí | 41.14, −7.1 % | sí | no |
| una o más, fusión de 177 | −0.0010 en ef 10 | sí | 9.39, +1.2 % | sí | 35.21, −20.5 % | sí | **sí** |

**La celda más barata que cumple, de las cuatro del encargo: el 25 % o más, fusión de 177.** Cumplen: el 25 % o más, fusión de 177; una o más, fusión de 177.

### Lo que cuesta la pasada

| celda | vectores reparados | de los vivos | cargas por reparación | cargas de la fase | endpoints por reparación | distancias exactas por reparación | aristas inversas |
|---|---|---|---|---|---|---|---|
| referencia: la celda 4 de v6.0 | 99 641 | 14.4 % | 30.9 | 3 078 233 | 10 329 | 64.0 | 6 746 |
| la mitad o más, fusión de 118 (mía) | 99 641 | 14.4 % | 39.6 | 3 948 363 | 19 174 | 64.0 | 6 224 |
| el 25 % o más, fusión de 118 | 412 369 | 59.5 % | 47.3 | 19 497 196 | 24 547 | 64.0 | 36 818 |
| el 25 % o más, fusión de 177 | 412 369 | 59.5 % | 47.3 | 19 497 196 | 24 547 | 64.0 | 36 818 |
| una o más, fusión de 118 | 580 809 | 83.8 % | 47.7 | 27 678 539 | 24 889 | 64.0 | 49 626 |
| una o más, fusión de 177 | 580 809 | 83.8 % | 47.7 | 27 678 539 | 24 889 | 64.0 | 49 626 |

| celda | postings reescritos | borrados soltados | aristas quitadas sin reparar | fusiones | no hechas por el tamaño | miembros movidos | splits tras una fusión | postings sin vivos retirados |
|---|---|---|---|---|---|---|---|---|
| referencia: la celda 4 de v6.0 | 2 076 | 181 165 | 0 | 743 | 0 | 63 045 | 61 | 0 |
| la mitad o más, fusión de 118 (mía) | 4 193 | 297 000 | 1 468 895 | 743 | 0 | 63 045 | 61 | 0 |
| el 25 % o más, fusión de 118 | 4 193 | 297 000 | 306 928 | 743 | 0 | 63 045 | 61 | 0 |
| el 25 % o más, fusión de 177 | 4 193 | 297 000 | 306 928 | 1 247 | 614 | 147 832 | 0 | 0 |
| una o más, fusión de 118 | 4 193 | 297 000 | 0 | 743 | 0 | 63 045 | 61 | 0 |
| una o más, fusión de 177 | 4 193 | 297 000 | 0 | 1 246 | 615 | 147 688 | 0 | 0 |

| celda | postings | tamaño medio, en vivos | miembros que ocupan sitio | por debajo de 118 | por debajo de 177 | aristas vivas por vector | aristas muertas |
|---|---|---|---|---|---|---|---|
| referencia: la celda 4 de v6.0 | 3 513 | 197.3 | 230.2 | 57 | 1 477 | 6.80 | 0.238 |
| la mitad o más, fusión de 118 (mía) | 3 513 | 197.3 | 197.3 | 57 | 1 477 | 6.82 | 0.000 |
| el 25 % o más, fusión de 118 | 3 513 | 197.3 | 197.3 | 57 | 1 477 | 8.53 | 0.000 |
| el 25 % o más, fusión de 177 | 2 948 | 235.1 | 235.1 | 118 | 612 | 8.53 | 0.000 |
| una o más, fusión de 118 | 3 513 | 197.3 | 197.3 | 57 | 1 477 | 9.10 | 0.000 |
| una o más, fusión de 177 | 2 949 | 235.0 | 235.0 | 118 | 613 | 9.10 | 0.000 |

### Contra su referencia de llenado

| referencia en lote | postings | cargas a 0.95 | sobre la de S = 236 | cargas a 0.99 | sobre la de S = 236 |
|---|---|---|---|---|---|
| S = 236, la del criterio | 2 937 | 9.28 | — | 44.29 | — |
| S = 197, de llenado | 3 518 | 9.86 | +6.3 % | 50.39 | +13.8 % |
| S = 235, de llenado | 2 949 | 9.30 | +0.2 % | 44.42 | +0.3 % |

| celda | su referencia | peor recall a igual ef contra ella | cargas a 0.95 | cargas a 0.99 | dentro de los dos umbrales |
|---|---|---|---|---|---|
| referencia: la celda 4 de v6.0 | S = 197 | −0.0140 | +21.5 % | +20.8 % | no |
| la mitad o más, fusión de 118 (mía) | S = 197 | −0.0138 | +20.2 % | +17.6 % | no |
| el 25 % o más, fusión de 118 | S = 197 | −0.0049 | +3.2 % | −11.0 % | sí |
| el 25 % o más, fusión de 177 | S = 235 | −0.0019 | +2.6 % | −11.0 % | sí |
| una o más, fusión de 118 | S = 197 | −0.0023 | +1.1 % | −18.4 % | sí |
| una o más, fusión de 177 | S = 235 | −0.0005 | +1.0 % | −20.7 % | sí |

### A igual ef

Lo que ve una búsqueda, en ef 160:

| | recall | cargas | vectores puntuados en el scan | endpoints puntuados |
|---|---|---|---|---|
| referencia, el lote con S = 236 | 0.9873 | 34.10 | 8 562 | 16 754 |
| referencia: la celda 4 de v6.0 | 0.9836 | 35.45 | 7 236 | 12 825 |
| la mitad o más, fusión de 118 (mía) | 0.9840 | 35.45 | 7 236 | 12 838 |
| el 25 % o más, fusión de 118 | 0.9880 | 36.12 | 7 380 | 15 325 |
| el 25 % o más, fusión de 177 | 0.9887 | 33.99 | 8 465 | 16 248 |
| una o más, fusión de 118 | 0.9889 | 36.26 | 7 411 | 16 067 |
| una o más, fusión de 177 | 0.9898 | 34.13 | 8 501 | 17 033 |

Recall a igual ef, frente a la referencia:

| celda | ef 10 | ef 20 | ef 40 | ef 80 | ef 160 | ef 320 | ef 640 | ef 1 024 |
|---|---|---|---|---|---|---|---|---|
| referencia: la celda 4 de v6.0 | −0.0178 | −0.0139 | −0.0104 | −0.0055 | −0.0036 | −0.0028 | −0.0014 | −0.0012 |
| la mitad o más, fusión de 118 (mía) | −0.0175 | −0.0137 | −0.0096 | −0.0050 | −0.0033 | −0.0025 | −0.0014 | −0.0010 |
| el 25 % o más, fusión de 118 | −0.0086 | −0.0047 | −0.0034 | +0.0009 | +0.0007 | +0.0002 | +0.0001 | −0.0001 |
| el 25 % o más, fusión de 177 | −0.0023 | −0.0017 | −0.0013 | +0.0011 | +0.0014 | +0.0005 | +0.0002 | −0.0000 |
| una o más, fusión de 118 | −0.0060 | −0.0033 | −0.0014 | +0.0019 | +0.0016 | +0.0010 | +0.0007 | +0.0003 |
| una o más, fusión de 177 | −0.0010 | −0.0002 | +0.0001 | +0.0023 | +0.0025 | +0.0012 | +0.0008 | +0.0004 |

Cargas a igual ef, frente a la referencia:

| celda | ef 10 | ef 20 | ef 40 | ef 80 | ef 160 | ef 320 | ef 640 | ef 1 024 |
|---|---|---|---|---|---|---|---|---|
| referencia: la celda 4 de v6.0 | +3.9 % | +3.8 % | +3.1 % | +3.9 % | +3.9 % | +4.2 % | +4.5 % | +4.7 % |
| la mitad o más, fusión de 118 (mía) | +3.6 % | +3.5 % | +3.2 % | +3.9 % | +3.9 % | +4.2 % | +4.6 % | +4.9 % |
| el 25 % o más, fusión de 118 | +3.2 % | +3.9 % | +3.5 % | +5.5 % | +5.9 % | +6.7 % | +7.1 % | +7.4 % |
| el 25 % o más, fusión de 177 | +1.7 % | +0.5 % | −0.6 % | −0.1 % | −0.3 % | −0.3 % | −0.6 % | −0.6 % |
| una o más, fusión de 118 | +2.7 % | +3.3 % | +3.9 % | +5.8 % | +6.3 % | +7.2 % | +7.7 % | +8.1 % |
| una o más, fusión de 177 | +1.3 % | +0.7 % | −0.1 % | +0.4 % | +0.1 % | +0.3 % | +0.0 % | +0.0 % |

### Churn, con la celda el 25 % o más, fusión de 177

- **Por qué esa celda:** es la más barata que cumple el criterio con el 30 % al azar.

| ronda | borrados | insertados | postings | tamaño medio | aristas muertas | splits de la ronda | cargas por insert | reparados en la ronda | cargas por reparación | fusiones, acumuladas | no hechas por el tamaño, acumuladas |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 22 800 | 22 800 | 3 180 | 239.0 | 0.000 | 295 | 35.5 | 11 880 | 25.9 | 336 | 514 |
| 2 | 45 600 | 45 600 | 3 178 | 239.1 | 0.000 | 67 | 34.8 | 12 474 | 25.9 | 405 | 1 009 |
| 3 | 68 400 | 68 400 | 3 187 | 238.5 | 0.000 | 48 | 35.1 | 13 201 | 26.3 | 444 | 1 506 |
| 4 | 91 200 | 91 200 | 3 192 | 238.1 | 0.000 | 37 | 35.2 | 13 540 | 26.5 | 476 | 2 010 |
| 5 | 114 000 | 114 000 | 3 192 | 238.1 | 0.000 | 33 | 34.8 | 14 178 | 26.8 | 509 | 2 510 |
| 6 | 136 800 | 136 800 | 3 193 | 238.0 | 0.000 | 32 | 34.9 | 14 803 | 27.1 | 540 | 3 001 |
| 7 | 159 600 | 159 600 | 3 192 | 238.1 | 0.000 | 26 | 35.0 | 15 295 | 27.3 | 567 | 3 491 |
| 8 | 182 400 | 182 400 | 3 191 | 238.2 | 0.000 | 36 | 34.9 | 16 167 | 27.6 | 604 | 3 972 |
| 9 | 205 200 | 205 200 | 3 192 | 238.1 | 0.000 | 35 | 35.2 | 16 391 | 28.3 | 638 | 4 453 |
| 10 | 228 000 | 228 000 | 3 191 | 238.2 | 0.000 | 34 | 35.0 | 17 007 | 28.1 | 673 | 4 934 |

Las aristas, ronda a ronda:

| ronda | reparados | de los vivos | aristas de un reparado, antes | después | aristas quitadas sin reparar, en la ronda | aristas por vector vivo, al acabar |
|---|---|---|---|---|---|---|
| 1 | 11 880 | 1.6 % | 4.35 | 4.20 | 177 405 | 8.62 |
| 2 | 12 474 | 1.6 % | 4.43 | 4.36 | 174 653 | 8.41 |
| 3 | 13 201 | 1.7 % | 4.48 | 4.50 | 170 478 | 8.21 |
| 4 | 13 540 | 1.8 % | 4.51 | 4.66 | 168 480 | 8.03 |
| 5 | 14 178 | 1.9 % | 4.53 | 4.80 | 164 259 | 7.86 |
| 6 | 14 803 | 1.9 % | 4.63 | 5.03 | 161 825 | 7.70 |
| 7 | 15 295 | 2.0 % | 4.61 | 5.10 | 160 216 | 7.55 |
| 8 | 16 167 | 2.1 % | 4.68 | 5.31 | 158 093 | 7.42 |
| 9 | 16 391 | 2.2 % | 4.70 | 5.44 | 154 937 | 7.29 |
| 10 | 17 007 | 2.2 % | 4.73 | 5.62 | 152 865 | 7.18 |

- **Corpus:** 760 000 vectores, 228 000 borrados y 228 000 insertados.
- **Referencia, el lote del conjunto final con S = 236:** 3 221 postings. Cargas a 0.95: 9.37. A 0.99: 44.39.

| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | las tres |
|---|---|---|---|---|---|---|---|
| el 25 % o más, fusión de 177 | −0.0071 en ef 10 | no | 10.51, +12.1 % | sí | 51.91, +16.9 % | no | no |

- **Al acabar:** 3 191 postings de 238.2 vivos de media, 62 por debajo de 118 y 481 por debajo de 177.
- **Reparación, en las diez rondas:** 144 936 vectores, 3 924 085 cargas, 27.1 por reparación.
- **Eliminación:** 32 435 postings reescritos, 228 000 borrados soltados, 1 643 211 aristas quitadas sin reparar, 673 fusiones, 4 934 no hechas por el tamaño, 84 016 miembros movidos, 0 splits tras una fusión.

| referencia en lote | postings | cargas a 0.95 | sobre la de S = 236 | cargas a 0.99 | sobre la de S = 236 |
|---|---|---|---|---|---|
| S = 236, la del criterio | 3 221 | 9.37 | — | 44.39 | — |
| S = 238, de llenado | 3 194 | 9.35 | −0.2 % | 44.36 | −0.1 % |

| celda | su referencia | peor recall a igual ef contra ella | cargas a 0.95 | cargas a 0.99 | dentro de los dos umbrales |
|---|---|---|---|---|---|
| el 25 % o más, fusión de 177 | S = 238 | −0.0071 | +12.4 % | +17.0 % | no |

Lo que ve una búsqueda, en ef 160:

| | recall | cargas | vectores puntuados en el scan | endpoints puntuados |
|---|---|---|---|---|
| referencia, el lote con S = 236 | 0.9876 | 34.36 | 8 627 | 17 092 |
| el 25 % o más, fusión de 177 | 0.9854 | 34.56 | 8 526 | 14 950 |

Recall a igual ef, frente a la referencia:

| celda | ef 10 | ef 20 | ef 40 | ef 80 | ef 160 | ef 320 | ef 640 | ef 1 024 |
|---|---|---|---|---|---|---|---|---|
| el 25 % o más, fusión de 177 | −0.0071 | −0.0069 | −0.0057 | −0.0032 | −0.0022 | −0.0013 | −0.0002 | −0.0001 |

Cargas a igual ef, frente a la referencia:

| celda | ef 10 | ef 20 | ef 40 | ef 80 | ef 160 | ef 320 | ef 640 | ef 1 024 |
|---|---|---|---|---|---|---|---|---|
| el 25 % o más, fusión de 177 | +3.5 % | +2.7 % | +1.3 % | +0.4 % | +0.6 % | +0.0 % | −0.2 % | −0.3 % |

