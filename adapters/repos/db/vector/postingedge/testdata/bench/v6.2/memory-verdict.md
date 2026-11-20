### Qué corrió

- **Dataset:** dbpedia-openai-1000k-angular.bin, 990 000 vectores y 5 000 queries, k = 10.
- **Umbral de split** 360, **ef de inserción** 160, y la fusión de la ganadora de v6.1.
- **El churn: diez rondas del 3 %:** 760 000 vectores de partida, 228 000 borrados y 228 000 insertados, en 105 minutos.
- **El 30 % de una vez:** 990 000 vectores de partida, 297 000 borrados, en 35 minutos.

### El churn: diez rondas del 3 %

**Contra el lote de S = 236 de los 760 000 vectores vivos:** 3 221 postings; cargas a 0.95 9.41 y a 0.99 44.52.

| celda | peor recall a igual ef | entra | cargas a 0.95 | entra | cargas a 0.99 | entra | las tres |
|---|---|---|---|---|---|---|---|
| la de v6.1, repetida (mía) | −0.0069 en ef 20 | no | 10.53, +12.0 % | sí | 51.54, +15.8 % | no | no |
| A: umbral acumulado | −0.0013 en ef 20 | sí | 9.75, +3.7 % | sí | 43.96, −1.3 % | sí | **sí** |
| B: sustitución local | −0.0004 en ef 20 | sí | 9.66, +2.7 % | sí | 45.31, +1.8 % | sí | **sí** |
| C: sustitución con red | −0.0005 en ef 40 | sí | 9.64, +2.4 % | sí | 45.79, +2.9 % | sí | **sí** |

**Contra su referencia de llenado,** el lote con el tamaño medio de posting que deja cada celda. Sin veredicto.

| celda | postings | vivos por posting | lote de | peor recall a igual ef | cargas a 0.95 | cargas a 0.99 | cargas dentro |
|---|---|---|---|---|---|---|---|
| la de v6.1, repetida (mía) | 3 189 | 238.3 | S = 238 | −0.0072 | +12.6 % | +16.2 % | no |
| A: umbral acumulado | 3 193 | 238.0 | S = 238 | −0.0017 | +4.2 % | −0.9 % | sí |
| B: sustitución local | 3 191 | 238.2 | S = 238 | −0.0007 | +3.3 % | +2.1 % | sí |
| C: sustitución con red | 3 204 | 237.2 | S = 237 | −0.0006 | +2.6 % | +2.8 % | sí |

**Coste por vector borrado,** sobre los 228 000 borrados de la etapa.

| celda | listas reparadas por búsqueda | cargas de búsqueda | postings leídos y escritos por la fase 2 | distancias exactas | de ellas, de aristas inversas | distancias entre códigos | del ensayo, aparte |
|---|---|---|---|---|---|---|---|
| la de v6.1, repetida (mía) | 0.63 | 17.2 | 0.14 | 54.2 | 13.6 | 0.0 | 0.0 |
| A: umbral acumulado | 1.87 | 68.2 | 0.14 | 203.6 | 84.2 | 0.0 | 0.0 |
| B: sustitución local | 0.00 | 0.0 | 0.14 | 0.0 | 0.0 | 62.8 | 0.0 |
| C: sustitución con red | 0.08 | 1.5 | 0.14 | 6.3 | 0.9 | 62.8 | 62.8 |

| celda | listas reparadas | cargas | por reparación | aristas sustituidas | quitadas sin sustituto | con sustituto, de las muertas | fusiones |
|---|---|---|---|---|---|---|---|
| la de v6.1, repetida (mía) | 144 778 | 3 923 765 | 27.1 | 0 | 1 643 064 | 0.0 % | 681 |
| A: umbral acumulado | 425 379 | 15 549 454 | 36.6 | 0 | 1 344 229 | 0.0 % | 686 |
| B: sustitución local | 0 | 0 | — | 1 925 329 | 114 249 | 94.4 % | 677 |
| C: sustitución con red | 19 222 | 337 487 | 17.6 | 1 925 753 | 93 430 | 95.4 % | 642 |

- **Las aristas muertas que una reparación quita de la lista que construye no están en ninguna de las dos columnas:** se cuentan las que llegan a la fase 2.

**Aristas por vector vivo después de la pasada de cada ronda,** y entre paréntesis cuántas son sustitutos.

| ronda | la de v6.1, repetida (mía) | A: umbral acumulado | B: sustitución local | C: sustitución con red |
|---|---|---|---|---|
| 1 | 8.62 | 8.62 | 8.84 (0.24) | 8.85 (0.24) |
| 2 | 8.41 | 8.43 | 8.84 (0.43) | 8.84 (0.43) |
| 3 | 8.21 | 8.29 | 8.84 (0.59) | 8.84 (0.58) |
| 4 | 8.03 | 8.19 | 8.84 (0.72) | 8.84 (0.72) |
| 5 | 7.86 | 8.13 | 8.84 (0.84) | 8.85 (0.84) |
| 6 | 7.70 | 8.10 | 8.84 (0.95) | 8.85 (0.95) |
| 7 | 7.55 | 8.09 | 8.84 (1.05) | 8.85 (1.05) |
| 8 | 7.42 | 8.09 | 8.84 (1.15) | 8.85 (1.14) |
| 9 | 7.29 | 8.10 | 8.84 (1.23) | 8.86 (1.23) |
| 10 | 7.18 | 8.11 | 8.84 (1.31) | 8.86 (1.31) |

**la de v6.1, repetida (mía), por ronda:**

| ronda | listas reparadas | cargas | aristas sustituidas | quitadas sin sustituto | postings de la fase 2 | fusiones | el contador: media por vector vivo | vectores con el contador por encima de cero |
|---|---|---|---|---|---|---|---|---|
| 1 | 11 876 | 307 979 | 0 | 177 403 | 3 519 | 345 | 0.233 | 154 450 |
| 2 | 12 492 | 323 209 | 0 | 174 637 | 3 231 | 57 | 0.454 | 260 642 |
| 3 | 13 183 | 347 673 | 0 | 170 424 | 3 224 | 39 | 0.660 | 333 347 |
| 4 | 13 543 | 358 572 | 0 | 168 511 | 3 224 | 41 | 0.855 | 383 393 |
| 5 | 14 202 | 381 936 | 0 | 164 266 | 3 223 | 43 | 1.036 | 417 769 |
| 6 | 14 797 | 402 257 | 0 | 161 834 | 3 219 | 38 | 1.203 | 440 735 |
| 7 | 15 273 | 417 622 | 0 | 160 179 | 3 213 | 33 | 1.359 | 456 589 |
| 8 | 16 173 | 445 820 | 0 | 157 978 | 3 218 | 29 | 1.503 | 467 368 |
| 9 | 16 299 | 462 213 | 0 | 155 028 | 3 215 | 26 | 1.635 | 474 150 |
| 10 | 16 940 | 476 484 | 0 | 152 804 | 3 219 | 30 | 1.753 | 478 430 |

**A: umbral acumulado, por ronda:**

| ronda | listas reparadas | cargas | aristas sustituidas | quitadas sin sustituto | postings de la fase 2 | fusiones | el contador: media por vector vivo | vectores con el contador por encima de cero |
|---|---|---|---|---|---|---|---|---|
| 1 | 11 877 | 308 088 | 0 | 177 396 | 3 518 | 338 | 0.233 | 154 446 |
| 2 | 19 720 | 593 031 | 0 | 164 750 | 3 251 | 67 | 0.427 | 253 387 |
| 3 | 29 091 | 974 389 | 0 | 149 801 | 3 240 | 49 | 0.572 | 312 240 |
| 4 | 38 009 | 1 345 659 | 0 | 138 272 | 3 233 | 39 | 0.671 | 343 822 |
| 5 | 45 792 | 1 681 939 | 0 | 127 431 | 3 229 | 34 | 0.728 | 358 050 |
| 6 | 51 344 | 1 914 299 | 0 | 121 252 | 3 225 | 33 | 0.757 | 362 109 |
| 7 | 55 159 | 2 092 803 | 0 | 118 018 | 3 221 | 27 | 0.767 | 361 435 |
| 8 | 57 807 | 2 195 586 | 0 | 116 356 | 3 226 | 31 | 0.767 | 359 294 |
| 9 | 58 152 | 2 222 863 | 0 | 115 465 | 3 224 | 32 | 0.764 | 357 514 |
| 10 | 58 428 | 2 220 797 | 0 | 115 488 | 3 229 | 36 | 0.759 | 356 329 |

**B: sustitución local, por ronda:**

| ronda | listas reparadas | cargas | aristas sustituidas | quitadas sin sustituto | postings de la fase 2 | fusiones | el contador: media por vector vivo | vectores con el contador por encima de cero |
|---|---|---|---|---|---|---|---|---|
| 1 | 0 | 0 | 182 375 | 11 402 | 3 519 | 340 | 0.015 | 11 039 |
| 2 | 0 | 0 | 185 709 | 11 236 | 3 244 | 68 | 0.029 | 20 818 |
| 3 | 0 | 0 | 187 214 | 11 251 | 3 234 | 40 | 0.043 | 29 759 |
| 4 | 0 | 0 | 189 482 | 11 525 | 3 226 | 37 | 0.057 | 38 015 |
| 5 | 0 | 0 | 190 720 | 11 167 | 3 227 | 37 | 0.070 | 45 500 |
| 6 | 0 | 0 | 193 671 | 11 179 | 3 217 | 25 | 0.082 | 52 343 |
| 7 | 0 | 0 | 195 889 | 11 325 | 3 215 | 26 | 0.095 | 58 829 |
| 8 | 0 | 0 | 199 182 | 11 568 | 3 226 | 36 | 0.107 | 64 965 |
| 9 | 0 | 0 | 200 082 | 11 536 | 3 224 | 36 | 0.118 | 70 662 |
| 10 | 0 | 0 | 201 005 | 12 060 | 3 223 | 32 | 0.130 | 76 019 |

**C: sustitución con red, por ronda:**

| ronda | listas reparadas | cargas | aristas sustituidas | quitadas sin sustituto | postings de la fase 2 | fusiones | el contador: media por vector vivo | vectores con el contador por encima de cero |
|---|---|---|---|---|---|---|---|---|
| 1 | 1 497 | 25 725 | 182 336 | 9 772 | 3 519 | 335 | 0.013 | 9 499 |
| 2 | 1 410 | 22 992 | 185 626 | 9 699 | 3 246 | 57 | 0.025 | 17 946 |
| 3 | 1 746 | 30 753 | 187 185 | 9 299 | 3 237 | 41 | 0.036 | 25 242 |
| 4 | 1 810 | 31 386 | 189 423 | 9 566 | 3 233 | 29 | 0.046 | 32 019 |
| 5 | 1 802 | 32 050 | 190 707 | 9 206 | 3 234 | 36 | 0.056 | 38 136 |
| 6 | 2 042 | 34 260 | 193 677 | 8 959 | 3 225 | 29 | 0.065 | 43 399 |
| 7 | 2 049 | 37 188 | 196 058 | 9 084 | 3 225 | 27 | 0.073 | 48 419 |
| 8 | 2 134 | 38 295 | 199 214 | 9 314 | 3 232 | 32 | 0.081 | 53 182 |
| 9 | 2 200 | 40 622 | 200 280 | 9 154 | 3 227 | 30 | 0.088 | 57 500 |
| 10 | 2 532 | 44 216 | 201 247 | 9 377 | 3 230 | 26 | 0.095 | 61 349 |

**Recall de arista al final,** el de v2.x, sobre una muestra de vectores vivos, la misma en todas las celdas y en el lote.

| | vectores | recall de arista | recall de postings | aristas por vector | sustitutos en la lista | de ellos, aristas verdaderas |
|---|---|---|---|---|---|---|
| el lote de S = 236 | 2 000 | 0.9680 | 0.9510 | 8.70 | — | — |
| el lote de S = 238, de llenado | 2 000 | 0.9681 | 0.9511 | 8.68 | — | — |
| el lote de S = 237, de llenado | 2 000 | 0.9680 | 0.9510 | 8.69 | — | — |
| **la de v6.1, repetida (mía)** | 2 000 | 0.7500 | 0.7968 | 7.18 | — | — |
| &nbsp;&nbsp;insertados en el churn | 609 | 0.8431 | 0.8612 | 8.37 | — | — |
| &nbsp;&nbsp;reparados por búsqueda | 220 | 0.8630 | 0.8993 | 5.18 | — | — |
| &nbsp;&nbsp;ni reparados ni con sustituto | 1 171 | 0.6895 | 0.7519 | 6.94 | — | — |
| **A: umbral acumulado** | 2 000 | 0.8438 | 0.8739 | 8.08 | — | — |
| &nbsp;&nbsp;insertados en el churn | 609 | 0.8794 | 0.8956 | 8.81 | — | — |
| &nbsp;&nbsp;reparados por búsqueda | 714 | 0.8966 | 0.9170 | 8.98 | — | — |
| &nbsp;&nbsp;ni reparados ni con sustituto | 677 | 0.7411 | 0.7969 | 6.47 | — | — |
| **B: sustitución local** | 2 000 | 0.7833 | 0.8358 | 8.81 | 2 491 | 312, un 12.5 % |
| &nbsp;&nbsp;insertados en el churn | 609 | 0.8594 | 0.8784 | 9.47 | 500 | 37, un 7.4 % |
| &nbsp;&nbsp;con algún sustituto | 870 | 0.7424 | 0.8139 | 10.44 | 1 991 | 275, un 13.8 % |
| &nbsp;&nbsp;ni reparados ni con sustituto | 521 | 0.7624 | 0.8235 | 5.33 | — | — |
| **C: sustitución con red** | 2 000 | 0.7833 | 0.8352 | 8.86 | 2 498 | 302, un 12.1 % |
| &nbsp;&nbsp;insertados en el churn | 609 | 0.8581 | 0.8772 | 9.53 | 513 | 37, un 7.2 % |
| &nbsp;&nbsp;con algún sustituto | 864 | 0.7408 | 0.8137 | 10.46 | 1 985 | 265, un 13.4 % |
| &nbsp;&nbsp;reparados por búsqueda | 27 | 0.9021 | 0.9474 | 4.93 | — | — |
| &nbsp;&nbsp;ni reparados ni con sustituto | 500 | 0.7623 | 0.8189 | 5.50 | — | — |

### La celda más barata que cumple

**B: sustitución local.**

| celda | cumple en el churn | cargas de búsqueda por borrado | distancias exactas por borrado |
|---|---|---|---|
| A: umbral acumulado | sí | 68.2 | 203.6 |
| B: sustitución local | sí | 0.0 | 0.0 |
| C: sustitución con red | sí | 1.5 | 6.3 |

### El 30 % de una vez

**Contra el lote de S = 236 de los 693 000 vectores vivos:** 2 937 postings; cargas a 0.95 9.31 y a 0.99 44.47.

| celda | peor recall a igual ef | entra | cargas a 0.95 | entra | cargas a 0.99 | entra | las tres |
|---|---|---|---|---|---|---|---|
| la de v6.1, repetida (mía) | −0.0020 en ef 10 | sí | 9.53, +2.3 % | sí | 39.75, −10.6 % | sí | **sí** |
| B: sustitución local | −0.0038 en ef 10 | sí | 9.70, +4.2 % | sí | 46.33, +4.2 % | sí | **sí** |
| C: sustitución con red | −0.0041 en ef 10 | sí, a menos del suelo de ruido | 9.74, +4.6 % | sí | 45.88, +3.2 % | sí | **sí** |

**Contra su referencia de llenado,** el lote con el tamaño medio de posting que deja cada celda. Sin veredicto.

| celda | postings | vivos por posting | lote de | peor recall a igual ef | cargas a 0.95 | cargas a 0.99 | cargas dentro |
|---|---|---|---|---|---|---|---|
| la de v6.1, repetida (mía) | 2 948 | 235.1 | S = 235 | −0.0015 | +1.8 % | −10.6 % | sí |
| B: sustitución local | 2 948 | 235.1 | S = 235 | −0.0033 | +3.7 % | +4.2 % | sí |
| C: sustitución con red | 2 948 | 235.1 | S = 235 | −0.0036 | +4.0 % | +3.2 % | sí |

**Coste por vector borrado,** sobre los 297 000 borrados de la etapa.

| celda | listas reparadas por búsqueda | cargas de búsqueda | postings leídos y escritos por la fase 2 | distancias exactas | de ellas, de aristas inversas | distancias entre códigos | del ensayo, aparte |
|---|---|---|---|---|---|---|---|
| la de v6.1, repetida (mía) | 1.39 | 65.6 | 0.01 | 165.0 | 76.1 | 0.0 | 0.0 |
| B: sustitución local | 0.00 | 0.0 | 0.01 | 0.0 | 0.0 | 31.2 | 0.0 |
| C: sustitución con red | 0.09 | 2.2 | 0.01 | 7.5 | 1.4 | 31.0 | 31.2 |

| celda | listas reparadas | cargas | por reparación | aristas sustituidas | quitadas sin sustituto | con sustituto, de las muertas | fusiones |
|---|---|---|---|---|---|---|---|
| la de v6.1, repetida (mía) | 412 369 | 19 497 196 | 47.3 | 0 | 306 928 | 0.0 % | 1 247 |
| B: sustitución local | 0 | 0 | — | 1 704 723 | 160 959 | 91.4 % | 1 247 |
| C: sustitución con red | 27 982 | 662 008 | 23.7 | 1 686 910 | 108 452 | 94.0 % | 1 247 |

- **Las aristas muertas que una reparación quita de la lista que construye no están en ninguna de las dos columnas:** se cuentan las que llegan a la fase 2.

**Recall de arista al final,** el de v2.x, sobre una muestra de vectores vivos, la misma en todas las celdas y en el lote.

| | vectores | recall de arista | recall de postings | aristas por vector | sustitutos en la lista | de ellos, aristas verdaderas |
|---|---|---|---|---|---|---|
| el lote de S = 236 | 2 000 | 0.9658 | 0.9494 | 8.70 | — | — |
| el lote de S = 235, de llenado | 2 000 | 0.9658 | 0.9491 | 8.70 | — | — |
| **la de v6.1, repetida (mía)** | 2 000 | 0.9132 | 0.9242 | 8.35 | — | — |
| &nbsp;&nbsp;reparados por búsqueda | 1 211 | 0.9860 | 0.9812 | 9.72 | — | — |
| &nbsp;&nbsp;ni reparados ni con sustituto | 789 | 0.7784 | 0.8155 | 6.26 | — | — |
| **B: sustitución local** | 2 000 | 0.7106 | 0.7755 | 8.57 | 4 981 | 741, un 14.9 % |
| &nbsp;&nbsp;con algún sustituto | 1 605 | 0.7125 | 0.7788 | 10.10 | 4 981 | 741, un 14.9 % |
| &nbsp;&nbsp;ni reparados ni con sustituto | 395 | 0.6860 | 0.7293 | 2.32 | — | — |
| **C: sustitución con red** | 2 000 | 0.7210 | 0.7807 | 8.65 | 4 931 | 727, un 14.7 % |
| &nbsp;&nbsp;con algún sustituto | 1 571 | 0.7148 | 0.7797 | 10.23 | 4 931 | 727, un 14.7 % |
| &nbsp;&nbsp;reparados por búsqueda | 87 | 0.9851 | 0.9665 | 5.05 | — | — |
| &nbsp;&nbsp;ni reparados ni con sustituto | 342 | 0.7135 | 0.7433 | 2.30 | — | — |

### El churn y el 30 % de una vez, celda a celda

| celda | cumple en el churn | entra con el 30 % de una vez |
|---|---|---|
| la de v6.1, repetida (mía) | no | sí |
| A: umbral acumulado | sí | sí: es la ganadora de v6.1, con los contadores a cero |
| B: sustitución local | sí | sí |
| C: sustitución con red | sí | sí |

**Veredicto:** B: sustitución local.
