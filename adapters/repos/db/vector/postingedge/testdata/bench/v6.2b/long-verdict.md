### Qué corrió

- **Dataset:** dbpedia-openai-1000k-angular.bin, 990 000 vectores y 5 000 queries, k = 10.
- **Umbral de split** 360, **ef de inserción** 160, y la fusión de la ganadora de v6.1.
- **El churn largo:** una base de 520 000 vectores; 468 000 borrados y 468 000 insertados en las rondas previstas; 30 rondas hechas por la celda que más lleva; 79 minutos.
- **El borrado por regiones:** 990 000 vectores de partida, 297 259 borrados en 22 regiones, 1 380 postings; 8 minutos.

### El churn largo: el criterio en cada ronda medida

- **Ronda 10:** el lote de S = 236 de 520 000 vectores vivos, en 2 204 postings: 8.80 cargas a 0.95 y 36.07 a 0.99.
- **Ronda 20:** el lote de S = 236 de 520 000 vectores vivos, en 2 204 postings: 8.79 cargas a 0.95 y 34.69 a 0.99.
- **Ronda 30:** el lote de S = 236 de 520 000 vectores vivos, en 2 204 postings: 8.79 cargas a 0.95 y 36.48 a 0.99.

| ronda | celda | peor recall a igual ef | entra | cargas a 0.95 | entra | cargas a 0.99 | entra | las tres |
|---|---|---|---|---|---|---|---|---|
| 10 | B: sustitución local | −0.0024 en ef 20 | sí | 9.11, +3.5 % | sí | 36.44, +1.0 % | sí | **sí** |
| 10 | C: sustitución con red | −0.0020 en ef 20 | sí | 9.07, +3.1 % | sí | 36.07, −0.0 % | sí | **sí** |
| 20 | B: sustitución local | −0.0020 en ef 20 | sí | 9.01, +2.4 % | sí | 36.19, +4.3 % | sí | **sí** |
| 20 | C: sustitución con red | −0.0021 en ef 10 | sí | 9.04, +2.7 % | sí | 35.53, +2.4 % | sí | **sí** |
| 30 | B: sustitución local | −0.0011 en ef 20 | sí | 8.93, +1.6 % | sí | 36.00, −1.3 % | sí | **sí** |
| 30 | C: sustitución con red | −0.0008 en ef 20 | sí | 8.97, +2.1 % | sí | 35.58, −2.5 % | sí | **sí** |

### La regla

B se sostiene si en la última ronda medida sigue dentro del criterio. Si falla y C cumple, la red de búsqueda pasa a ser obligatoria. La última medida es la de la ronda 30.

**B se sostiene:** cumple las tres cantidades en la ronda 30.
C también cumple.

### El churn largo: lo que cuesta, por vector borrado hasta cada ronda medida

| ronda | celda | listas reparadas por búsqueda | cargas de búsqueda | postings de la fase 2 | distancias exactas | distancias entre códigos | del ensayo, aparte | aristas muertas con sustituto |
|---|---|---|---|---|---|---|---|---|
| 10 | B: sustitución local | 0.00 | 0.0 | 0.14 | 0.0 | 59.6 | 0.0 | 93.8 % |
| 10 | C: sustitución con red | 0.09 | 1.6 | 0.14 | 7.1 | 59.7 | 59.7 | 94.9 % |
| 20 | B: sustitución local | 0.00 | 0.0 | 0.14 | 0.0 | 63.1 | 0.0 | 93.8 % |
| 20 | C: sustitución con red | 0.12 | 2.1 | 0.14 | 9.2 | 63.2 | 63.2 | 95.1 % |
| 30 | B: sustitución local | 0.00 | 0.0 | 0.14 | 0.0 | 65.7 | 0.0 | 93.6 % |
| 30 | C: sustitución con red | 0.14 | 2.6 | 0.14 | 11.0 | 66.0 | 66.1 | 95.1 % |

### El churn largo, ronda a ronda

**Por ronda: aristas por vector vivo después de la pasada, y la fracción de ellas que son sustitutos.**

| ronda | B: sustitución local: aristas por vector | sustitutos | C: sustitución con red: aristas por vector | sustitutos |
|---|---|---|---|---|
| 1 | 8.65 | 2.7 % | 8.65 | 2.7 % |
| 2 | 8.65 | 4.8 % | 8.65 | 4.8 % |
| 3 | 8.64 | 6.5 % | 8.65 | 6.5 % |
| 4 | 8.64 | 8.1 % | 8.65 | 8.0 % |
| 5 | 8.63 | 9.4 % | 8.65 | 9.4 % |
| 6 | 8.63 | 10.7 % | 8.65 | 10.6 % |
| 7 | 8.63 | 11.8 % | 8.65 | 11.7 % |
| 8 | 8.63 | 12.8 % | 8.65 | 12.8 % |
| 9 | 8.62 | 13.8 % | 8.65 | 13.7 % |
| 10 | 8.62 | 14.7 % | 8.65 | 14.6 % |
| 11 | 8.62 | 15.5 % | 8.65 | 15.4 % |
| 12 | 8.62 | 16.3 % | 8.66 | 16.1 % |
| 13 | 8.62 | 17.0 % | 8.66 | 16.8 % |
| 14 | 8.62 | 17.7 % | 8.67 | 17.5 % |
| 15 | 8.62 | 18.4 % | 8.67 | 18.1 % |
| 16 | 8.62 | 19.0 % | 8.67 | 18.7 % |
| 17 | 8.62 | 19.5 % | 8.67 | 19.2 % |
| 18 | 8.61 | 20.0 % | 8.67 | 19.7 % |
| 19 | 8.61 | 20.6 % | 8.68 | 20.2 % |
| 20 | 8.61 | 21.0 % | 8.68 | 20.6 % |
| 21 | 8.62 | 21.4 % | 8.69 | 21.0 % |
| 22 | 8.62 | 21.8 % | 8.69 | 21.3 % |
| 23 | 8.62 | 22.1 % | 8.70 | 21.6 % |
| 24 | 8.62 | 22.4 % | 8.70 | 21.9 % |
| 25 | 8.62 | 22.7 % | 8.70 | 22.2 % |
| 26 | 8.62 | 22.9 % | 8.71 | 22.4 % |
| 27 | 8.62 | 23.2 % | 8.71 | 22.6 % |
| 28 | 8.62 | 23.3 % | 8.72 | 22.7 % |
| 29 | 8.62 | 23.4 % | 8.72 | 22.8 % |
| 30 | 8.62 | 23.5 % | 8.72 | 22.8 % |

**B: sustitución local, por ronda:**

| ronda | listas reparadas | cargas | aristas sustituidas | quitadas sin sustituto | postings de la fase 2 | fusiones | el contador: media por vector vivo | vectores con el contador por encima de cero |
|---|---|---|---|---|---|---|---|---|
| 1 | 0 | 0 | 121 601 | 8 181 | 2 396 | 238 | 0.016 | 7 904 |
| 2 | 0 | 0 | 120 966 | 8 380 | 2 203 | 45 | 0.031 | 15 282 |
| 3 | 0 | 0 | 122 708 | 8 554 | 2 187 | 24 | 0.047 | 22 050 |
| 4 | 0 | 0 | 126 214 | 8 200 | 2 196 | 35 | 0.061 | 27 940 |
| 5 | 0 | 0 | 126 375 | 8 508 | 2 189 | 27 | 0.076 | 33 552 |
| 6 | 0 | 0 | 129 882 | 8 252 | 2 189 | 22 | 0.089 | 38 608 |
| 7 | 0 | 0 | 129 725 | 8 365 | 2 184 | 20 | 0.102 | 43 274 |
| 8 | 0 | 0 | 131 273 | 8 511 | 2 180 | 15 | 0.115 | 47 657 |
| 9 | 0 | 0 | 133 020 | 8 084 | 2 186 | 22 | 0.127 | 51 405 |
| 10 | 0 | 0 | 132 343 | 8 528 | 2 184 | 19 | 0.139 | 55 178 |
| 11 | 0 | 0 | 135 027 | 8 950 | 2 183 | 15 | 0.151 | 59 041 |
| 12 | 0 | 0 | 136 697 | 9 066 | 2 181 | 17 | 0.164 | 62 617 |
| 13 | 0 | 0 | 138 435 | 9 063 | 2 183 | 18 | 0.175 | 65 900 |
| 14 | 0 | 0 | 139 467 | 8 769 | 2 182 | 21 | 0.186 | 68 727 |
| 15 | 0 | 0 | 139 751 | 9 324 | 2 186 | 24 | 0.197 | 71 582 |
| 16 | 0 | 0 | 142 494 | 9 233 | 2 180 | 16 | 0.208 | 74 233 |
| 17 | 0 | 0 | 143 161 | 9 508 | 2 178 | 17 | 0.219 | 77 044 |
| 18 | 0 | 0 | 144 229 | 9 882 | 2 182 | 19 | 0.230 | 79 856 |
| 19 | 0 | 0 | 146 404 | 9 793 | 2 182 | 17 | 0.240 | 82 393 |
| 20 | 0 | 0 | 146 600 | 9 567 | 2 184 | 20 | 0.250 | 84 323 |
| 21 | 0 | 0 | 149 400 | 9 716 | 2 185 | 19 | 0.259 | 86 441 |
| 22 | 0 | 0 | 147 237 | 10 177 | 2 179 | 15 | 0.268 | 88 474 |
| 23 | 0 | 0 | 147 643 | 10 358 | 2 185 | 24 | 0.278 | 90 653 |
| 24 | 0 | 0 | 150 182 | 10 320 | 2 181 | 17 | 0.286 | 92 304 |
| 25 | 0 | 0 | 153 297 | 10 883 | 2 176 | 14 | 0.296 | 94 269 |
| 26 | 0 | 0 | 152 420 | 11 208 | 2 175 | 14 | 0.306 | 96 289 |
| 27 | 0 | 0 | 153 540 | 11 522 | 2 170 | 9 | 0.316 | 98 326 |
| 28 | 0 | 0 | 152 438 | 11 654 | 2 169 | 7 | 0.326 | 100 191 |
| 29 | 0 | 0 | 154 888 | 11 178 | 2 175 | 15 | 0.334 | 101 503 |
| 30 | 0 | 0 | 153 909 | 11 794 | 2 174 | 14 | 0.343 | 103 245 |

**C: sustitución con red, por ronda:**

| ronda | listas reparadas | cargas | aristas sustituidas | quitadas sin sustituto | postings de la fase 2 | fusiones | el contador: media por vector vivo | vectores con el contador por encima de cero |
|---|---|---|---|---|---|---|---|---|
| 1 | 971 | 17 532 | 121 560 | 7 132 | 2 396 | 235 | 0.014 | 6 928 |
| 2 | 1 128 | 18 318 | 120 938 | 7 175 | 2 208 | 48 | 0.027 | 13 237 |
| 3 | 1 319 | 21 917 | 122 687 | 7 113 | 2 192 | 25 | 0.039 | 18 821 |
| 4 | 1 409 | 24 126 | 126 198 | 6 677 | 2 194 | 28 | 0.050 | 23 504 |
| 5 | 1 475 | 25 173 | 126 317 | 6 914 | 2 191 | 33 | 0.060 | 27 941 |
| 6 | 1 448 | 24 579 | 129 896 | 6 645 | 2 185 | 20 | 0.069 | 31 903 |
| 7 | 1 638 | 28 841 | 129 688 | 6 629 | 2 184 | 22 | 0.078 | 35 423 |
| 8 | 1 656 | 27 979 | 131 397 | 6 636 | 2 180 | 18 | 0.086 | 38 732 |
| 9 | 1 810 | 31 959 | 133 003 | 6 299 | 2 186 | 22 | 0.093 | 41 443 |
| 10 | 1 880 | 33 678 | 132 223 | 6 650 | 2 183 | 20 | 0.100 | 44 185 |
| 11 | 2 018 | 34 705 | 135 151 | 6 811 | 2 181 | 18 | 0.107 | 46 955 |
| 12 | 2 172 | 37 952 | 136 793 | 6 833 | 2 182 | 22 | 0.113 | 49 294 |
| 13 | 2 034 | 35 605 | 138 478 | 6 927 | 2 176 | 21 | 0.119 | 51 632 |
| 14 | 2 018 | 37 903 | 139 429 | 6 704 | 2 178 | 21 | 0.124 | 53 669 |
| 15 | 2 238 | 40 680 | 139 648 | 7 051 | 2 175 | 17 | 0.129 | 55 712 |
| 16 | 2 319 | 42 744 | 142 402 | 6 852 | 2 174 | 15 | 0.134 | 57 442 |
| 17 | 2 239 | 41 556 | 143 036 | 7 272 | 2 177 | 15 | 0.139 | 59 371 |
| 18 | 2 462 | 45 834 | 144 162 | 7 240 | 2 178 | 15 | 0.143 | 61 159 |
| 19 | 2 400 | 43 972 | 146 579 | 7 061 | 2 178 | 15 | 0.147 | 62 769 |
| 20 | 2 492 | 46 895 | 146 603 | 6 962 | 2 185 | 19 | 0.151 | 64 024 |
| 21 | 2 532 | 47 507 | 149 366 | 7 181 | 2 186 | 17 | 0.154 | 65 428 |
| 22 | 2 541 | 46 813 | 147 168 | 7 457 | 2 182 | 15 | 0.158 | 66 820 |
| 23 | 2 617 | 49 699 | 147 461 | 7 463 | 2 185 | 18 | 0.162 | 68 164 |
| 24 | 2 703 | 51 006 | 150 099 | 7 391 | 2 182 | 16 | 0.165 | 69 108 |
| 25 | 2 914 | 57 938 | 153 050 | 7 779 | 2 179 | 15 | 0.168 | 70 274 |
| 26 | 2 857 | 55 167 | 152 295 | 7 918 | 2 179 | 13 | 0.171 | 71 564 |
| 27 | 3 063 | 58 458 | 153 594 | 7 901 | 2 181 | 12 | 0.174 | 72 586 |
| 28 | 3 124 | 61 249 | 152 015 | 8 166 | 2 180 | 16 | 0.177 | 73 754 |
| 29 | 3 051 | 59 953 | 154 479 | 7 806 | 2 181 | 13 | 0.179 | 74 451 |
| 30 | 3 337 | 64 572 | 153 689 | 8 108 | 2 180 | 10 | 0.181 | 75 322 |

### El churn largo: recall de arista en cada ronda medida

**Ronda 10:**

| | vectores | recall de arista | recall de postings | aristas por vector | sustitutos en la lista | de ellos, aristas verdaderas |
|---|---|---|---|---|---|---|
| el lote de S = 236 | 2 000 | 0.9706 | 0.9553 | 8.71 | — | — |
| **B: sustitución local** | 2 000 | 0.7823 | 0.8407 | 8.57 | 2 544 | 302, un 11.9 % |
| &nbsp;&nbsp;insertados en el churn | 589 | 0.8669 | 0.8992 | 9.30 | 492 | 52, un 10.6 % |
| &nbsp;&nbsp;con algún sustituto | 885 | 0.7416 | 0.8122 | 10.27 | 2 052 | 250, un 12.2 % |
| &nbsp;&nbsp;ni reparados ni con sustituto | 526 | 0.7503 | 0.8200 | 4.87 | — | — |
| **C: sustitución con red** | 2 000 | 0.7869 | 0.8425 | 8.58 | 2 525 | 304, un 12.0 % |
| &nbsp;&nbsp;insertados en el churn | 589 | 0.8702 | 0.9004 | 9.28 | 489 | 53, un 10.8 % |
| &nbsp;&nbsp;con algún sustituto | 874 | 0.7449 | 0.8134 | 10.36 | 2 036 | 251, un 12.3 % |
| &nbsp;&nbsp;reparados por búsqueda | 37 | 0.8613 | 0.9275 | 4.32 | — | — |
| &nbsp;&nbsp;ni reparados ni con sustituto | 500 | 0.7553 | 0.8198 | 4.95 | — | — |

**Ronda 20:**

| | vectores | recall de arista | recall de postings | aristas por vector | sustitutos en la lista | de ellos, aristas verdaderas |
|---|---|---|---|---|---|---|
| el lote de S = 236 | 2 000 | 0.9707 | 0.9556 | 8.70 | — | — |
| **B: sustitución local** | 2 000 | 0.7273 | 0.7990 | 8.66 | 3 520 | 526, un 14.9 % |
| &nbsp;&nbsp;insertados en el churn | 1 257 | 0.7796 | 0.8321 | 8.77 | 1 797 | 226, un 12.6 % |
| &nbsp;&nbsp;con algún sustituto | 544 | 0.6379 | 0.7457 | 10.01 | 1 723 | 300, un 17.4 % |
| &nbsp;&nbsp;ni reparados ni con sustituto | 199 | 0.6488 | 0.7377 | 4.34 | — | — |
| **C: sustitución con red** | 2 000 | 0.7343 | 0.8035 | 8.73 | 3 497 | 526, un 15.0 % |
| &nbsp;&nbsp;insertados en el churn | 1 257 | 0.7836 | 0.8367 | 8.81 | 1 803 | 231, un 12.8 % |
| &nbsp;&nbsp;con algún sustituto | 533 | 0.6467 | 0.7507 | 10.18 | 1 694 | 295, un 17.4 % |
| &nbsp;&nbsp;reparados por búsqueda | 36 | 0.8392 | 0.8780 | 4.89 | — | — |
| &nbsp;&nbsp;ni reparados ni con sustituto | 174 | 0.6448 | 0.7221 | 4.51 | — | — |

**Ronda 30:**

| | vectores | recall de arista | recall de postings | aristas por vector | sustitutos en la lista | de ellos, aristas verdaderas |
|---|---|---|---|---|---|---|
| el lote de S = 236 | 2 000 | 0.9739 | 0.9607 | 8.70 | — | — |
| **B: sustitución local** | 2 000 | 0.7035 | 0.7953 | 8.68 | 4 009 | 619, un 15.4 % |
| &nbsp;&nbsp;insertados en el churn | 1 793 | 0.7226 | 0.8080 | 8.68 | 3 295 | 468, un 14.2 % |
| &nbsp;&nbsp;con algún sustituto | 159 | 0.5401 | 0.6899 | 10.16 | 714 | 151, un 21.1 % |
| &nbsp;&nbsp;ni reparados ni con sustituto | 48 | 0.5786 | 0.7222 | 3.65 | — | — |
| **C: sustitución con red** | 2 000 | 0.7152 | 0.7998 | 8.79 | 3 985 | 620, un 15.6 % |
| &nbsp;&nbsp;insertados en el churn | 1 793 | 0.7337 | 0.8122 | 8.79 | 3 280 | 473, un 14.4 % |
| &nbsp;&nbsp;con algún sustituto | 155 | 0.5426 | 0.6900 | 10.21 | 705 | 147, un 20.9 % |
| &nbsp;&nbsp;reparados por búsqueda | 11 | 0.8028 | 0.9130 | 5.27 | — | — |
| &nbsp;&nbsp;ni reparados ni con sustituto | 41 | 0.6207 | 0.7463 | 4.27 | — | — |

### El 30 % borrado por regiones

Contra el lote de S = 236 de 692 741 vectores vivos, en 2 936 postings: 18.99 cargas a 0.95 y 91.01 a 0.99.

| | celda | peor recall a igual ef | entra | cargas a 0.95 | entra | cargas a 0.99 | entra | las tres |
|---|---|---|---|---|---|---|---|---|
| por regiones | B: sustitución local | +0.0002 en ef 1024 | sí | 15.31, −19.4 % | sí | 79.39, −12.8 % | sí | **sí** |
| por regiones | C: sustitución con red | +0.0004 en ef 1024 | sí | 15.35, −19.2 % | sí | 77.11, −15.3 % | sí | **sí** |

| | celda | listas reparadas por búsqueda | cargas de búsqueda | postings de la fase 2 | distancias exactas | distancias entre códigos | del ensayo, aparte | aristas muertas con sustituto |
|---|---|---|---|---|---|---|---|---|
| por borrado | B: sustitución local | 0.00 | 0.0 | 0.01 | 0.0 | 12.9 | 0.0 | 82.2 % |
| por borrado | C: sustitución con red | 0.10 | 4.5 | 0.01 | 8.9 | 12.3 | 12.9 | 90.7 % |

| celda | vivos | postings | vivos por posting | listas reparadas | aristas sustituidas | quitadas sin sustituto | fusiones | aristas por vector vivo |
|---|---|---|---|---|---|---|---|---|
| B: sustitución local | 692 741 | 2 675 | 259.0 | 0 | 796 948 | 172 964 | 185 | 8.29 |
| C: sustitución con red | 692 741 | 2 675 | 259.0 | 29 917 | 725 386 | 74 338 | 185 | 8.29 |

**Recall de arista:**

| | vectores | recall de arista | recall de postings | aristas por vector | sustitutos en la lista | de ellos, aristas verdaderas |
|---|---|---|---|---|---|---|
| el lote de S = 236 | 2 000 | 0.9775 | 0.9652 | 8.11 | — | — |
| **B: sustitución local** | 2 000 | 0.8941 | 0.8997 | 8.10 | 2 298 | 279, un 12.1 % |
| &nbsp;&nbsp;con algún sustituto | 845 | 0.8014 | 0.8286 | 9.70 | 2 298 | 279, un 12.1 % |
| &nbsp;&nbsp;ni reparados ni con sustituto | 1 155 | 0.9826 | 0.9749 | 6.92 | — | — |
| **C: sustitución con red** | 2 000 | 0.9031 | 0.9076 | 8.10 | 2 140 | 255, un 11.9 % |
| &nbsp;&nbsp;con algún sustituto | 784 | 0.8122 | 0.8372 | 10.09 | 2 140 | 255, un 11.9 % |
| &nbsp;&nbsp;reparados por búsqueda | 78 | 0.9808 | 0.9762 | 3.97 | — | — |
| &nbsp;&nbsp;ni reparados ni con sustituto | 1 138 | 0.9836 | 0.9762 | 7.01 | — | — |

