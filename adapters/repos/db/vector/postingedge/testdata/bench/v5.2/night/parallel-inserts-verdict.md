### Las celdas

- **Dataset:** dbpedia-openai-1000k-angular.bin, 990 000 vectores. Del 10 % al 100 %: 891 000 inserts por celda.
- **Camino:** el de v5.0. Split con T = 360, ef de inserción 160, códigos de ruteo de 512 bits en RAM.

### Cobertura en cada checkpoint, con el reloj parado

| celda | 20 % | 40 % | 60 % | 80 % | 100 % | al acabar |
|---|---|---|---|---|---|---|
| N = 1, el insert de siempre | completa | completa | completa | completa | completa | completa |
| N = 4, tandas de 32 | completa | completa | completa | completa | completa | completa |
| N = 8, tandas de 32 | completa | completa | completa | completa | completa | completa |

### Inserts por segundo

| celda | hasta el 20 % | hasta el 40 % | hasta el 60 % | hasta el 80 % | hasta el 100 % | toda la corrida | frente a N = 1 |
|---|---|---|---|---|---|---|---|
| N = 1, el insert de siempre | 240 | 211 | 183 | 169 | 160 | 184 | 1.00x |
| N = 4, tandas de 32 | 582 | 527 | 495 | 493 | 476 | 505 | 2.74x |
| N = 8, tandas de 32 | 706 | 690 | 646 | 652 | 647 | 663 | 3.60x |

### Dónde se va el tiempo

| celda | el aplicador, del tiempo de reloj | los buscadores, del suyo, buscando | búsqueda por insert, ms | aplicar un insert, ms | escritura | aristas inversas | splits | filas de aristas | ciclo del store |
|---|---|---|---|---|---|---|---|---|---|
| N = 1, el insert de siempre | 4.5 % | 100 % | 5.18 | 0.245 | 0.130 | 0.062 | 0.031 | 0.000 | 0.020 |
| N = 4, tandas de 32 | 15.1 % | 92 % | 6.20 | 0.298 | 0.146 | 0.049 | 0.075 | 0.000 | 0.027 |
| N = 8, tandas de 32 | 19.3 % | 83 % | 8.03 | 0.291 | 0.170 | 0.050 | 0.048 | 0.000 | 0.023 |

- **El aplicador** es el tiempo de reloj con las escrituras de los inserts, una detrás de otra. Lo demás es el tiempo de reloj con las búsquedas corriendo.
- **Los buscadores, buscando:** lo que suman sus búsquedas entre N veces el tiempo de reloj de sus fases. Lo que falta hasta 100 es espera.

### Lo que el aplicador tuvo que rehacer

| celda | tandas | planes que eligieron posting otra vez | de los planes | planes que releyeron alguna lista | de los planes |
|---|---|---|---|---|---|
| N = 4, tandas de 32 | 27 846 | 46 | 0.005 % | 1 668 | 0.187 % |
| N = 8, tandas de 32 | 27 846 | 44 | 0.005 % | 1 642 | 0.184 % |

### Lo que daría una cadena continua, calculado y no medido

El menor de N entre el tiempo de una búsqueda y 1 entre el tiempo de aplicar un insert, con los tiempos medidos en cada celda.

| celda | medido, inserts por segundo | cadena continua | frente a N = 1 medido | la limita |
|---|---|---|---|---|
| N = 1, el insert de siempre | 184 | 193 | 1.05x | los buscadores |
| N = 4, tandas de 32 | 505 | 645 | 3.50x | los buscadores |
| N = 8, tandas de 32 | 663 | 996 | 5.41x | los buscadores |

### Calidad del índice que deja cada celda

| celda | postings | splits | 0.95: ef | recall | cargas | frente a N = 1 | 0.99: ef | recall | cargas | frente a N = 1 |
|---|---|---|---|---|---|---|---|---|---|---|
| N = 1, el insert de siempre | 7 706 | 3 643 | 21 | 0.9516 | 19.57 | — | 167 | 0.9900 | 74.19 | — |
| N = 4, tandas de 32 | 7 768 | 3 674 | 21 | 0.9520 | 19.50 | −0.4 % | 161 | 0.9900 | 72.51 | −2.3 % |
| N = 8, tandas de 32 | 7 778 | 3 679 | 20 | 0.9509 | 19.07 | −2.6 % | 167 | 0.9901 | 74.44 | +0.3 % |

Con el ef de N = 1:

| celda | ef | recall | el de N = 1 | cargas | las de N = 1 | |
|---|---|---|---|---|---|---|
| N = 4, tandas de 32 | 21 | 0.9520 | 0.9516 | 19.50 | 19.57 | −0.4 % |
| N = 4, tandas de 32 | 167 | 0.9902 | 0.9900 | 74.34 | 74.19 | +0.2 % |
| N = 8, tandas de 32 | 21 | 0.9524 | 0.9516 | 19.63 | 19.57 | +0.3 % |
| N = 8, tandas de 32 | 167 | 0.9901 | 0.9900 | 74.44 | 74.19 | +0.3 % |

- **El ruido entre dos corridas:** N = 4 y N = 8 llevan la misma tanda y difieren en un 2.2 % a 0.95 (19.50 y 19.07 cargas) y un 2.7 % a 0.99 (72.51 y 74.44 cargas).

### El criterio

Sirve si N = 8 da 4 veces o más los inserts por segundo de N = 1, con cargas a 0.95 y a 0.99 a un 5 % o menos por encima de las de N = 1.

- **Inserts por segundo:** 663 frente a 184, 3.60x. No se cumple.
- **Cargas a 0.95:** 19.07 frente a 19.57, −2.6 %. Se cumple.
- **Cargas a 0.99:** 74.44 frente a 74.19, +0.3 %. Se cumple.
- **No sirve, por la regla como está escrita.**

### La sesión

- **N = 1, el insert de siempre:** de 2026-11-20 23:32:06 a 2026-11-21 00:55:01, sin teclado ni trackpad.
- **N = 4, tandas de 32:** de 2026-11-21 00:55:02 a 2026-11-21 01:26:55, sin teclado ni trackpad.
- **N = 8, tandas de 32:** de 2026-11-21 01:26:55 a 2026-11-21 01:51:52, sin teclado ni trackpad.
- **El tiempo de cada corrida** lleva, además de los inserts, la base, las comprobaciones de cobertura y las búsquedas del final, que no se cronometran.
