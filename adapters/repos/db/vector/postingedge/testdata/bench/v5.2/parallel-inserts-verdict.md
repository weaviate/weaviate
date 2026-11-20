### Las celdas

- **Dataset:** dbpedia-openai-1000k-angular.bin, 990 000 vectores. Del 10 % al 100 %: 891 000 inserts por celda.
- **Camino:** el de v5.0. Split con T = 360, ef de inserción 160, códigos de ruteo de 512 bits en RAM.

### Cobertura en cada checkpoint, con el reloj parado

| celda | 20 % | 40 % | 60 % | 80 % | 100 % | al acabar |
|---|---|---|---|---|---|---|
| N = 1, el insert de siempre | completa | completa | completa | completa | completa | completa |
| N = 4, tandas de 32 | completa | completa | completa | completa | completa | completa |
| N = 8, tandas de 32 | completa | completa | completa | completa | completa | completa |
| N = 8, tandas de 8 | completa | completa | completa | completa | completa | completa |

### Inserts por segundo

| celda | hasta el 20 % | hasta el 40 % | hasta el 60 % | hasta el 80 % | hasta el 100 % | toda la corrida | frente a N = 1 |
|---|---|---|---|---|---|---|---|
| N = 1, el insert de siempre | 207 | 189 | 164 | 154 | 157 | 169 | 1.00x |
| N = 4, tandas de 32 | 577 | 521 | 501 | 487 | 486 | 506 | 3.00x |
| N = 8, tandas de 32 | 709 | 642 | 629 | 620 | 615 | 634 | 3.76x |
| N = 8, tandas de 8 | 598 | 540 | 489 | 468 | 442 | 493 | 2.92x |

### Dónde se va el tiempo

| celda | el aplicador, del tiempo de reloj | los buscadores, del suyo, buscando | búsqueda por insert, ms | aplicar un insert, ms | escritura | aristas inversas | splits | filas de aristas | ciclo del store |
|---|---|---|---|---|---|---|---|---|---|
| N = 1, el insert de siempre | 5.7 % | 100 % | 5.59 | 0.340 | 0.146 | 0.063 | 0.108 | 0.000 | 0.021 |
| N = 4, tandas de 32 | 14.8 % | 92 % | 6.21 | 0.292 | 0.143 | 0.048 | 0.077 | 0.000 | 0.023 |
| N = 8, tandas de 32 | 18.8 % | 84 % | 8.59 | 0.296 | 0.154 | 0.049 | 0.068 | 0.000 | 0.025 |
| N = 8, tandas de 8 | 15.0 % | 53 % | 7.38 | 0.304 | 0.148 | 0.055 | 0.076 | 0.000 | 0.024 |

- **El aplicador** es el tiempo de reloj con las escrituras de los inserts, una detrás de otra. Lo demás es el tiempo de reloj con las búsquedas corriendo.
- **Los buscadores, buscando:** lo que suman sus búsquedas entre N veces el tiempo de reloj de sus fases. Lo que falta hasta 100 es espera.

### Lo que el aplicador tuvo que rehacer

| celda | tandas | planes que eligieron posting otra vez | de los planes | planes que releyeron alguna lista | de los planes |
|---|---|---|---|---|---|
| N = 4, tandas de 32 | 27 846 | 55 | 0.006 % | 1 662 | 0.187 % |
| N = 8, tandas de 32 | 27 846 | 35 | 0.004 % | 1 622 | 0.182 % |
| N = 8, tandas de 8 | 111 375 | 9 | 0.001 % | 369 | 0.041 % |

### Lo que daría una cadena continua, calculado y no medido

El menor de N entre el tiempo de una búsqueda y 1 entre el tiempo de aplicar un insert, con los tiempos medidos en cada celda.

| celda | medido, inserts por segundo | cadena continua | frente a N = 1 medido | la limita |
|---|---|---|---|---|
| N = 1, el insert de siempre | 169 | 179 | 1.06x | los buscadores |
| N = 4, tandas de 32 | 506 | 644 | 3.82x | los buscadores |
| N = 8, tandas de 32 | 634 | 932 | 5.53x | los buscadores |
| N = 8, tandas de 8 | 493 | 1 084 | 6.43x | los buscadores |

### Calidad del índice que deja cada celda

| celda | postings | splits | 0.95: ef | recall | cargas | frente a N = 1 | 0.99: ef | recall | cargas | frente a N = 1 |
|---|---|---|---|---|---|---|---|---|---|---|
| N = 1, el insert de siempre | 7 774 | 3 677 | 21 | 0.9513 | 19.55 | — | 144 | 0.9901 | 67.39 | — |
| N = 4, tandas de 32 | 7 774 | 3 677 | 20 | 0.9502 | 19.00 | −2.8 % | 149 | 0.9900 | 68.89 | +2.2 % |
| N = 8, tandas de 32 | 7 774 | 3 677 | 21 | 0.9513 | 19.54 | −0.1 % | 179 | 0.9900 | 78.05 | +15.8 % |
| N = 8, tandas de 8 | 7 780 | 3 680 | 21 | 0.9511 | 19.53 | −0.1 % | 167 | 0.9900 | 74.27 | +10.2 % |

Con el ef de N = 1:

| celda | ef | recall | el de N = 1 | cargas | las de N = 1 | |
|---|---|---|---|---|---|---|
| N = 4, tandas de 32 | 21 | 0.9516 | 0.9513 | 19.55 | 19.55 | +0.0 % |
| N = 4, tandas de 32 | 144 | 0.9896 | 0.9901 | 67.26 | 67.39 | −0.2 % |
| N = 8, tandas de 32 | 21 | 0.9513 | 0.9513 | 19.54 | 19.55 | −0.1 % |
| N = 8, tandas de 32 | 144 | 0.9890 | 0.9901 | 67.26 | 67.39 | −0.2 % |
| N = 8, tandas de 8 | 21 | 0.9511 | 0.9513 | 19.53 | 19.55 | −0.1 % |
| N = 8, tandas de 8 | 144 | 0.9890 | 0.9901 | 67.11 | 67.39 | −0.4 % |

- **El ruido entre dos corridas:** N = 4 y N = 8 llevan la misma tanda y difieren en un 2.9 % a 0.95 (19.00 y 19.54 cargas) y un 13.3 % a 0.99 (68.89 y 78.05 cargas).

### El criterio

Sirve si N = 8 da 4 veces o más los inserts por segundo de N = 1, con cargas a 0.95 y a 0.99 a un 5 % o menos por encima de las de N = 1.

- **Inserts por segundo:** 634 frente a 169, 3.76x. No se cumple.
- **Cargas a 0.95:** 19.54 frente a 19.55, −0.1 %. Se cumple.
- **Cargas a 0.99:** 78.05 frente a 67.39, +15.8 %. No se cumple.
- **No sirve, por la regla como está escrita.**

### La sesión

- **N = 1, el insert de siempre:** de 2026-11-20 06:39:41 a 2026-11-20 08:09:50, con teclado o trackpad durante la corrida, 5 eventos de energía o de actividad.
- **N = 4, tandas de 32:** de 2026-11-20 08:09:50 a 2026-11-20 08:41:38, sin teclado ni trackpad.
- **N = 8, tandas de 32:** de 2026-11-20 08:41:38 a 2026-11-20 09:07:37, con teclado o trackpad durante la corrida, 3 eventos de energía o de actividad.
- **N = 8, tandas de 8:** de 2026-11-20 09:18:39 a 2026-11-20 09:51:25, con teclado o trackpad durante la corrida, 2 eventos de energía o de actividad.
- **El tiempo de cada corrida** lleva, además de los inserts, la base, las comprobaciones de cobertura y las búsquedas del final, que no se cronometran.
