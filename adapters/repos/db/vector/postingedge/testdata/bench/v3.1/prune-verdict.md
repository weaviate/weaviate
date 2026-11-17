### Estado

- **La corrida:** 990000 vectores, 5000 queries, k = 10. 24 celdas de 24: 12 con sketch de 384 bits, 12 con sketch de 512 bits.
- **La malla de ef:** 10, 14, 20, 28, 40, 56, 80, 113, 160, 226, 320, 452, 640, 1024, 2048.
- **El build es el de v3.0:** los miembros, las aristas y los endpoints distintos de sus 4195 postings son los de `v3.0/dbpedia-1m-halo-s236.json`.
  - Base de 384 bits, a igual ef: 0 de los 9 puntos de v3.0 salen idénticos en recall, cargas, rondas y entradas de halo; la mayor diferencia en cargas es de 0.04.
  - Base de 512 bits, a igual ef: 0 de los 9 puntos de v3.0 salen idénticos en recall, cargas, rondas y entradas de halo; la mayor diferencia en cargas es de 0.03.
  - Referencia con código completo, a igual ef: 0 de 9 puntos idénticos.

### Las aristas de cada poda

| aristas | por vector | máximo por vector | postings de destino por vector | aristas repetidas hacia un posting | frente a todas | por posting: media | p95 | endpoints distintos por posting: media | p95 | por arista | frente a todas |
|---|---|---|---|---|---|---|---|---|---|---|---|
| todas | 8.98 | 16 | 5.51 | 3436183 | — | 2118.7 | 3665 | 1318.0 | 2557 | 0.622 | — |
| una por posting de destino | 5.51 | 16 | 5.51 | 0 | −38.7 % | 1299.6 | 2650 | 950.2 | 2060 | 0.731 | −27.9 % |
| cap 8 | 6.35 | 8 | 4.22 | 2109934 | −29.3 % | 1497.8 | 2494 | 946.8 | 1699 | 0.632 | −28.2 % |
| una por posting de destino y cap 8 | 4.90 | 8 | 4.90 | 0 | −45.4 % | 1156.5 | 2145 | 839.5 | 1647 | 0.726 | −36.3 % |

**La comprobación de cada poda,** hecha en el paso, antes de medir: cada fila se rehace desde la fila completa, arista a arista, y tiene que coincidir.

- **Una por posting de destino:** 0 aristas repetidas hacia un mismo posting; 5451843 postings de destino, los 5451843 de todas las aristas: ningún vector pierde un destino.
- **Cap 8:** como mucho 8 aristas por vector, con el cap en 8.
- **Una por posting de destino y cap 8:** 0 aristas repetidas hacia un mismo posting; como mucho 8 aristas por vector, con el cap en 8; 4851404 postings de destino de 5451843: con el cap se pierde el 11.0 %.

**El posting, bloque a bloque,** de media:

| aristas | miembros | aristas | halo de 384 bits | el posting entero | frente a todas | halo de 512 bits | el posting entero | frente a todas |
|---|---|---|---|---|---|---|---|---|
| todas | 49.1 KB | 8.5 KB | 79.1 KB | 136.6 KB | — | 100.2 KB | 157.7 KB | — |
| una por posting de destino | 49.1 KB | 5.2 KB | 57.0 KB | 111.3 KB | −18.5 % | 72.2 KB | 126.5 KB | −19.8 % |
| cap 8 | 49.1 KB | 6.0 KB | 56.8 KB | 111.9 KB | −18.1 % | 72.0 KB | 127.0 KB | −19.5 % |
| una por posting de destino y cap 8 | 49.1 KB | 4.6 KB | 50.4 KB | 104.1 KB | −23.8 % | 63.8 KB | 117.5 KB | −25.5 % |

### Las celdas

Bytes de posting por query, de los postings que carga cada una, con sus tres bloques y sin el rescore. Entre paréntesis, la parte que es halo. El listón son 2.46 MB a 0.95.

**Sketch de 384 bits:**

| aristas | M | celda | gate de 0.99 | a 0.95: cargas | rondas | bytes (halo) | frente a la base | ¿2.46 MB o menos? | a 0.99: cargas | rondas | bytes (halo) | frente a la base | disco total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| todas | 0.04 | celda 3 | pasa | 23.14 | 5.54 | 3.64 MB (2.20 MB) | −2.3 % | no | 93.06 | 23.02 | 14.47 MB (8.74 MB) | +2.5 % | 573.20 MB |
| todas | 0.06 | **base** | pasa | 23.72 | 5.68 | 3.72 MB (2.25 MB) | — | — | 90.70 | 22.43 | 14.11 MB (8.52 MB) | — | 573.20 MB |
| todas | 0.08 | celda 3 | pasa | 23.70 | 5.68 | 3.72 MB (2.24 MB) | −0.1 % | no | 91.86 | 22.72 | 14.29 MB (8.62 MB) | +1.3 % | 573.20 MB |
| una por posting de destino | 0.04 | combinación | pasa | 24.21 | 5.80 | 3.14 MB (1.72 MB) | −15.7 % | no | 103.25 | 25.56 | 13.19 MB (7.22 MB) | −6.5 % | 466.89 MB |
| una por posting de destino | 0.06 | celda 1 | pasa | 24.81 | 5.95 | 3.21 MB (1.76 MB) | −13.7 % | no | 102.52 | 25.38 | 13.10 MB (7.16 MB) | −7.2 % | 466.89 MB |
| una por posting de destino | 0.08 | combinación | pasa | 25.01 | 6.00 | 3.24 MB (1.77 MB) | −13.1 % | no | 103.12 | 25.53 | 13.18 MB (7.20 MB) | −6.6 % | 466.89 MB |
| cap 8 | 0.04 | combinación | pasa | 24.65 | 5.91 | 3.10 MB (1.65 MB) | −16.6 % | no | 110.87 | 27.47 | 13.77 MB (7.29 MB) | −2.4 % | 469.35 MB |
| cap 8 | 0.06 | celda 2 | pasa | 25.38 | 6.10 | 3.20 MB (1.70 MB) | −14.1 % | no | 111.80 | 27.70 | 13.90 MB (7.36 MB) | −1.5 % | 469.35 MB |
| cap 8 | 0.08 | combinación | pasa | 25.70 | 6.17 | 3.23 MB (1.72 MB) | −13.1 % | no | 112.69 | 27.92 | 14.02 MB (7.42 MB) | −0.7 % | 469.35 MB |
| una por posting de destino y cap 8 | 0.04 | combinación | pasa | 24.76 | 5.94 | 2.94 MB (1.51 MB) | −21.0 % | no | 110.38 | 27.34 | 12.91 MB (6.63 MB) | −8.5 % | 436.63 MB |
| una por posting de destino y cap 8 | 0.06 | combinación | pasa | 25.33 | 6.08 | 3.01 MB (1.55 MB) | −19.2 % | no | 109.76 | 27.19 | 12.85 MB (6.59 MB) | −8.9 % | 436.63 MB |
| una por posting de destino y cap 8 | 0.08 | combinación | pasa | 25.46 | 6.11 | 3.02 MB (1.55 MB) | −18.8 % | no | 110.53 | 27.38 | 12.95 MB (6.64 MB) | −8.2 % | 436.63 MB |

**Sketch de 512 bits:**

| aristas | M | celda | gate de 0.99 | a 0.95: cargas | rondas | bytes (halo) | frente a la base | ¿2.46 MB o menos? | a 0.99: cargas | rondas | bytes (halo) | frente a la base | disco total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| todas | 0.04 | celda 3 | pasa | 18.43 | 4.36 | 3.37 MB (2.22 MB) | +1.9 % | no | 79.06 | 19.52 | 14.33 MB (9.45 MB) | +2.2 % | 661.67 MB |
| todas | 0.06 | **base** | pasa | 18.12 | 4.28 | 3.31 MB (2.18 MB) | — | — | 77.36 | 19.09 | 14.02 MB (9.25 MB) | — | 661.67 MB |
| todas | 0.08 | celda 3 | pasa | 18.19 | 4.30 | 3.32 MB (2.18 MB) | +0.2 % | no | 75.90 | 18.73 | 13.77 MB (9.08 MB) | −1.8 % | 661.67 MB |
| una por posting de destino | 0.04 | combinación | pasa | 19.51 | 4.63 | 2.91 MB (1.76 MB) | −12.1 % | no | 85.20 | 21.05 | 12.53 MB (7.60 MB) | −10.6 % | 530.66 MB |
| una por posting de destino | 0.06 | celda 1 | pasa | 19.63 | 4.66 | 2.92 MB (1.77 MB) | −11.7 % | no | 81.41 | 20.10 | 11.99 MB (7.26 MB) | −14.5 % | 530.66 MB |
| una por posting de destino | 0.08 | combinación | pasa | 19.66 | 4.66 | 2.92 MB (1.77 MB) | −11.7 % | no | 81.70 | 20.18 | 12.03 MB (7.29 MB) | −14.2 % | 530.66 MB |
| cap 8 | 0.04 | combinación | pasa | 20.44 | 4.86 | 2.95 MB (1.74 MB) | −10.9 % | no | 94.41 | 23.35 | 13.43 MB (7.91 MB) | −4.2 % | 532.90 MB |
| cap 8 | 0.06 | celda 2 | pasa | 20.49 | 4.87 | 2.95 MB (1.74 MB) | −10.7 % | no | 90.66 | 22.41 | 12.92 MB (7.61 MB) | −7.9 % | 532.90 MB |
| cap 8 | 0.08 | combinación | pasa | 20.35 | 4.84 | 2.93 MB (1.73 MB) | −11.3 % | no | 90.30 | 22.33 | 12.87 MB (7.58 MB) | −8.2 % | 532.90 MB |
| una por posting de destino y cap 8 | 0.04 | combinación | pasa | 20.38 | 4.85 | 2.76 MB (1.58 MB) | −16.5 % | no | 91.66 | 22.67 | 12.25 MB (7.02 MB) | −12.7 % | 492.98 MB |
| una por posting de destino y cap 8 | 0.06 | combinación | pasa | 20.40 | 4.85 | 2.76 MB (1.58 MB) | −16.5 % | no | 87.26 | 21.57 | 11.68 MB (6.69 MB) | −16.7 % | 492.98 MB |
| una por posting de destino y cap 8 | 0.08 | combinación | pasa | 20.36 | 4.84 | 2.75 MB (1.58 MB) | −16.7 % | no | 86.65 | 21.41 | 11.60 MB (6.65 MB) | −17.3 % | 492.98 MB |

Dónde cruza cada celda, en su curva de ef:

| sketch | aristas | M | cruza 0.95 entre | cruza 0.99 entre |
|---|---|---|---|---|
| 384 bits | todas | 0.04 | ef 20 y 28 (0.9438 y 0.9561) | ef 160 y 226 (0.9885 y 0.9911) |
| 384 bits | todas | 0.06 | ef 20 y 28 (0.9482 y 0.9596) | ef 160 y 226 (0.9899 y 0.9921) |
| 384 bits | todas | 0.08 | ef 14 y 20 (0.9356 y 0.9505) | ef 113 y 160 (0.9870 y 0.9902) |
| 384 bits | una por posting de destino | 0.04 | ef 20 y 28 (0.9381 y 0.9506) | ef 226 y 320 (0.9894 y 0.9920) |
| 384 bits | una por posting de destino | 0.06 | ef 20 y 28 (0.9435 y 0.9553) | ef 160 y 226 (0.9882 y 0.9906) |
| 384 bits | una por posting de destino | 0.08 | ef 20 y 28 (0.9452 y 0.9574) | ef 160 y 226 (0.9885 y 0.9909) |
| 384 bits | cap 8 | 0.04 | ef 28 y 40 (0.9491 y 0.9607) | ef 226 y 320 (0.9885 y 0.9911) |
| 384 bits | cap 8 | 0.06 | ef 20 y 28 (0.9418 y 0.9536) | ef 226 y 320 (0.9896 y 0.9920) |
| 384 bits | cap 8 | 0.08 | ef 20 y 28 (0.9434 y 0.9557) | ef 226 y 320 (0.9900 y 0.9924) |
| 384 bits | una por posting de destino y cap 8 | 0.04 | ef 28 y 40 (0.9481 y 0.9603) | ef 226 y 320 (0.9883 y 0.9910) |
| 384 bits | una por posting de destino y cap 8 | 0.06 | ef 20 y 28 (0.9410 y 0.9530) | ef 226 y 320 (0.9897 y 0.9921) |
| 384 bits | una por posting de destino y cap 8 | 0.08 | ef 20 y 28 (0.9434 y 0.9552) | ef 160 y 226 (0.9873 y 0.9900) |
| 512 bits | todas | 0.04 | ef 20 y 28 (0.9453 y 0.9566) | ef 160 y 226 (0.9874 y 0.9905) |
| 512 bits | todas | 0.06 | ef 20 y 28 (0.9498 y 0.9604) | ef 160 y 226 (0.9885 y 0.9915) |
| 512 bits | todas | 0.08 | ef 14 y 20 (0.9353 y 0.9508) | ef 160 y 226 (0.9891 y 0.9916) |
| 512 bits | una por posting de destino | 0.04 | ef 20 y 28 (0.9387 y 0.9513) | ef 226 y 320 (0.9892 y 0.9916) |
| 512 bits | una por posting de destino | 0.06 | ef 20 y 28 (0.9426 y 0.9553) | ef 160 y 226 (0.9871 y 0.9904) |
| 512 bits | una por posting de destino | 0.08 | ef 20 y 28 (0.9442 y 0.9565) | ef 160 y 226 (0.9874 y 0.9906) |
| 512 bits | cap 8 | 0.04 | ef 28 y 40 (0.9483 y 0.9595) | ef 226 y 320 (0.9876 y 0.9906) |
| 512 bits | cap 8 | 0.06 | ef 20 y 28 (0.9409 y 0.9523) | ef 226 y 320 (0.9890 y 0.9918) |
| 512 bits | cap 8 | 0.08 | ef 20 y 28 (0.9430 y 0.9539) | ef 226 y 320 (0.9893 y 0.9920) |
| 512 bits | una por posting de destino y cap 8 | 0.04 | ef 28 y 40 (0.9479 y 0.9595) | ef 226 y 320 (0.9882 y 0.9906) |
| 512 bits | una por posting de destino y cap 8 | 0.06 | ef 20 y 28 (0.9394 y 0.9518) | ef 226 y 320 (0.9894 y 0.9918) |
| 512 bits | una por posting de destino y cap 8 | 0.08 | ef 20 y 28 (0.9414 y 0.9536) | ef 226 y 320 (0.9897 y 0.9919) |

### El margen (celda 3)

Con todas las aristas. Se elige, por ancho, el M que menos bytes lee a 0.95.

| sketch | M | a 0.95: cargas | bytes | frente a M = 0.06 | a 0.99: cargas | bytes | frente a M = 0.06 |
|---|---|---|---|---|---|---|---|
| 384 bits | 0.04 | 23.14 | 3.64 MB | −2.3 % | 93.06 | 14.47 MB | +2.5 % |
| 384 bits | 0.06 (base) | 23.72 | 3.72 MB | — | 90.70 | 14.11 MB | — |
| 384 bits | 0.08 | 23.70 | 3.72 MB | −0.1 % | 91.86 | 14.29 MB | +1.3 % |
| 512 bits | 0.04 | 18.43 | 3.37 MB | +1.9 % | 79.06 | 14.33 MB | +2.2 % |
| 512 bits | 0.06 (base) | 18.12 | 3.31 MB | — | 77.36 | 14.02 MB | — |
| 512 bits | 0.08 | 18.19 | 3.32 MB | +0.2 % | 75.90 | 13.77 MB | −1.8 % |

- **Sketch de 384 bits: M = 0.04,** con 3.64 MB a 0.95, un 2.3 % menos que con M = 0.06. A 0.99 lee menos M = 0.06: 14.11 MB, frente a 14.47 MB con M = 0.04.
- **Sketch de 512 bits: M = 0.06,** con 3.31 MB a 0.95. Es el de la base: ningún otro margen lee menos. A 0.99 lee menos M = 0.08: 13.77 MB, frente a 14.02 MB con M = 0.06.

### La mejor combinación (celda 4)

De las celdas con dos palancas o más, la que menos bytes lee a 0.95 entre las que pasan el gate.

- **Sketch de 384 bits: una por posting de destino y cap 8, M = 0.04.** Lee 2.94 MB a 0.95 (un 21.0 % menos que la base) y 12.91 MB a 0.99 (un 8.5 % menos).
  - La mejor palanca sola es cap 8, M = 0.06, con 3.20 MB a 0.95: la combinación lee un 8.0 % menos.
  - A 0.99 la combinación que menos lee es otra: una por posting de destino y cap 8, M = 0.06, con 12.85 MB.
- **Sketch de 512 bits: una por posting de destino y cap 8, M = 0.08.** Lee 2.75 MB a 0.95 (un 16.7 % menos que la base) y 11.60 MB a 0.99 (un 17.3 % menos).
  - La mejor palanca sola es una por posting de destino, M = 0.06, con 2.92 MB a 0.95: la combinación lee un 5.7 % menos.

### El criterio

**La poda sirve si alguna celda lee 2.46 MB o menos a 0.95 y mantiene 0.99 alcanzable.**

- **Sketch de 384 bits.** La base lee 3.72 MB a 0.95 y 14.11 MB a 0.99. Para llegar al listón hay que leer un 33.9 % menos.
  - Las 11 celdas podadas pasan el gate.
  - La que menos lee a 0.95 es una por posting de destino y cap 8, M = 0.04: 2.94 MB, un 21.0 % menos que la base, y a 0.48 MB del listón.
  - **Cumplen 0 de 11.**
- **Sketch de 512 bits.** La base lee 3.31 MB a 0.95 y 14.02 MB a 0.99. Para llegar al listón hay que leer un 25.6 % menos.
  - Las 11 celdas podadas pasan el gate.
  - La que menos lee a 0.95 es una por posting de destino y cap 8, M = 0.08: 2.75 MB, un 16.7 % menos que la base, y a 0.29 MB del listón.
  - **Cumplen 0 de 11.**

**Veredicto: la poda no sirve,** por el criterio: ninguna celda lee 2.46 MB o menos a 0.95 pasando el gate de 0.99.

**Cuánto baja a 0.99:**

| sketch | celda | bytes a 0.99 | frente a la base | en MB |
|---|---|---|---|---|
| 384 bits | base | 14.11 MB | — | — |
| 384 bits | una por posting de destino y cap 8, M = 0.06: la que menos lee a 0.99 | 12.85 MB | −8.9 % | −1.26 MB |
| 384 bits | una por posting de destino y cap 8, M = 0.04: la que menos lee a 0.95 | 12.91 MB | −8.5 % | −1.20 MB |
| 384 bits | cap 8, M = 0.06: la mejor palanca sola a 0.95 | 13.90 MB | −1.5 % | −0.21 MB |
| 512 bits | base | 14.02 MB | — | — |
| 512 bits | una por posting de destino y cap 8, M = 0.08: la que menos lee a 0.99, la que menos lee a 0.95 | 11.60 MB | −17.3 % | −2.42 MB |
| 512 bits | una por posting de destino, M = 0.06: la mejor palanca sola a 0.95 | 11.99 MB | −14.5 % | −2.04 MB |

### Lo que quita cada palanca sola a 0.95, y lo que añade la combinación

| sketch | celda | bytes a 0.95 | frente a la base | cargas | frente a la base | bytes por carga | frente a la base |
|---|---|---|---|---|---|---|---|
| 384 bits | base | 3.72 MB | — | 23.72 | — | 157.0 KB | — |
| 384 bits | todas, M = 0.04 | 3.64 MB | −2.3 % | 23.14 | −2.4 % | 157.1 KB | +0.1 % |
| 384 bits | todas, M = 0.08 | 3.72 MB | −0.1 % | 23.70 | −0.1 % | 156.9 KB | −0.1 % |
| 384 bits | una por posting de destino, M = 0.06 | 3.21 MB | −13.7 % | 24.81 | +4.6 % | 129.5 KB | −17.5 % |
| 384 bits | cap 8, M = 0.06 | 3.20 MB | −14.1 % | 25.38 | +7.0 % | 126.0 KB | −19.8 % |
| 384 bits | una por posting de destino y cap 8, M = 0.04 (la mejor combinación) | 2.94 MB | −21.0 % | 24.76 | +4.4 % | 118.8 KB | −24.3 % |
| 512 bits | base | 3.31 MB | — | 18.12 | — | 182.5 KB | — |
| 512 bits | todas, M = 0.04 | 3.37 MB | +1.9 % | 18.43 | +1.7 % | 182.7 KB | +0.1 % |
| 512 bits | todas, M = 0.08 | 3.32 MB | +0.2 % | 18.19 | +0.4 % | 182.3 KB | −0.1 % |
| 512 bits | una por posting de destino, M = 0.06 | 2.92 MB | −11.7 % | 19.63 | +8.3 % | 148.7 KB | −18.5 % |
| 512 bits | cap 8, M = 0.06 | 2.95 MB | −10.7 % | 20.49 | +13.1 % | 144.1 KB | −21.0 % |
| 512 bits | una por posting de destino y cap 8, M = 0.08 (la mejor combinación) | 2.75 MB | −16.7 % | 20.36 | +12.3 % | 135.3 KB | −25.9 % |

### Recall, cargas y rondas a igual ef, frente a la base

**Sketch de 384 bits,** con rescoreLimit 64:

| ef | base: recall | cargas | rondas | todas, M = 0.04: recall | cargas | todas, M = 0.08: recall | cargas | una por posting de destino, M = 0.06: recall | cargas | cap 8, M = 0.06: recall | cargas | una por posting de destino y cap 8, M = 0.04: recall | cargas |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 0.9182 | 15.34 | 3.59 | 0.9117 (−0.0064) | 13.90 (0.91x) | 0.9195 (+0.0013) | 15.95 (1.04x) | 0.9116 (−0.0065) | 14.95 (0.97x) | 0.9110 (−0.0072) | 14.86 (0.97x) | 0.9013 (−0.0169) | 13.19 (0.86x) |
| 14 | 0.9341 | 18.58 | 4.40 | 0.9286 (−0.0054) | 16.84 (0.91x) | 0.9356 (+0.0015) | 19.32 (1.04x) | 0.9281 (−0.0059) | 17.92 (0.96x) | 0.9270 (−0.0071) | 17.83 (0.96x) | 0.9189 (−0.0152) | 15.83 (0.85x) |
| 20 | 0.9482 | 22.93 | 5.48 | 0.9438 (−0.0044) | 20.77 (0.91x) | 0.9505 (+0.0023) | 23.87 (1.04x) | 0.9435 (−0.0047) | 22.13 (0.97x) | 0.9418 (−0.0064) | 22.03 (0.96x) | 0.9355 (−0.0127) | 19.41 (0.85x) |
| 28 | 0.9596 | 28.34 | 6.84 | 0.9561 (−0.0034) | 25.74 (0.91x) | 0.9613 (+0.0017) | 29.46 (1.04x) | 0.9553 (−0.0042) | 27.22 (0.96x) | 0.9536 (−0.0059) | 27.02 (0.95x) | 0.9481 (−0.0115) | 23.88 (0.84x) |
| 40 | 0.9694 | 35.68 | 8.67 | 0.9662 (−0.0033) | 32.27 (0.90x) | 0.9706 (+0.0012) | 37.01 (1.04x) | 0.9664 (−0.0030) | 34.09 (0.96x) | 0.9643 (−0.0051) | 33.96 (0.95x) | 0.9603 (−0.0092) | 29.97 (0.84x) |
| 56 | 0.9770 | 44.26 | 10.82 | 0.9745 (−0.0025) | 40.13 (0.91x) | 0.9782 (+0.0011) | 45.95 (1.04x) | 0.9741 (−0.0029) | 42.35 (0.96x) | 0.9725 (−0.0046) | 42.13 (0.95x) | 0.9686 (−0.0084) | 37.22 (0.84x) |
| 80 | 0.9826 | 56.02 | 13.76 | 0.9804 (−0.0022) | 50.68 (0.90x) | 0.9834 (+0.0008) | 58.24 (1.04x) | 0.9800 (−0.0026) | 53.56 (0.96x) | 0.9781 (−0.0045) | 53.16 (0.95x) | 0.9755 (−0.0072) | 46.91 (0.84x) |
| 113 | 0.9863 | 70.65 | 17.41 | 0.9848 (−0.0015) | 63.94 (0.91x) | 0.9870 (+0.0007) | 73.46 (1.04x) | 0.9846 (−0.0018) | 67.52 (0.96x) | 0.9832 (−0.0032) | 67.00 (0.95x) | 0.9807 (−0.0056) | 59.22 (0.84x) |
| 160 | 0.9899 | 89.55 | 22.14 | 0.9885 (−0.0014) | 81.10 (0.91x) | 0.9902 (+0.0003) | 92.96 (1.04x) | 0.9882 (−0.0017) | 85.33 (0.95x) | 0.9866 (−0.0033) | 84.72 (0.95x) | 0.9853 (−0.0046) | 74.86 (0.84x) |
| 226 | 0.9921 | 113.71 | 28.18 | 0.9911 (−0.0011) | 102.89 (0.90x) | 0.9923 (+0.0002) | 118.00 (1.04x) | 0.9906 (−0.0016) | 108.41 (0.95x) | 0.9896 (−0.0025) | 107.73 (0.95x) | 0.9883 (−0.0038) | 95.00 (0.84x) |
| 320 | 0.9940 | 144.87 | 35.97 | 0.9934 (−0.0006) | 131.04 (0.90x) | 0.9942 (+0.0002) | 150.22 (1.04x) | 0.9928 (−0.0012) | 138.05 (0.95x) | 0.9920 (−0.0020) | 137.37 (0.95x) | 0.9910 (−0.0029) | 120.98 (0.84x) |
| 452 | 0.9955 | 184.90 | 45.98 | 0.9950 (−0.0005) | 167.55 (0.91x) | 0.9957 (+0.0002) | 191.60 (1.04x) | 0.9945 (−0.0010) | 176.18 (0.95x) | 0.9942 (−0.0013) | 175.45 (0.95x) | 0.9933 (−0.0022) | 154.59 (0.84x) |
| 640 | 0.9965 | 236.99 | 59.00 | 0.9962 (−0.0003) | 215.15 (0.91x) | 0.9967 (+0.0002) | 245.28 (1.03x) | 0.9959 (−0.0006) | 225.84 (0.95x) | 0.9957 (−0.0008) | 225.12 (0.95x) | 0.9950 (−0.0015) | 198.80 (0.84x) |
| 1024 | 0.9977 | 332.30 | 82.83 | 0.9972 (−0.0004) | 302.41 (0.91x) | 0.9978 (+0.0001) | 343.26 (1.03x) | 0.9972 (−0.0005) | 317.04 (0.95x) | 0.9969 (−0.0008) | 316.45 (0.95x) | 0.9962 (−0.0015) | 279.74 (0.84x) |
| 2048 | 0.9986 | 546.59 | 136.40 | 0.9984 (−0.0002) | 500.70 (0.92x) | 0.9986 (+0.0001) | 562.08 (1.03x) | 0.9983 (−0.0003) | 522.78 (0.96x) | 0.9981 (−0.0005) | 522.86 (0.96x) | 0.9977 (−0.0008) | 465.24 (0.85x) |

**Sketch de 512 bits,** con rescoreLimit 64:

| ef | base: recall | cargas | rondas | todas, M = 0.04: recall | cargas | todas, M = 0.08: recall | cargas | una por posting de destino, M = 0.06: recall | cargas | cap 8, M = 0.06: recall | cargas | una por posting de destino y cap 8, M = 0.08: recall | cargas |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 0.9180 | 12.29 | 2.82 | 0.9127 (−0.0052) | 11.56 (0.94x) | 0.9201 (+0.0021) | 12.54 (1.02x) | 0.9095 (−0.0085) | 12.02 (0.98x) | 0.9097 (−0.0083) | 12.07 (0.98x) | 0.9076 (−0.0104) | 12.18 (0.99x) |
| 14 | 0.9340 | 14.69 | 3.42 | 0.9291 (−0.0050) | 13.81 (0.94x) | 0.9353 (+0.0013) | 14.95 (1.02x) | 0.9269 (−0.0071) | 14.33 (0.98x) | 0.9254 (−0.0086) | 14.39 (0.98x) | 0.9254 (−0.0086) | 14.50 (0.99x) |
| 20 | 0.9498 | 18.05 | 4.26 | 0.9453 (−0.0044) | 16.97 (0.94x) | 0.9508 (+0.0010) | 18.38 (1.02x) | 0.9426 (−0.0072) | 17.49 (0.97x) | 0.9409 (−0.0089) | 17.53 (0.97x) | 0.9414 (−0.0084) | 17.68 (0.98x) |
| 28 | 0.9604 | 22.06 | 5.27 | 0.9566 (−0.0038) | 20.70 (0.94x) | 0.9615 (+0.0011) | 22.52 (1.02x) | 0.9553 (−0.0051) | 21.31 (0.97x) | 0.9523 (−0.0081) | 21.31 (0.97x) | 0.9536 (−0.0068) | 21.58 (0.98x) |
| 40 | 0.9696 | 27.58 | 6.65 | 0.9661 (−0.0035) | 25.92 (0.94x) | 0.9709 (+0.0013) | 28.13 (1.02x) | 0.9654 (−0.0042) | 26.67 (0.97x) | 0.9631 (−0.0065) | 26.51 (0.96x) | 0.9647 (−0.0049) | 26.93 (0.98x) |
| 56 | 0.9767 | 34.09 | 8.28 | 0.9740 (−0.0028) | 32.07 (0.94x) | 0.9775 (+0.0008) | 34.80 (1.02x) | 0.9733 (−0.0034) | 32.88 (0.96x) | 0.9716 (−0.0052) | 32.71 (0.96x) | 0.9722 (−0.0045) | 33.09 (0.97x) |
| 80 | 0.9816 | 42.97 | 10.49 | 0.9800 (−0.0017) | 40.48 (0.94x) | 0.9825 (+0.0009) | 43.85 (1.02x) | 0.9792 (−0.0025) | 41.37 (0.96x) | 0.9777 (−0.0040) | 41.17 (0.96x) | 0.9785 (−0.0031) | 41.66 (0.97x) |
| 113 | 0.9856 | 54.11 | 13.28 | 0.9842 (−0.0014) | 51.06 (0.94x) | 0.9861 (+0.0005) | 55.17 (1.02x) | 0.9836 (−0.0020) | 52.20 (0.96x) | 0.9821 (−0.0035) | 51.78 (0.96x) | 0.9829 (−0.0027) | 52.41 (0.97x) |
| 160 | 0.9885 | 68.55 | 16.89 | 0.9874 (−0.0012) | 64.55 (0.94x) | 0.9891 (+0.0006) | 69.89 (1.02x) | 0.9871 (−0.0014) | 65.92 (0.96x) | 0.9852 (−0.0033) | 65.52 (0.96x) | 0.9863 (−0.0023) | 66.15 (0.96x) |
| 226 | 0.9915 | 87.15 | 21.54 | 0.9905 (−0.0010) | 82.03 (0.94x) | 0.9916 (+0.0002) | 88.85 (1.02x) | 0.9904 (−0.0011) | 83.69 (0.96x) | 0.9890 (−0.0025) | 83.13 (0.95x) | 0.9897 (−0.0018) | 83.86 (0.96x) |
| 320 | 0.9937 | 111.02 | 27.51 | 0.9929 (−0.0009) | 104.52 (0.94x) | 0.9939 (+0.0002) | 113.17 (1.02x) | 0.9926 (−0.0012) | 106.56 (0.96x) | 0.9918 (−0.0019) | 106.00 (0.95x) | 0.9919 (−0.0018) | 106.82 (0.96x) |
| 452 | 0.9951 | 142.03 | 35.26 | 0.9945 (−0.0006) | 133.66 (0.94x) | 0.9952 (+0.0001) | 144.75 (1.02x) | 0.9941 (−0.0010) | 136.36 (0.96x) | 0.9933 (−0.0019) | 135.59 (0.95x) | 0.9936 (−0.0015) | 136.76 (0.96x) |
| 640 | 0.9962 | 182.46 | 45.37 | 0.9957 (−0.0005) | 171.91 (0.94x) | 0.9963 (+0.0001) | 185.81 (1.02x) | 0.9956 (−0.0006) | 175.09 (0.96x) | 0.9949 (−0.0013) | 174.19 (0.95x) | 0.9952 (−0.0010) | 175.44 (0.96x) |
| 1024 | 0.9973 | 256.60 | 63.90 | 0.9971 (−0.0002) | 242.12 (0.94x) | 0.9975 (+0.0002) | 261.08 (1.02x) | 0.9968 (−0.0005) | 246.23 (0.96x) | 0.9965 (−0.0008) | 245.49 (0.96x) | 0.9964 (−0.0009) | 246.78 (0.96x) |
| 2048 | 0.9983 | 427.95 | 106.74 | 0.9981 (−0.0002) | 404.91 (0.95x) | 0.9984 (+0.0000) | 434.79 (1.02x) | 0.9980 (−0.0003) | 411.35 (0.96x) | 0.9979 (−0.0004) | 410.81 (0.96x) | 0.9979 (−0.0005) | 412.16 (0.96x) |

### Endpoints puntuados, de las entradas de halo leídas

| sketch | aristas | M | a 0.95: entradas leídas | endpoints puntuados | parte | a 0.99: entradas leídas | endpoints puntuados | parte |
|---|---|---|---|---|---|---|---|---|
| 384 bits | todas | 0.04 | 36617 | 3558 | 10 % | 145640 | 15163 | 10 % |
| 384 bits | todas | 0.06 | 37463 | 7592 | 20 % | 141972 | 28242 | 20 % |
| 384 bits | todas | 0.08 | 37391 | 12653 | 34 % | 143695 | 45518 | 32 % |
| 384 bits | una por posting de destino | 0.04 | 28725 | 3010 | 10 % | 120308 | 13603 | 11 % |
| 384 bits | una por posting de destino | 0.06 | 29368 | 6403 | 22 % | 119399 | 25572 | 21 % |
| 384 bits | una por posting de destino | 0.08 | 29552 | 10697 | 36 % | 120051 | 40590 | 34 % |
| 384 bits | cap 8 | 0.04 | 27482 | 3007 | 11 % | 121538 | 14586 | 12 % |
| 384 bits | cap 8 | 0.06 | 28294 | 6362 | 22 % | 122664 | 27708 | 23 % |
| 384 bits | cap 8 | 0.08 | 28612 | 10614 | 37 % | 123688 | 43853 | 35 % |
| 384 bits | una por posting de destino y cap 8 | 0.04 | 25227 | 2779 | 11 % | 110418 | 13298 | 12 % |
| 384 bits | una por posting de destino y cap 8 | 0.06 | 25790 | 5871 | 23 % | 109894 | 24954 | 23 % |
| 384 bits | una por posting de destino y cap 8 | 0.08 | 25896 | 9745 | 38 % | 110683 | 39544 | 36 % |
| 512 bits | todas | 0.04 | 29207 | 3308 | 11 % | 124398 | 15338 | 12 % |
| 512 bits | todas | 0.06 | 28654 | 6720 | 23 % | 121714 | 28145 | 23 % |
| 512 bits | todas | 0.08 | 28718 | 10927 | 38 % | 119456 | 42127 | 35 % |
| 512 bits | una por posting de destino | 0.04 | 23179 | 2823 | 12 % | 99963 | 13287 | 13 % |
| 512 bits | una por posting de destino | 0.06 | 23268 | 5857 | 25 % | 95579 | 23715 | 25 % |
| 512 bits | una por posting de destino | 0.08 | 23267 | 9411 | 40 % | 95897 | 35999 | 38 % |
| 512 bits | cap 8 | 0.04 | 22862 | 2913 | 13 % | 104078 | 14654 | 14 % |
| 512 bits | cap 8 | 0.06 | 22914 | 5920 | 26 % | 100112 | 26111 | 26 % |
| 512 bits | cap 8 | 0.08 | 22750 | 9371 | 41 % | 99739 | 39116 | 39 % |
| 512 bits | una por posting de destino y cap 8 | 0.04 | 20842 | 2663 | 13 % | 92352 | 13040 | 14 % |
| 512 bits | una por posting de destino y cap 8 | 0.06 | 20831 | 5466 | 26 % | 88048 | 23100 | 26 % |
| 512 bits | una por posting de destino y cap 8 | 0.08 | 20770 | 8699 | 42 % | 87472 | 34611 | 40 % |

### La referencia con sidecar residente, en la misma corrida

Ruteo con el código completo, todas las aristas y M = 0.06. Lee los miembros y las aristas de los postings que carga; el sidecar, 198.00 MB, está en RAM. No entra en el criterio.

| | a 0.95: cargas | rondas | bytes | a 0.99: cargas | rondas | bytes | disco total |
|---|---|---|---|---|---|---|---|
| sidecar residente | 10.11 | 2.28 | 0.63 MB | 52.21 | 12.80 | 3.23 MB | 241.47 MB |
| 384 bits, base | 23.72 (2.34x) | 5.68 | 3.72 MB (5.87x) | 90.70 (1.74x) | 22.43 | 14.11 MB (4.37x) | 573.20 MB (2.37x) |
| 384 bits, una por posting de destino y cap 8, M = 0.04 | 24.76 (2.45x) | 5.94 | 2.94 MB (4.63x) | 110.38 (2.11x) | 27.34 | 12.91 MB (4.00x) | 436.63 MB (1.81x) |
| 384 bits, una por posting de destino y cap 8, M = 0.06 | 25.33 (2.50x) | 6.08 | 3.01 MB (4.74x) | 109.76 (2.10x) | 27.19 | 12.85 MB (3.98x) | 436.63 MB (1.81x) |
| 512 bits, base | 18.12 (1.79x) | 4.28 | 3.31 MB (5.21x) | 77.36 (1.48x) | 19.09 | 14.02 MB (4.34x) | 661.67 MB (2.74x) |
| 512 bits, una por posting de destino y cap 8, M = 0.08 | 20.36 (2.01x) | 4.84 | 2.75 MB (4.34x) | 86.65 (1.66x) | 21.41 | 11.60 MB (3.59x) | 492.98 MB (2.04x) |

### La regla de ruido

- **Cada celda se compara con la base de su ancho,** dentro de la corrida, sobre el mismo build y con las mismas queries.
- **Nada a 0.99 se compara con otra corrida.** Las cifras de v3.0 no entran en el veredicto.
- **El listón de 2.46 MB es de otra fuente,** una simulación con otra contabilidad (`disk-design.md`, «Contexto: frente a HFresh»): se usa tal cual.
