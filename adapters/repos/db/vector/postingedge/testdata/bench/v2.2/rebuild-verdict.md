### Estado

| estado | postings | tamaño medio (p50 / p95 / máx) | cobertura | recall a ef 10 | cargas a 0.95 | frente al lote | cargas a 0.99 | frente al lote | rondas a 0.99 | endpoints a 0.99 |
|---|---|---|---|---|---|---|---|---|---|---|
| lote del 100 % | 4195 | 236.0 (234 / 380 / 786) | — | 0.9229 | 10.06 | +0.0 % | 52.78 | +0.0 % | 12.94 | 26634 |
| primera corrida, al 100 % | 4077 | 242.8 (244 / 345 / 360) | complete | 0.9192 | 10.99 | +9.2 % | 62.21 | +17.9 % | 15.30 | 31806 |
| esta corrida, al 100 %: antes | 4094 | 241.8 (241 / 344 / 360) | complete | 0.9199 | 11.11 | +10.4 % | 69.69 | +32.0 % | 17.17 | 35139 |
| aristas reconstruidas | 4094 | 241.8 (241 / 344 / 360) | complete | 0.9254 | 10.36 | +3.0 % | 52.39 | -0.8 % | 12.84 | 27611 |
| aristas y grafo de centroides reconstruidos | 4094 | 241.8 (241 / 344 / 360) | complete | 0.9261 | 10.31 | +2.4 % | 52.37 | -0.8 % | 12.84 | 27599 |

### La regla

- **El lote carga 52.78 postings a 0.99.** +10 % son 58.06 cargas.
- **La primera corrida cargaba 62.21 (+17.9 %).** Su banda de ruido, de ±3.0 %, va de 60.34 a 64.07 cargas.
- **Esta corrida, antes de reconstruir, carga 69.69 (+32.0 % frente al lote):** +12.0 % frente a la primera, más que el suelo de ruido de v2.0.
- **Con las aristas reconstruidas carga 52.39 (-0.8 % frente al lote).**
- **Lectura por la regla:** a +10 % o menos del lote: **el déficit es de las aristas**.
- **Parte recuperada de las cargas a 0.99:** 102 %, de 69.69 a 52.39 con el lote en 52.78.
- **Con el grafo de centroides reconstruido además, informativo:** 52.37 (-0.8 % frente al lote); recuperado desde antes, 102 %.

### Recall a igual ef de búsqueda, diferencia frente al lote

| ef de búsqueda | lote | primera corrida, al 100 % | esta corrida, al 100 %: antes | aristas reconstruidas | aristas y grafo de centroides reconstruidos |
|---|---|---|---|---|---|
| 10 | 0.9229 | -0.0037 | -0.0031 | +0.0025 | +0.0032 |
| 20 | 0.9506 | -0.0021 | -0.0023 | +0.0029 | +0.0034 |
| 40 | 0.9664 | -0.0023 | -0.0021 | +0.0034 | +0.0037 |
| 80 | 0.9782 | -0.0021 | -0.0023 | +0.0021 | +0.0021 |
| 160 | 0.9858 | -0.0015 | -0.0024 | +0.0009 | +0.0009 |
| 320 | 0.9908 | -0.0010 | -0.0020 | +0.0008 | +0.0008 |
| 640 | 0.9940 | -0.0008 | -0.0007 | +0.0011 | +0.0011 |
| 1024 | 0.9960 | -0.0005 | -0.0008 | +0.0008 | +0.0008 |
| peor | | -0.0037 | -0.0031 | +0.0008 | +0.0008 |

### Cargas de posting a igual ef de búsqueda, frente al lote

| ef de búsqueda | lote | primera corrida, al 100 % | esta corrida, al 100 %: antes | aristas reconstruidas | aristas y grafo de centroides reconstruidos |
|---|---|---|---|---|---|
| 10 | 7.58 | 7.90 (+4.2 %) | 8.01 (+5.8 %) | 7.99 (+5.4 %) | 8.00 (+5.6 %) |
| 20 | 10.13 | 10.60 (+4.7 %) | 10.70 (+5.7 %) | 10.75 (+6.2 %) | 10.75 (+6.2 %) |
| 40 | 14.56 | 15.37 (+5.6 %) | 15.45 (+6.2 %) | 15.60 (+7.2 %) | 15.60 (+7.2 %) |
| 80 | 22.12 | 23.56 (+6.5 %) | 23.59 (+6.6 %) | 23.84 (+7.8 %) | 23.83 (+7.7 %) |
| 160 | 35.18 | 37.41 (+6.3 %) | 37.58 (+6.8 %) | 37.80 (+7.5 %) | 37.79 (+7.4 %) |
| 320 | 57.27 | 60.72 (+6.0 %) | 61.21 (+6.9 %) | 61.54 (+7.5 %) | 61.53 (+7.4 %) |
| 640 | 95.54 | 101.00 (+5.7 %) | 101.67 (+6.4 %) | 102.16 (+6.9 %) | 102.15 (+6.9 %) |
| 1024 | 136.38 | 144.32 (+5.8 %) | 145.14 (+6.4 %) | 145.82 (+6.9 %) | 145.82 (+6.9 %) |

### Parte recuperada, de antes a después

La parte de la distancia al lote que cierra la reconstrucción: (antes − después) / (antes − lote). Por encima del 100 % queda mejor que el lote; en negativo, peor que antes.

| | lote | antes | aristas reconstruidas | recuperado | aristas y grafo de centroides reconstruidos | recuperado |
|---|---|---|---|---|---|---|
| cargas a 0.95 | 10.06 | 11.11 | 10.36 | 72 % | 10.31 | 77 % |
| rondas a 0.95 | 2.27 | 2.53 | 2.34 | 71 % | 2.33 | 77 % |
| endpoints a 0.95 | 4952.57 | 5455.39 | 5165.40 | 58 % | 5132.92 | 64 % |
| cargas a 0.99 | 52.78 | 69.69 | 52.39 | 102 % | 52.37 | 102 % |
| rondas a 0.99 | 12.94 | 17.17 | 12.84 | 102 % | 12.84 | 103 % |
| endpoints a 0.99 | 26634.11 | 35138.86 | 27611.46 | 89 % | 27599.46 | 89 % |
| recall a ef 10 | 0.9229 | 0.9199 | 0.9254 | 182 % | 0.9261 | 204 % |
| recall a ef 20 | 0.9506 | 0.9484 | 0.9535 | 228 % | 0.9540 | 250 % |
| recall a ef 40 | 0.9664 | 0.9643 | 0.9698 | 263 % | 0.9702 | 278 % |
| recall a ef 80 | 0.9782 | 0.9759 | 0.9802 | 190 % | 0.9803 | 192 % |
| recall a ef 160 | 0.9858 | 0.9834 | 0.9867 | 137 % | 0.9867 | 137 % |
| recall a ef 320 | 0.9908 | 0.9889 | 0.9916 | 139 % | 0.9916 | 139 % |
| recall a ef 640 | 0.9940 | 0.9933 | 0.9951 | 257 % | 0.9951 | 257 % |
| recall a ef 1024 | 0.9960 | 0.9952 | 0.9968 | 203 % | 0.9968 | 203 % |
| cargas a ef 10 | 7.58 | 8.01 | 7.99 | 6 % | 8.00 | 3 % |
| cargas a ef 20 | 10.13 | 10.70 | 10.75 | -9 % | 10.75 | -9 % |
| cargas a ef 40 | 14.56 | 15.45 | 15.60 | -16 % | 15.60 | -16 % |
| cargas a ef 80 | 22.12 | 23.59 | 23.84 | -17 % | 23.83 | -16 % |
| cargas a ef 160 | 35.18 | 37.58 | 37.80 | -9 % | 37.79 | -9 % |
| cargas a ef 320 | 57.27 | 61.21 | 61.54 | -8 % | 61.53 | -8 % |
| cargas a ef 640 | 95.54 | 101.67 | 102.16 | -8 % | 102.15 | -8 % |
| cargas a ef 1024 | 136.38 | 145.14 | 145.82 | -8 % | 145.82 | -8 % |

### Grado por cohorte

| estado | lote | 10 → 20 % | 20 → 40 % | 40 → 60 % | 60 → 80 % | 80 → 100 % | todos |
|---|---|---|---|---|---|---|---|
| esta corrida, al 100 %: antes | 7.63 | 8.07 | 8.66 | 9.04 | 9.27 | 9.44 | 8.85 |
| aristas reconstruidas | 10.26 | 10.13 | 9.85 | 9.71 | 9.61 | 9.57 | 9.79 |
| aristas y grafo de centroides reconstruidos | 10.26 | 10.13 | 9.85 | 9.71 | 9.61 | 9.57 | 9.79 |
| lote del 100 % |  |  |  |  |  |  | 8.98 |

### Recall de arista contra el corpus actual

De la muestra de cada grupo, a nivel de vector y, entre paréntesis, de posting, con su grado.

| estado | insertados: arista (posting) | grado | vectores del lote: arista (posting) | grado |
|---|---|---|---|---|
| primera corrida, al 100 % | 0.7061 (0.7800) | 8.86 | 0.4855 (0.5959) | 7.54 |
| esta corrida, al 100 %: antes | 0.7065 (0.7800) | 8.88 | 0.4867 (0.5961) | 7.54 |
| aristas reconstruidas | 0.9697 (0.9555) | 9.69 | 0.9688 (0.9508) | 10.18 |
| aristas y grafo de centroides reconstruidos | 0.9697 (0.9555) | 9.69 | 0.9688 (0.9508) | 10.18 |

### La partición al final, y el trabajo de la reconstrucción

- **Vectores que no están en el posting de su centroide más cercano:** 171128 de 990000, el 17.29 %. Fuera de sus 256 centroides más cercanos: 1.
- **Aristas:** 9689587, 9.79 por vector.
- **Trabajo, en millones:** 253.4 postings sondeados, 62989 candidatos ordenados por código, 63.4 rescoreados y 4053.1 distancias exactas de la asignación. En 558 s, indicativo.
- **El lote del 100 %, en millones:** 253.4 postings sondeados, 51008 candidatos por código, 63.4 rescoreados y 4153.1 distancias de la asignación.

### La celda repetida frente a la primera corrida, al 100 %

| | primera corrida | esta corrida | diferencia | suelo de ruido de v2.0 |
|---|---|---|---|---|
| recall a ef 10 | 0.9192 | 0.9199 | +0.0007, dentro | 0.0009 |
| recall a ef 20 | 0.9485 | 0.9484 | -0.0001, dentro | 0.0009 |
| recall a ef 40 | 0.9641 | 0.9643 | +0.0002, dentro | 0.0009 |
| recall a ef 80 | 0.9761 | 0.9759 | -0.0002, dentro | 0.0009 |
| recall a ef 160 | 0.9843 | 0.9834 | -0.0009 | 0.0009 |
| recall a ef 320 | 0.9898 | 0.9889 | -0.0010 | 0.0009 |
| recall a ef 640 | 0.9932 | 0.9933 | +0.0001, dentro | 0.0009 |
| recall a ef 1024 | 0.9954 | 0.9952 | -0.0003, dentro | 0.0009 |
| cargas a 0.95 | 10.99 | 11.11 | +1.2 % | 0.5 % |
| cargas a 0.99 | 62.21 | 69.69 | +12.0 % | 3.0 % |
| postings | 4077 | 4094 | +0.4 % | |
| tamaño medio | 242.8 | 241.8 | -0.4 % | |
| splits | 3657 | 3674 | +0.5 % | |
| no-nearest, acumulado | 0.02681 | 0.02689 | +0.3 % | |
| cargas por insert | 21.52 | 21.54 | +0.1 % | |
| escrituras inversas por insert | 4.521 | 4.538 | +0.4 % | |
| grado, todo el índice | 8.835 | 8.853 | +0.2 % | |

