### Estado

| estado | vectores | cargas a 0.95 | cargas a 0.99 | cruza 0.99 entre | recall en esos ef | rondas a 0.99 | endpoints a 0.99 | aristas por vector | recall de arista: insertados / lote del 10 % | cobertura | listas con una arista repetida |
|---|---|---|---|---|---|---|---|---|---|---|---|
| lote del 100 % (v2.0) | 990000 | 10.06 | 52.78 | ef 160 y 320 | 0.9858 y 0.9908 | 12.94 | 26634 | — | — | — | — |
| lote del 10 % | 99000 | 6.39 | 16.30 | ef 40 y 80 | 0.9847 y 0.9917 | 3.82 | 6058 | 7.63 | — / 0.9935 | complete | 0 |
| corpus de 200000, antes de su tanda | 200000 | 7.60 | 23.92 | ef 80 y 160 | 0.9879 y 0.9926 | 5.72 | 10067 | 7.85 | 0.8080 / 0.6601 | complete | 0 |
| corpus de 200000, después de su tanda | 200000 | 7.25 | 23.75 | ef 80 y 160 | 0.9879 y 0.9927 | 5.68 | 10119 | 8.28 | 0.8196 / 0.9883 | complete | 0 |
| corpus de 400000, antes de su tanda | 400000 | 8.89 | 36.06 | ef 160 y 320 | 0.9893 y 0.9937 | 8.76 | 16662 | 8.48 | 0.7337 / 0.6762 | complete | 0 |
| corpus de 400000, después de su tanda | 400000 | 8.67 | 34.47 | ef 160 y 320 | 0.9898 y 0.9934 | 8.37 | 16185 | 8.95 | 0.8737 / 0.9842 | complete | 0 |
| corpus de 800000, antes de su tanda | 800000 | 10.11 | 50.45 | ef 160 y 320 | 0.9868 y 0.9916 | 12.36 | 25592 | 9.05 | 0.7331 / 0.6902 | complete | 0 |
| corpus de 800000, después de su tanda | 800000 | 9.93 | 46.86 | ef 160 y 320 | 0.9877 y 0.9922 | 11.46 | 24173 | 9.47 | 0.8956 / 0.9804 | complete | 0 |
| al 100 % | 990000 | 10.67 (+6.1 %) | 56.88 (+7.8 %) | ef 160 y 320 | 0.9857 y 0.9908 | 13.97 | 29628 | 9.47 | 0.8141 / 0.8626 | complete | 0 |
| referencia interna: aristas reconstruidas en lote | 990000 | 10.36 (+3.0 %) | 47.89 (-9.3 %) | ef 160 y 320 | 0.9877 y 0.9924 | 11.72 | 25369 | 9.77 | 0.9688 / 0.9685 | complete | 0 |
| informativo: todo refrescado | 990000 | 10.43 (+3.7 %) | 52.03 (-1.4 %) | ef 160 y 320 | 0.9869 y 0.9916 | 12.75 | 27523 | 9.77 | 0.9806 / 0.9815 | complete | 0 |
| informativo: cada lista como la dejó su refresco | 990000 | 10.46 (+3.9 %) | 52.47 (-0.6 %) | ef 160 y 320 | 0.9868 y 0.9915 | 12.86 | 27722 | 9.77 | 0.9798 / 0.9800 | complete | 0 |

Entre paréntesis, frente al lote del 100 %. Los estados de un corpus menor no tienen lote con el que compararse.

### El criterio

- **El lote carga 52.78 postings a 0.99.** +10 % son 58.06 cargas.
- **Al 100 %, con las 3 tandas y sin refresco final, carga 56.88 (+7.8 % frente al lote):** **cumple** el criterio.
  - Cruza 0.99 entre ef 160 y ef 320, con recall 0.9857 y 0.9908. El lote, entre ef 160 y ef 320.
  - En ese tramo, 0.001 de recall son 4.7 cargas.
  - A 0.95 carga 10.67 (+6.1 % frente al lote).
- **Referencia interna, con las aristas reconstruidas en lote sobre la misma partición:** 47.89 a 0.99 (-9.3 % frente al lote) y 10.36 a 0.95.
  - El 100 % de la corrida carga un 18.8 % más que su referencia interna a 0.99, y un 3.0 % más a 0.95.

### Cada tanda, de su antes a su después

Sobre el mismo índice y contra los vecinos exactos del corpus de ese momento.

| tanda | corpus | vectores refrescados | cargas a 0.95: antes → después | cargas a 0.99: antes → después | cruza 0.99 entre: antes, después | recall a igual ef: lo que sube, del peor al mejor ef | aristas por vector: antes → después |
|---|---|---|---|---|---|---|---|
| 1 | 200000 | 100000 | 7.60 → 7.25 (-4.6 %) | 23.92 → 23.75 (-0.7 %) | ef 80 y 160, ef 80 y 160 | de -0.0001 a +0.0043 | 7.85 → 8.28 |
| 2 | 400000 | 200000 | 8.89 → 8.67 (-2.5 %) | 36.06 → 34.47 (-4.4 %) | ef 160 y 320, ef 160 y 320 | de -0.0003 a +0.0033 | 8.48 → 8.95 |
| 3 | 800000 | 400000 | 10.11 → 9.93 (-1.7 %) | 50.45 → 46.86 (-7.1 %) | ef 160 y 320, ef 160 y 320 | de +0.0004 a +0.0016 | 9.05 → 9.47 |

**Tanda 1, con 200000 vectores:** recall y cargas a igual ef.

| ef de búsqueda | recall antes | recall después | lo que sube | cargas antes | cargas después |
|---|---|---|---|---|---|
| 10 | 0.9421 | 0.9464 | +0.0043 | 6.94 | 6.92 |
| 20 | 0.9669 | 0.9680 | +0.0011 | 9.24 | 9.16 |
| 40 | 0.9804 | 0.9806 | +0.0002 | 12.99 | 12.95 |
| 80 | 0.9879 | 0.9879 | +0.0000 | 19.46 | 19.48 |
| 160 | 0.9926 | 0.9927 | +0.0002 | 30.70 | 30.75 |
| 320 | 0.9955 | 0.9956 | +0.0001 | 50.23 | 50.46 |
| 640 | 0.9976 | 0.9974 | -0.0001 | 84.01 | 84.19 |
| 1024 | 0.9982 | 0.9982 | +0.0000 | 119.69 | 120.08 |

**Tanda 2, con 400000 vectores:** recall y cargas a igual ef.

| ef de búsqueda | recall antes | recall después | lo que sube | cargas antes | cargas después |
|---|---|---|---|---|---|
| 10 | 0.9314 | 0.9346 | +0.0033 | 7.37 | 7.34 |
| 20 | 0.9606 | 0.9621 | +0.0015 | 9.90 | 9.89 |
| 40 | 0.9740 | 0.9756 | +0.0016 | 14.06 | 14.09 |
| 80 | 0.9829 | 0.9838 | +0.0009 | 21.21 | 21.23 |
| 160 | 0.9893 | 0.9898 | +0.0005 | 33.42 | 33.55 |
| 320 | 0.9937 | 0.9934 | -0.0003 | 54.59 | 54.69 |
| 640 | 0.9964 | 0.9964 | +0.0000 | 91.06 | 91.29 |
| 1024 | 0.9975 | 0.9974 | -0.0001 | 130.26 | 130.68 |

**Tanda 3, con 800000 vectores:** recall y cargas a igual ef.

| ef de búsqueda | recall antes | recall después | lo que sube | cargas antes | cargas después |
|---|---|---|---|---|---|
| 10 | 0.9260 | 0.9272 | +0.0012 | 7.89 | 7.83 |
| 20 | 0.9537 | 0.9553 | +0.0016 | 10.50 | 10.50 |
| 40 | 0.9695 | 0.9706 | +0.0012 | 15.12 | 15.16 |
| 80 | 0.9798 | 0.9805 | +0.0007 | 23.05 | 23.07 |
| 160 | 0.9868 | 0.9877 | +0.0009 | 36.51 | 36.62 |
| 320 | 0.9916 | 0.9922 | +0.0006 | 59.25 | 59.45 |
| 640 | 0.9944 | 0.9952 | +0.0007 | 98.62 | 98.98 |
| 1024 | 0.9964 | 0.9967 | +0.0004 | 140.89 | 141.25 |

### Recall a igual ef de búsqueda, diferencia frente al lote

| ef de búsqueda | lote | al 100 % | reconstruidas en lote | todo refrescado | sin las inversas |
|---|---|---|---|---|---|
| 10 | 0.9229 | +0.0002 | +0.0018 | +0.0009 | +0.0007 |
| 20 | 0.9506 | -0.0000 | +0.0032 | +0.0020 | +0.0017 |
| 40 | 0.9664 | +0.0008 | +0.0029 | +0.0025 | +0.0023 |
| 80 | 0.9782 | +0.0001 | +0.0022 | +0.0020 | +0.0018 |
| 160 | 0.9858 | -0.0001 | +0.0019 | +0.0011 | +0.0010 |
| 320 | 0.9908 | -0.0001 | +0.0015 | +0.0008 | +0.0007 |
| 640 | 0.9940 | +0.0001 | +0.0013 | +0.0005 | +0.0005 |
| 1024 | 0.9960 | +0.0002 | +0.0010 | +0.0004 | +0.0004 |
| peor | | -0.0001 | +0.0010 | +0.0004 | +0.0004 |

### Cargas de posting a igual ef de búsqueda, frente al lote

| ef de búsqueda | lote | al 100 % | reconstruidas en lote | todo refrescado | sin las inversas |
|---|---|---|---|---|---|
| 10 | 7.58 | 8.01 (+5.7 %) | 8.01 (+5.7 %) | 7.98 (+5.3 %) | 7.98 (+5.3 %) |
| 20 | 10.13 | 10.74 (+6.0 %) | 10.77 (+6.3 %) | 10.71 (+5.8 %) | 10.71 (+5.8 %) |
| 40 | 14.56 | 15.48 (+6.4 %) | 15.48 (+6.4 %) | 15.49 (+6.4 %) | 15.49 (+6.4 %) |
| 80 | 22.12 | 23.64 (+6.9 %) | 23.78 (+7.5 %) | 23.76 (+7.4 %) | 23.75 (+7.4 %) |
| 160 | 35.18 | 37.56 (+6.8 %) | 37.77 (+7.3 %) | 37.76 (+7.3 %) | 37.74 (+7.3 %) |
| 320 | 57.27 | 61.21 (+6.9 %) | 61.50 (+7.4 %) | 61.53 (+7.4 %) | 61.48 (+7.4 %) |
| 640 | 95.54 | 101.79 (+6.5 %) | 102.10 (+6.9 %) | 102.13 (+6.9 %) | 102.05 (+6.8 %) |
| 1024 | 136.38 | 145.49 (+6.7 %) | 145.99 (+7.0 %) | 145.98 (+7.0 %) | 145.90 (+7.0 %) |

### Al 100 %, frente a la referencia interna

| ef de búsqueda | recall al 100 % | recall de la referencia interna | diferencia | cargas al 100 % | cargas de la referencia |
|---|---|---|---|---|---|
| 10 | 0.9231 | 0.9247 | -0.0016 | 8.01 | 8.01 |
| 20 | 0.9506 | 0.9538 | -0.0032 | 10.74 | 10.77 |
| 40 | 0.9672 | 0.9693 | -0.0021 | 15.48 | 15.48 |
| 80 | 0.9783 | 0.9804 | -0.0021 | 23.64 | 23.78 |
| 160 | 0.9857 | 0.9877 | -0.0020 | 37.56 | 37.77 |
| 320 | 0.9908 | 0.9924 | -0.0016 | 61.21 | 61.50 |
| 640 | 0.9941 | 0.9953 | -0.0012 | 101.79 | 102.10 |
| 1024 | 0.9962 | 0.9969 | -0.0008 | 145.49 | 145.99 |

| | al 100 % | referencia interna | diferencia |
|---|---|---|---|
| cargas a 0.95 | 10.67 | 10.36 | +3.0 % |
| cargas a 0.99 | 56.88 | 47.89 | +18.8 % |
| rondas a 0.99 | 13.97 | 11.72 | +19.2 % |
| endpoints a 0.99 | 29628 | 25369 | +16.8 % |

### Grado por cohorte

| estado | 0 → 99000 | 99000 → 200000 | 200000 → 400000 | 400000 → 800000 | 800000 → 990000 | todos |
|---|---|---|---|---|---|---|
| lote del 10 % | 7.63 | — | — | — | — | 7.63 |
| corpus de 200000, antes de su tanda | 7.63 | 8.06 | — | — | — | 7.85 |
| corpus de 200000, después de su tanda | 8.50 | 8.07 | — | — | — | 8.28 |
| corpus de 400000, antes de su tanda | 8.50 | 8.07 | 8.67 | — | — | 8.48 |
| corpus de 400000, después de su tanda | 9.31 | 9.16 | 8.67 | — | — | 8.95 |
| corpus de 800000, antes de su tanda | 9.31 | 9.16 | 8.67 | 9.15 | — | 9.05 |
| corpus de 800000, después de su tanda | 10.04 | 9.88 | 9.61 | 9.15 | — | 9.47 |
| al 100 % | 10.04 | 9.88 | 9.61 | 9.15 | 9.47 | 9.47 |
| referencia interna: aristas reconstruidas en lote | 10.26 | 10.11 | 9.83 | 9.63 | 9.59 | 9.77 |
| informativo: todo refrescado | 10.27 | 10.11 | 9.83 | 9.63 | 9.58 | 9.77 |
| informativo: cada lista como la dejó su refresco | 10.27 | 10.11 | 9.83 | 9.63 | 9.58 | 9.77 |

Cada cohorte, por el lugar en que entraron sus vectores. Al 100 %, los que entraron antes de 400000 llevan aristas de la última tanda, y los demás, las de su insert.

### Recall de arista contra el corpus actual

De la muestra de cada grupo, a nivel de vector y, entre paréntesis, de posting, con su grado.

| estado | insertados: arista (posting) | grado | vectores del lote del 10 %: arista (posting) | grado |
|---|---|---|---|---|
| lote del 10 % | — | — | 0.9935 (0.9915) | 7.54 |
| corpus de 200000, antes de su tanda | 0.8080 (0.8554) | 8.04 | 0.6601 (0.7737) | 7.54 |
| corpus de 200000, después de su tanda | 0.8196 (0.8691) | 8.05 | 0.9883 (0.9835) | 8.41 |
| corpus de 400000, antes de su tanda | 0.7337 (0.8043) | 8.26 | 0.6762 (0.7791) | 8.41 |
| corpus de 400000, después de su tanda | 0.8737 (0.9037) | 8.67 | 0.9842 (0.9755) | 9.27 |
| corpus de 800000, antes de su tanda | 0.7331 (0.8024) | 8.81 | 0.6902 (0.7766) | 9.27 |
| corpus de 800000, después de su tanda | 0.8956 (0.9136) | 9.28 | 0.9804 (0.9696) | 10.03 |
| al 100 % | 0.8141 (0.8589) | 9.34 | 0.8626 (0.8889) | 10.03 |
| referencia interna: aristas reconstruidas en lote | 0.9688 (0.9538) | 9.70 | 0.9685 (0.9499) | 10.20 |
| informativo: todo refrescado | 0.9806 (0.9735) | 9.70 | 0.9815 (0.9725) | 10.20 |
| informativo: cada lista como la dejó su refresco | 0.9798 (0.9720) | 9.70 | 0.9800 (0.9703) | 10.20 |

### El coste de cada tanda

Por vector refrescado, salvo donde dice otra cosa.

| | tanda 1, con 200000 | tanda 2, con 400000 | tanda 3, con 800000 | las 3 |
|---|---|---|---|---|
| vectores refrescados | 100000 | 200000 | 400000 | 700000 |
| cargas de posting | 19.2 | 20.9 | 22.6 | 21.6 |
| rondas | 4.6 | 5.0 | 5.4 | 5.2 |
| distancias por código: scan | 4871 | 5294 | 5820 | 5534 |
| distancias por código: endpoints | 8135 | 9817 | 11897 | 10765 |
| distancias por código, todas | 13006 | 15111 | 17717 | 16299 |
| exactas: rescore de la búsqueda | 64.0 | 64.0 | 64.0 | 64.0 |
| exactas: comprobar las aristas inversas | 34.5 | 40.5 | 45.8 | 42.7 |
| vecinos que ya tenían al vector | 4.18 | 4.55 | 4.83 | 4.66 |
| listas de vecinos leídas y comprobadas | 4.05 | 4.47 | 4.79 | 4.59 |
| aristas inversas escritas | 0.26 | 0.27 | 0.27 | 0.27 |
| refrescos que no escriben ninguna | 81.6 % | 80.7 % | 80.6 % | 80.8 % |
| postings distintos con una lista de vecino escrita: media (p50 / p95) | 0.20 (0 / 1) | 0.21 (0 / 1) | 0.21 (0 / 1) | 0.21 (0 / 1) |
| listas propias que cambian | 90.6 % | 92.9 % | 93.3 % | 92.8 % |
| aristas propias: antes | 7.63 | 8.28 | 8.95 | 8.57 |
| aristas propias: después | 8.50 | 9.24 | 9.79 | 9.44 |
| aristas propias: conservadas | 5.73 | 6.12 | 6.61 | 6.34 |
| vectores que su búsqueda no encontró | 0 | 0 | 0 | 0 |
| segundos, indicativos | 104 | 247 | 923 | 1274 |

### El coste total, frente a v2.3

En millones, salvo donde dice otra cosa. v2.3 refresca los 990000 vectores una vez, al final. v2.4 hace 700000 refrescos en 3 tandas, durante el crecimiento.

| | v2.3 | v2.4 | v2.4 frente a v2.3 |
|---|---|---|---|
| refrescos, en vectores | 990000 | 700000 | -29.3 % |
| inserts: cargas de posting de sus búsquedas | 19.2 | 19.2 | +0.1 % |
| inserts: distancias por código | 14004 | 14163 | +1.1 % |
| inserts: distancias exactas | 289.1 | 288.6 | -0.2 % |
| inserts: aristas inversas escritas | 4.04 | 4.04 | +0.0 % |
| refrescos: cargas de posting | 23.0 | 15.1 | -34.1 % |
| refrescos: distancias por código | 18318 | 11409 | -37.7 % |
| refrescos: distancias exactas | 110.9 | 74.7 | -32.7 % |
| refrescos: listas propias escritas | 0.844 | 0.650 | -23.0 % |
| refrescos: aristas inversas escritas | 0.213 | 0.188 | -11.6 % |
| todo: cargas de posting | 42.1 | 34.3 | -18.5 % |
| todo: distancias por código | 32322 | 25573 | -20.9 % |
| todo: distancias exactas | 400.0 | 363.3 | -9.2 % |

| por refresco | v2.3 | v2.4 |
|---|---|---|
| cargas de posting | 23.2 | 21.6 |
| distancias por código | 18503 | 16299 |
| aristas inversas escritas | 0.21 | 0.27 |
| listas propias que cambian | 85.2 % | 92.8 % |

| por insert | v2.3 | v2.4 |
|---|---|---|
| cargas de posting | 21.52 | 21.55 |
| distancias por código | 15717 | 15896 |
| distancias exactas | 324.5 | 323.9 |
| aristas inversas escritas | 4.53 | 4.53 |

- **La reconstrucción en lote de la referencia interna, en millones:** 63187 distancias por código y 4058 exactas de la asignación.
  - 171587 vectores, el 17.33 %, no están en el posting de su centroide más cercano.

### Informativo: por qué la regla del lote da más

- **El instrumento:** con las aristas del 100 % repuestas, un refresco de los 990000 vectores, como el de v2.3. Son 23.3 cargas de posting y 18550 distancias por código por vector, 0.10 aristas inversas escritas, y cambia el 77.6 % de las listas.
- **No es parte del protocolo ni del criterio de v2.4.**

**Recall a igual ef, en la misma corrida:**

| ef de búsqueda | al 100 % | todo refrescado | sin las inversas | reconstruidas en lote | lote menos refrescado | sin las inversas menos refrescado | parte que cierra quitar las inversas |
|---|---|---|---|---|---|---|---|
| 10 | 0.9231 | 0.9239 | 0.9237 | 0.9247 | +0.0009 | -0.0002 | -23 % |
| 20 | 0.9506 | 0.9526 | 0.9523 | 0.9538 | +0.0012 | -0.0003 | -22 % |
| 40 | 0.9672 | 0.9689 | 0.9687 | 0.9693 | +0.0004 | -0.0002 | -60 % |
| 80 | 0.9783 | 0.9802 | 0.9800 | 0.9804 | +0.0002 | -0.0002 | -73 % |
| 160 | 0.9857 | 0.9869 | 0.9868 | 0.9877 | +0.0008 | -0.0001 | -12 % |
| 320 | 0.9908 | 0.9916 | 0.9915 | 0.9924 | +0.0008 | -0.0001 | -10 % |
| 640 | 0.9941 | 0.9945 | 0.9945 | 0.9953 | +0.0008 | -0.0000 | -5 % |
| 1024 | 0.9962 | 0.9964 | 0.9963 | 0.9969 | +0.0005 | -0.0001 | -11 % |

| | al 100 % | todo refrescado | sin las inversas | reconstruidas en lote |
|---|---|---|---|---|
| cargas a 0.95 | 10.67 | 10.43 | 10.46 | 10.36 |
| cargas a 0.99 | 56.88 | 52.03 | 52.47 | 47.89 |
| aristas por vector | 9.47 | 9.77 | 9.77 | 9.77 |
| recall de arista: insertados / lote del 10 % | 0.8141 / 0.8626 | 0.9806 / 0.9815 | 0.9798 / 0.9800 | 0.9688 / 0.9685 |

**Comprobación previa.** La regla del lote da +0.0008 y +0.0008 más de recall que el índice refrescado a ef 160 y 320.
La diferencia de v2.3 se repite en esta corrida (el umbral era 0.0005 en alguno de los dos).

**La muestra:** 10000 vectores, con 99004 vecinos exactos fuera de su posting, que son las aristas que pide la regla.

| | el refresco | la regla del lote |
|---|---|---|
| aristas que pide la regla | 99004 | 99004 |
| las que no pone | 2172 (2.2 %) | 3229 (3.3 %) |
| recall de arista | 0.9781 | 0.9674 |
| las que no pone ninguna de las dos | 940 | 940 |

**Hipótesis 1: lo que deja fuera cada regla, por causa.**

| el vecino que falta está | el refresco | parte de sus fallos | la regla del lote | parte de sus fallos |
|---|---|---|---|---|
| (a) en un posting al que la regla no tenía cómo llegar: ninguna arista seguida llevaba a él, o fuera de los sondeados | 365 | 16.8 % | 2891 | 89.5 % |
| (b) en un posting al que llevaba una arista seguida, sin leer | 1476 | 68.0 % | — | — |
| (c) en un posting leído, fuera de los candidatos por la estimación del código | 331 | 15.2 % | 338 | 10.5 % |

- **Lectura:** el 84.8 % de los fallos del refresco están en postings que su búsqueda no leyó, y el 10.5 % de los del lote, en postings que sí leyó. Hipótesis 1, **a medias**.
  - De los que el refresco no leyó, a 365 no llevaba ninguna arista seguida y a 1476 sí: el 19.8 % es del grafo y el 80.2 % del ef.

**Hipótesis 2: las escrituras inversas del refresco.**

- **Lectura:** quitar el efecto de las inversas cierra el -12 % y el -10 % de lo que separa al índice refrescado de la regla del lote, a ef 160 y 320. Hipótesis 2, **refutada**.
- **En la muestra,** las escrituras inversas de los demás sacaron 94 aristas de las listas que dejó cada refresco, y 17 eran vecinos exactos (18.1 %). Metieron 94, y 94 lo eran (100.0 %).

**Hipótesis 3: los fallos, por las aristas entrantes del posting del vecino.**

*Aristas entrantes del posting, en el índice del 100 %:*

| quinta parte | aristas entrantes | vecinos que pide la regla | fallos del refresco | tasa | cuota de sus fallos | fallos del lote | tasa | cuota de sus fallos |
|---|---|---|---|---|---|---|---|---|
| 1 | de 6 a 1831 | 19800 | 594 | 3.0 % | 27.3 % | 722 | 3.6 % | 22.4 % |
| 2 | de 1831 a 2339 | 19801 | 432 | 2.2 % | 19.9 % | 652 | 3.3 % | 20.2 % |
| 3 | de 2339 a 2836 | 19801 | 377 | 1.9 % | 17.4 % | 596 | 3.0 % | 18.5 % |
| 4 | de 2836 a 3433 | 19801 | 390 | 2.0 % | 18.0 % | 640 | 3.2 % | 19.8 % |
| 5 | de 3433 a 6442 | 19801 | 379 | 1.9 % | 17.4 % | 619 | 3.1 % | 19.2 % |

*Aristas entrantes por miembro, en el índice del 100 %:*

| quinta parte | aristas entrantes | vecinos que pide la regla | fallos del refresco | tasa | cuota de sus fallos | fallos del lote | tasa | cuota de sus fallos |
|---|---|---|---|---|---|---|---|---|
| 1 | de 0.06 a 8.03 | 19800 | 636 | 3.2 % | 29.3 % | 738 | 3.7 % | 22.9 % |
| 2 | de 8.03 a 9.43 | 19801 | 379 | 1.9 % | 17.4 % | 594 | 3.0 % | 18.4 % |
| 3 | de 9.43 a 10.74 | 19801 | 410 | 2.1 % | 18.9 % | 648 | 3.3 % | 20.1 % |
| 4 | de 10.74 a 12.60 | 19801 | 393 | 2.0 % | 18.1 % | 651 | 3.3 % | 20.2 % |
| 5 | de 12.60 a 24.88 | 19801 | 354 | 1.8 % | 16.3 % | 598 | 3.0 % | 18.5 % |

*Aristas entrantes del posting, en el índice refrescado:*

| quinta parte | aristas entrantes | vecinos que pide la regla | fallos del refresco | tasa | cuota de sus fallos | fallos del lote | tasa | cuota de sus fallos |
|---|---|---|---|---|---|---|---|---|
| 1 | de 6 a 1921 | 19800 | 587 | 3.0 % | 27.0 % | 699 | 3.5 % | 21.6 % |
| 2 | de 1921 a 2411 | 19801 | 408 | 2.1 % | 18.8 % | 657 | 3.3 % | 20.3 % |
| 3 | de 2411 a 2878 | 19801 | 399 | 2.0 % | 18.4 % | 613 | 3.1 % | 19.0 % |
| 4 | de 2878 a 3471 | 19801 | 395 | 2.0 % | 18.2 % | 635 | 3.2 % | 19.7 % |
| 5 | de 3471 a 6895 | 19801 | 383 | 1.9 % | 17.6 % | 625 | 3.2 % | 19.4 % |

*Aristas entrantes del posting, en el reconstruido en lote:*

| quinta parte | aristas entrantes | vecinos que pide la regla | fallos del refresco | tasa | cuota de sus fallos | fallos del lote | tasa | cuota de sus fallos |
|---|---|---|---|---|---|---|---|---|
| 1 | de 145 a 1922 | 19800 | 589 | 3.0 % | 27.1 % | 725 | 3.7 % | 22.5 % |
| 2 | de 1922 a 2404 | 19801 | 408 | 2.1 % | 18.8 % | 664 | 3.4 % | 20.6 % |
| 3 | de 2404 a 2879 | 19801 | 392 | 2.0 % | 18.0 % | 613 | 3.1 % | 19.0 % |
| 4 | de 2879 a 3471 | 19801 | 397 | 2.0 % | 18.3 % | 625 | 3.2 % | 19.4 % |
| 5 | de 3471 a 6883 | 19801 | 386 | 1.9 % | 17.8 % | 602 | 3.0 % | 18.6 % |

- **Lectura,** sobre las aristas entrantes en el índice del 100 %: la quinta parte con menos tiene el 27.3 % de los fallos del refresco y el 22.4 % de los del lote. Hipótesis 3, **a medias**.
  - Los umbrales: confirmada con el 30 % o más del refresco y el 25 % o menos del lote; refutada con menos del 25 % del refresco, o con la cuota del lote a 3 puntos o menos de la del refresco.
  - Con las aristas entrantes por miembro, en el índice del 100 %: el 29.3 % y el 22.9 %.
  - Con las aristas entrantes del posting, en el índice refrescado: el 27.0 % y el 21.6 %.
  - Con las aristas entrantes del posting, en el reconstruido en lote: el 27.1 % y el 22.5 %.

**Las aristas entrantes de un posting, en cada estado:**

| | mínimo | p1 | p5 | p10 | p25 | p50 | p75 | p90 | p99 | máximo | media |
|---|---|---|---|---|---|---|---|---|---|---|---|
| al 100 % | 6 | 368 | 851 | 1121 | 1600 | 2212 | 2878 | 3540 | 4915 | 6442 | 2287 |
| todo refrescado | 6 | 450 | 949 | 1243 | 1703 | 2274 | 2929 | 3573 | 4935 | 6895 | 2360 |
| reconstruidas en lote | 71 | 532 | 947 | 1243 | 1703 | 2273 | 2932 | 3573 | 4949 | 6883 | 2361 |
| al 100 %, por miembro | 0.06 | 2.20 | 4.60 | 5.82 | 7.64 | 9.33 | 11.21 | 13.24 | 17.15 | 24.88 | 9.43 |
| todo refrescado, por miembro | 0.06 | 2.64 | 5.06 | 6.34 | 7.94 | 9.60 | 11.57 | 13.68 | 18.16 | 28.59 | 9.82 |
| reconstruidas en lote, por miembro | 0.57 | 2.90 | 5.05 | 6.32 | 7.94 | 9.61 | 11.54 | 13.68 | 18.14 | 28.69 | 9.83 |

### La regla de ruido

- **Ninguna comparación a 0.99 entre corridas distintas.** Todos los estados de arriba son de esta corrida, y el lote es la única referencia de fuera, porque el criterio está escrito contra él.
- **De v2.3 solo se leen conteos.** Ni su índice refrescado ni su reconstrucción en lote se comparan a 0.99 con los de aquí.
