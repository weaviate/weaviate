### Estado

| estado | cargas a 0.95 | frente al lote | cargas a 0.99 | frente al lote | cruza 0.99 entre | recall en esos ef | rondas a 0.99 | endpoints a 0.99 | aristas por vector | recall de arista: insertados / lote del 10 % | cobertura | listas con una arista repetida |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| lote del 100 % | 10.06 | +0.0 % | 52.78 | +0.0 % | ef 160 y 320 | 0.9858 y 0.9908 | 12.94 | 26634 | — | — | — | — |
| antes del refresco | 10.97 | +9.0 % | 64.75 | +22.7 % | ef 320 y 640 | 0.9896 y 0.9933 | 15.94 | 32877 | 8.85 | 0.7098 / 0.4804 | complete | 0 |
| etapa 1: refrescados los que entraron hasta el 10 % | 10.71 | +6.4 % | 62.88 | +19.1 % | ef 320 y 640 | 0.9898 y 0.9936 | 15.47 | 32305 | 9.11 | 0.7124 / 0.9746 | complete | 0 |
| etapa 2: refrescados los que entraron hasta el 40 % | 10.47 | +4.0 % | 59.06 | +11.9 % | ef 160 y 320 | 0.9850 y 0.9904 | 14.51 | 30832 | 9.55 | 0.8542 / 0.9750 | complete | 0 |
| etapa 3: refrescados los que entraron hasta el 100 % | 10.23 | +1.7 % | 56.09 | +6.3 % | ef 160 y 320 | 0.9859 y 0.9909 | 13.77 | 29565 | 9.77 | 0.9771 / 0.9773 | complete | 0 |
| informativa: aristas reconstruidas en lote | 10.17 | +1.0 % | 49.66 | -5.9 % | ef 160 y 320 | 0.9871 y 0.9922 | 12.16 | 26344 | 9.77 | 0.9702 / 0.9696 | complete | 0 |

### El criterio

- **El lote carga 52.78 postings a 0.99.** +10 % son 58.06 cargas.
- **Antes del refresco, esta corrida carga 64.75 (+22.7 % frente al lote).**
- **Con todo refrescado carga 56.09 (+6.3 % frente al lote):** **el refresco sirve**, por el criterio.
  - Cruza 0.99 entre ef 160 y ef 320, con recall 0.9859 y 0.9909. El lote, entre ef 160 y ef 320.
- **Informativa, con las aristas reconstruidas en lote después, en la misma corrida:** 49.66 (-5.9 % frente al lote).

### La recuperación, por etapa

La parte de una etapa es lo que baja la medida en ella sobre lo que baja de antes del refresco a después de la última etapa. La distancia al lote cerrada es (antes − después) / (antes − lote), acumulada.

**Cargas a 0.99** (lote: 52.78)

| | antes | etapa 1 | etapa 2 | etapa 3 | reconstruidas en lote |
|---|---|---|---|---|---|
| cargas a 0.99 | 64.75 | 62.88 | 59.06 | 56.09 | 49.66 |
| lo que baja en la etapa | | 1.88 | 3.82 | 2.97 | |
| parte de la recuperación | | 22 % | 44 % | 34 % | |
| por 100 000 vectores refrescados | | 1.894 | 1.287 | 0.499 | |
| distancia al lote cerrada, acumulada | | 16 % | 48 % | 72 % | 126 % |

**Cargas a 0.95** (lote: 10.06)

| | antes | etapa 1 | etapa 2 | etapa 3 | reconstruidas en lote |
|---|---|---|---|---|---|
| cargas a 0.95 | 10.97 | 10.71 | 10.47 | 10.23 | 10.17 |
| lo que baja en la etapa | | 0.26 | 0.24 | 0.23 | |
| parte de la recuperación | | 36 % | 33 % | 32 % | |
| por 100 000 vectores refrescados | | 0.264 | 0.081 | 0.039 | |
| distancia al lote cerrada, acumulada | | 29 % | 55 % | 81 % | 89 % |

**Rondas a 0.99** (lote: 12.94)

| | antes | etapa 1 | etapa 2 | etapa 3 | reconstruidas en lote |
|---|---|---|---|---|---|
| rondas a 0.99 | 15.94 | 15.47 | 14.51 | 13.77 | 12.16 |
| lo que baja en la etapa | | 0.47 | 0.96 | 0.74 | |
| parte de la recuperación | | 22 % | 44 % | 34 % | |
| por 100 000 vectores refrescados | | 0.472 | 0.322 | 0.125 | |
| distancia al lote cerrada, acumulada | | 16 % | 48 % | 72 % | 126 % |

**Endpoints a 0.99** (lote: 26634)

| | antes | etapa 1 | etapa 2 | etapa 3 | reconstruidas en lote |
|---|---|---|---|---|---|
| endpoints a 0.99 | 32877 | 32305 | 30832 | 29565 | 26344 |
| lo que baja en la etapa | | 571 | 1473 | 1267 | |
| parte de la recuperación | | 17 % | 44 % | 38 % | |
| por 100 000 vectores refrescados | | 577.1 | 496.1 | 213.3 | |
| distancia al lote cerrada, acumulada | | 9 % | 33 % | 53 % | 105 % |

**Endpoints a 0.95** (lote: 4953)

| | antes | etapa 1 | etapa 2 | etapa 3 | reconstruidas en lote |
|---|---|---|---|---|---|
| endpoints a 0.95 | 5378 | 5299 | 5241 | 5096 | 5042 |
| lo que baja en la etapa | | 79 | 58 | 146 | |
| parte de la recuperación | | 28 % | 21 % | 52 % | |
| por 100 000 vectores refrescados | | 79.8 | 19.5 | 24.5 | |
| distancia al lote cerrada, acumulada | | 19 % | 32 % | 66 % | 79 % |

### Recall a igual ef de búsqueda, diferencia frente al lote

| ef de búsqueda | lote | antes | etapa 1 | etapa 2 | etapa 3 | reconstruidas en lote |
|---|---|---|---|---|---|---|
| 10 | 0.9229 | -0.0032 | -0.0022 | +0.0009 | +0.0023 | +0.0040 |
| 20 | 0.9506 | -0.0018 | -0.0007 | +0.0010 | +0.0037 | +0.0047 |
| 40 | 0.9664 | -0.0013 | -0.0008 | +0.0002 | +0.0020 | +0.0037 |
| 80 | 0.9782 | -0.0014 | -0.0012 | -0.0004 | +0.0011 | +0.0019 |
| 160 | 0.9858 | -0.0018 | -0.0014 | -0.0009 | +0.0001 | +0.0013 |
| 320 | 0.9908 | -0.0013 | -0.0010 | -0.0004 | +0.0001 | +0.0014 |
| 640 | 0.9940 | -0.0007 | -0.0005 | +0.0000 | +0.0004 | +0.0011 |
| 1024 | 0.9960 | -0.0003 | -0.0004 | -0.0001 | +0.0001 | +0.0010 |
| peor | | -0.0032 | -0.0022 | -0.0009 | +0.0001 | +0.0010 |

### Recall a igual ef: la parte de cada etapa

| ef de búsqueda | antes | con todo refrescado | lo que sube | etapa 1 | etapa 2 | etapa 3 |
|---|---|---|---|---|---|---|
| 10 | 0.9198 | 0.9252 | +0.0054 | 18 % | 56 % | 26 % |
| 20 | 0.9488 | 0.9544 | +0.0055 | 19 % | 31 % | 49 % |
| 40 | 0.9651 | 0.9684 | +0.0034 | 17 % | 29 % | 55 % |
| 80 | 0.9768 | 0.9793 | +0.0025 | 6 % | 34 % | 60 % |
| 160 | 0.9840 | 0.9859 | +0.0019 | 20 % | 30 % | 50 % |
| 320 | 0.9896 | 0.9909 | +0.0013 | 16 % | 46 % | 37 % |
| 640 | 0.9933 | 0.9944 | +0.0011 | 24 % | 45 % | 31 % |
| 1024 | 0.9956 | 0.9961 | +0.0005 | -8 % | 58 % | 50 % |

### Cargas de posting a igual ef de búsqueda, frente al lote

| ef de búsqueda | lote | antes | etapa 1 | etapa 2 | etapa 3 | reconstruidas en lote |
|---|---|---|---|---|---|---|
| 10 | 7.58 | 7.94 (+4.8 %) | 7.90 (+4.2 %) | 7.92 (+4.5 %) | 7.92 (+4.5 %) | 7.97 (+5.2 %) |
| 20 | 10.13 | 10.68 (+5.4 %) | 10.68 (+5.4 %) | 10.65 (+5.1 %) | 10.70 (+5.7 %) | 10.75 (+6.1 %) |
| 40 | 14.56 | 15.44 (+6.1 %) | 15.43 (+6.0 %) | 15.38 (+5.7 %) | 15.45 (+6.1 %) | 15.47 (+6.3 %) |
| 80 | 22.12 | 23.62 (+6.8 %) | 23.59 (+6.6 %) | 23.67 (+7.0 %) | 23.72 (+7.2 %) | 23.78 (+7.5 %) |
| 160 | 35.18 | 37.45 (+6.4 %) | 37.50 (+6.6 %) | 37.62 (+6.9 %) | 37.66 (+7.0 %) | 37.67 (+7.1 %) |
| 320 | 57.27 | 61.16 (+6.8 %) | 61.21 (+6.9 %) | 61.32 (+7.1 %) | 61.34 (+7.1 %) | 61.42 (+7.3 %) |
| 640 | 95.54 | 101.37 (+6.1 %) | 101.61 (+6.4 %) | 101.73 (+6.5 %) | 101.94 (+6.7 %) | 102.02 (+6.8 %) |
| 1024 | 136.38 | 144.90 (+6.2 %) | 145.10 (+6.4 %) | 145.20 (+6.5 %) | 145.42 (+6.6 %) | 145.55 (+6.7 %) |

### Grado por cohorte

| estado | 0 → 10 % | 10 → 20 % | 20 → 40 % | 40 → 60 % | 60 → 80 % | 80 → 100 % | todos |
|---|---|---|---|---|---|---|---|
| antes del refresco | 7.63 | 8.07 | 8.66 | 9.04 | 9.27 | 9.43 | 8.85 |
| etapa 1: refrescados los que entraron hasta el 10 % | 10.26 | 8.07 | 8.66 | 9.04 | 9.27 | 9.43 | 9.11 |
| etapa 2: refrescados los que entraron hasta el 40 % | 10.26 | 10.11 | 9.83 | 9.04 | 9.27 | 9.43 | 9.55 |
| etapa 3: refrescados los que entraron hasta el 100 % | 10.26 | 10.11 | 9.83 | 9.68 | 9.59 | 9.55 | 9.77 |
| informativa: aristas reconstruidas en lote | 10.27 | 10.11 | 9.83 | 9.68 | 9.59 | 9.56 | 9.77 |

### Recall de arista contra el corpus actual

De la muestra de cada grupo, a nivel de vector y, entre paréntesis, de posting, con su grado.

| estado | insertados: arista (posting) | grado | vectores del lote del 10 %: arista (posting) | grado |
|---|---|---|---|---|
| antes del refresco | 0.7098 (0.7827) | 8.91 | 0.4804 (0.5949) | 7.54 |
| etapa 1: refrescados los que entraron hasta el 10 % | 0.7124 (0.7862) | 8.91 | 0.9746 (0.9630) | 10.23 |
| etapa 2: refrescados los que entraron hasta el 40 % | 0.8542 (0.8890) | 9.40 | 0.9750 (0.9638) | 10.23 |
| etapa 3: refrescados los que entraron hasta el 100 % | 0.9771 (0.9685) | 9.68 | 0.9773 (0.9674) | 10.23 |
| informativa: aristas reconstruidas en lote | 0.9702 (0.9560) | 9.69 | 0.9696 (0.9520) | 10.24 |

### El coste del refresco, por etapa

Por vector refrescado, salvo donde dice otra cosa.

| | etapa 1 | etapa 2 | etapa 3 | las tres |
|---|---|---|---|---|
| vectores refrescados | 99000 | 297000 | 594000 | 990000 |
| cargas de posting | 23.1 | 23.2 | 23.2 | 23.2 |
| rondas | 5.5 | 5.6 | 5.6 | 5.6 |
| distancias por código: scan | 5915 | 5929 | 5932 | 5930 |
| distancias por código: endpoints | 12357 | 12543 | 12625 | 12574 |
| distancias por código, todas | 18272 | 18473 | 18557 | 18503 |
| exactas: rescore de la búsqueda | 64.0 | 64.0 | 64.0 | 64.0 |
| exactas: comprobar las aristas inversas | 46.5 | 47.7 | 48.4 | 48.0 |
| listas de vecinos leídas para las inversas | 5.16 | 4.95 | 4.72 | 4.83 |
| vecinos que ya tenían al vector | 4.87 | 4.81 | 4.77 | 4.79 |
| aristas inversas escritas | 0.54 | 0.34 | 0.10 | 0.21 |
| listas propias que cambian | 96.9 % | 95.7 % | 78.0 % | 85.2 % |
| aristas propias: antes | 7.63 | 8.46 | 9.25 | 8.85 |
| aristas propias: después | 10.26 | 9.92 | 9.61 | 9.77 |
| aristas propias: conservadas | 5.14 | 5.82 | 7.80 | 6.94 |
| postings distintos con una lista de vecino escrita: media (p50 / p95) | 0.43 (0 / 2) | 0.28 (0 / 1) | 0.09 (0 / 1) | 0.18 (0 / 1) |
| vectores que su búsqueda no encontró | 0 | 0 | 0 | 0 |
| segundos, indicativos | 137 | 438 | 940 | 1516 |

### Un refresco junto a un insert de esta corrida y a la reconstrucción en lote

| por vector | un refresco, las tres etapas | un insert, toda la corrida | un insert, del 80 al 100 % | la regla del lote |
|---|---|---|---|---|
| cargas de posting | 23.2 | 21.5 | 23.1 | — |
| distancias por código | 18503 | 15717 | 17841 | 63900 |
| exactas: grafo de centroides o asignación | 0.0 | 129.7 | 138.7 | 4099.0 |
| exactas: rescore | 64.0 | 64.0 | 64.0 | 64.0 |
| exactas: comprobar las aristas inversas | 48.0 | 83.7 | 90.7 | — |
| aristas inversas escritas | 0.21 | 4.53 | 4.74 | — |

- **El refresco entero, en millones:** 23 cargas de posting, 18318 distancias por código y 110.9 exactas.
- **Los 891000 inserts de la corrida, en millones:** 19 cargas de posting y 14004 distancias por código en sus búsquedas.
- **La reconstrucción en lote, en millones:** 63261 distancias por código y 4058 exactas de la asignación. El refresco hace 0.29 veces sus distancias por código.

### La regla de ruido

- **Ninguna comparación a 0.99 entre corridas distintas.** Todos los estados de arriba son de esta corrida, y el lote es la única referencia de fuera, porque el criterio está escrito contra él.
- **El «antes» de esta corrida no se compara a 0.99** con las celdas de v2.2 ni con la corrida del diagnóstico.
