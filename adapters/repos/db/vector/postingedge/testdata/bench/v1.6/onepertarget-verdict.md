### Aristas de cada celda

| celda | aristas por vector | postings de destino por vector | aristas quitadas | recall de arista contra todas | aristas, MB | heap del índice, MB | self found |
|---|---|---|---|---|---|---|---|
| sin filtro (ancla) | 11.41 | 7.35 | 0.0 % | 0.9470 | 473.4 | 1712.4 | 1.0000 |
| una arista por posting | 7.35 | 7.35 | 35.6 % | 0.6034 | 318.4 | 1557.5 | 1.0000 |
| sin filtro (ancla del final) | 11.41 | 7.35 | 0.0 % | 0.9470 | 473.4 | 1712.4 | 1.0000 |

### La comprobación sin cronometrar, antes de ninguna celda

| ef | recall | cargas | rondas | endpoints | escaneados |
|---|---|---|---|---|---|
| 10 | 0.9088 | 9.6 | 2.2 | 3408 | 2515 |
| 20 | 0.9478 | 13.0 | 3.0 | 5390 | 3395 |
| 40 | 0.9678 | 19.6 | 4.6 | 9029 | 5053 |
| 80 | 0.9814 | 31.8 | 7.7 | 15598 | 8146 |
| 160 | 0.9890 | 54.1 | 13.3 | 27073 | 13726 |
| 320 | 0.9918 | 93.8 | 23.2 | 46465 | 23646 |
| 640 | 0.9948 | 164.0 | 40.8 | 78757 | 41080 |
| 1024 | 0.9959 | 240.4 | 59.8 | 112295 | 60002 |

### QPS a igual recall (primera ronda; entre paréntesis, la segunda)

**1 thread**

| celda | QPS@0.95 | contra el ancla | QPS@0.99 | contra el ancla | ef para 0.99 | recall máximo (ef 1024) |
|---|---|---|---|---|---|---|
| sin filtro (ancla) | 1065 (1062) | +0.0 % (-0.2 %) | 339 (340) | +0.0 % (+0.1 %) | 160 | 0.9964 |
| una arista por posting | 1102 (1129) | +3.5 % (+6.1 %) | 313 (316) | -7.6 % (-7.0 %) | 320 | 0.9959 |
| sin filtro (ancla del final) | 1062 (1063) | -0.2 % (-0.1 %) | 339 (339) | -0.1 % (-0.1 %) | 160 | 0.9964 |

**8 threads**

| celda | QPS@0.95 | contra el ancla | QPS@0.99 | contra el ancla | ef para 0.99 | recall máximo (ef 1024) |
|---|---|---|---|---|---|---|
| sin filtro (ancla) | 7080 (7044) | +0.0 % (-0.5 %) | 2265 (2296) | +0.0 % (+1.4 %) | 160 | 0.9964 |
| una arista por posting | 7462 (7463) | +5.4 % (+5.4 %) | 2167 (2174) | -4.4 % (-4.0 %) | 320 | 0.9959 |
| sin filtro (ancla del final) | 7031 (7076) | -0.7 % (-0.1 %) | 2255 (2302) | -0.4 % (+1.6 %) | 160 | 0.9964 |

### Trabajo por query a igual recall (1 thread, primera ronda)

| celda | recall | ef | endpoints | escaneados | distancias | cargas | rondas | µs por query | endpoints, µs | scan, µs | centroides, µs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| sin filtro (ancla) | 0.95 | 19 | 6872 | 3303 | 10178 | 12.6 | 2.9 | 968 | 367 | 116 | 214 |
| una arista por posting | 0.95 | 22 | 5705 | 3546 | 9252 | 13.6 | 3.2 | 932 | 317 | 124 | 220 |
| sin filtro (ancla del final) | 0.95 | 19 | 6872 | 3303 | 10178 | 12.6 | 2.9 | 983 | 372 | 116 | 219 |
| sin filtro (ancla) | 0.99 | 146 | 33017 | 13077 | 46094 | 51.3 | 12.6 | 3156 | 1797 | 377 | 234 |
| una arista por posting | 0.99 | 205 | 32833 | 16669 | 49502 | 65.8 | 16.2 | 3410 | 1831 | 478 | 240 |
| sin filtro (ancla del final) | 0.99 | 146 | 33017 | 13077 | 46094 | 51.3 | 12.6 | 3168 | 1803 | 377 | 234 |

### A igual ef (1 thread, primera ronda)

| ef | recall sin filtro / con filtro | QPS sin filtro / con filtro | endpoints sin filtro / con filtro | cargas sin filtro / con filtro |
|---|---|---|---|---|
| 10 | 0.9138 / 0.9088 | 1314 / 1409 | 4534 / 3408 | 9.5 / 9.6 |
| 20 | 0.9543 / 0.9478 | 1038 / 1144 | 7220 / 5390 | 13.1 / 13.0 |
| 40 | 0.9721 / 0.9678 | 741 / 814 | 11963 / 9029 | 19.6 / 19.6 |
| 80 | 0.9843 / 0.9814 | 506 / 586 | 20612 / 15598 | 32.3 / 31.8 |
| 160 | 0.9909 / 0.9890 | 319 / 375 | 35567 / 27073 | 55.1 / 54.1 |
| 320 | 0.9928 / 0.9918 | 192 / 227 | 60746 / 46465 | 95.8 / 93.8 |
| 640 | 0.9952 / 0.9948 | 113 / 136 | 102674 / 78757 | 168.5 / 164.0 |
| 1024 | 0.9964 / 0.9959 | 78 / 94 | 145967 / 112295 | 246.9 / 240.4 |

### Deriva entre las dos anclas

| threads | mediana del ancla del final contra la del principio, por punto | mínimo | máximo |
|---|---|---|---|
| 1 | -0.2 % | -3.3 % | +1.4 % |
| 8 | -0.4 % | -1.8 % | +0.4 % |

Todos los puntos: -0.2 % de mediana, sin deriva según el umbral del 2 %.

### El ancla del paso frente al brazo de posting-edge de la noche (sin ser criterio)

| threads | QPS@0.95 ancla / noche | QPS@0.99 ancla / noche |
|---|---|---|
| 1 | 1065 / 1038 (+2.6 %) | 339 / 333 (+1.8 %) |
| 8 | 7080 / 7031 (+0.7 %) | 2265 / 2286 (-0.9 %) |

### Veredicto

- **1 thread:** +3.5 % a 0.95 y -7.6 % a 0.99 contra el ancla del principio; +3.8 % a 0.95 y -7.6 % a 0.99 contra la del final.
- **8 threads:** +5.4 % a 0.95 y -4.4 % a 0.99 contra el ancla del principio; +6.1 % a 0.95 y -3.9 % a 0.99 contra la del final.
- **Con 8 threads, que decide:** 0.99 alcanzable con rescoreLimit 200. No gana contra el ancla del principio.
