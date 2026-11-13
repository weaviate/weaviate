### Aristas de cada celda

| celda | aristas por vector | vectores recortados | aristas quitadas | recall de arista (contra las k más cercanas) | recall de arista (contra todas) | memoria de aristas, MB | heap del índice, MB | self found |
|---|---|---|---|---|---|---|---|---|
| sin cap (ancla) | 11.41 | 0.0 % | 0.0 % | 0.9470 | 0.9470 | 473.4 | 1712.4 | 0.9999 |
| cap 10 | 8.80 | 66.6 % | 22.8 % | 0.9552 | 0.7646 | 374.0 | 1613.0 | 1.0000 |
| cap 8 | 7.30 | 77.6 % | 36.0 % | 0.9572 | 0.6371 | 316.7 | 1555.7 | 0.9999 |
| sin cap (ancla del final) | 11.41 | 0.0 % | 0.0 % | 0.9470 | 0.9470 | 473.4 | 1712.4 | 0.9999 |

### QPS a igual recall (primera ronda; entre paréntesis, la segunda)

**1 thread**

| celda | QPS@0.90 | QPS@0.95 | contra el ancla | QPS@0.99 | contra el ancla | ef para 0.99 | recall máximo (ef 1024) |
|---|---|---|---|---|---|---|---|
| sin cap (ancla) | 1126 (1142) | 895 (906) | +0.0 % (+1.3 %) | 265 (269) | +0.0 % (+1.8 %) | 160 | 0.9964 |
| cap 10 | 1227 (1226) | 889 (888) | -0.7 % (-0.8 %) | 191 (192) | -27.7 % (-27.6 %) | 320 | 0.9950 |
| cap 8 | 1268 (1273) | 881 (882) | -1.6 % (-1.5 %) | 183 (183) | -30.8 % (-30.7 %) | 640 | 0.9943 |
| sin cap (ancla del final) | 1116 (1121) | 901 (891) | +0.7 % (-0.4 %) | 266 (260) | +0.5 % (-1.7 %) | 160 | 0.9964 |

**8 threads**

| celda | QPS@0.90 | QPS@0.95 | contra el ancla | QPS@0.99 | contra el ancla | ef para 0.99 | recall máximo (ef 1024) |
|---|---|---|---|---|---|---|---|
| sin cap (ancla) | 7763 (7728) | 6239 (6222) | +0.0 % (-0.3 %) | 1905 (1893) | +0.0 % (-0.6 %) | 160 | 0.9964 |
| cap 10 | 8292 (8324) | 6142 (6166) | -1.6 % (-1.2 %) | 1360 (1358) | -28.6 % (-28.7 %) | 320 | 0.9950 |
| cap 8 | 8663 (8684) | 6167 (6106) | -1.2 % (-2.1 %) | 1292 (1286) | -32.2 % (-32.5 %) | 640 | 0.9943 |
| sin cap (ancla del final) | 7572 (7617) | 6201 (6117) | -0.6 % (-2.0 %) | 1870 (1885) | -1.8 % (-1.0 %) | 160 | 0.9964 |

### Trabajo por query a igual recall (1 thread, primera ronda)

| celda | recall | ef | endpoints | escaneados | distancias | cargas | rondas | µs por query | endpoints, µs | scan, µs | centroides, µs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| sin cap (ancla) | 0.95 | 19 | 6871 | 3303 | 10176 | 12.6 | 2.9 | 1168 | 496 | 167 | 225 |
| cap 10 | 0.95 | 24 | 6671 | 3813 | 10486 | 14.6 | 3.4 | 1171 | 480 | 187 | 218 |
| cap 8 | 0.95 | 28 | 6412 | 4151 | 10568 | 16.0 | 3.7 | 1163 | 459 | 201 | 214 |
| sin cap (ancla) | 0.99 | 146 | 33017 | 13077 | 46095 | 51.3 | 12.6 | 3972 | 2389 | 571 | 241 |
| cap 10 | 0.99 | 290 | 45153 | 22023 | 67176 | 86.9 | 21.5 | 5569 | 3279 | 941 | 238 |
| cap 8 | 0.99 | 359 | 45629 | 25518 | 71148 | 100.9 | 25.0 | 5883 | 3339 | 1092 | 241 |

### A igual ef (1 thread, primera ronda)

| ef | recall sin cap / cap 10 / cap 8 | QPS sin cap / cap 10 / cap 8 | endpoints sin cap / cap 10 / cap 8 |
|---|---|---|---|
| 10 | 0.9138 / 0.9061 / 0.9007 | 1126 / 1227 / 1268 | 4532 / 3723 / 3243 |
| 20 | 0.9543 / 0.9442 / 0.9390 | 871 / 963 / 1012 | 7219 / 5881 / 5048 |
| 40 | 0.9721 / 0.9671 / 0.9618 | 605 / 703 / 758 | 11961 / 9673 / 8289 |
| 80 | 0.9843 / 0.9765 / 0.9725 | 400 / 470 / 518 | 20611 / 16480 / 14040 |
| 160 | 0.9909 / 0.9857 / 0.9841 | 248 / 297 / 329 | 35567 / 28336 / 24307 |
| 320 | 0.9928 / 0.9907 / 0.9894 | 150 / 178 / 200 | 60748 / 48711 / 41737 |
| 640 | 0.9952 / 0.9937 / 0.9930 | 88 / 105 / 117 | 102674 / 82686 / 71255 |
| 1024 | 0.9964 / 0.9950 / 0.9943 | 60 / 72 / 81 | 145968 / 118039 / 101962 |

### Deriva entre las dos anclas

| threads | mediana del ancla del final contra la del principio, por punto | mínimo | máximo |
|---|---|---|---|
| 1 | -0.1 % | -0.9 % | +0.9 % |
| 8 | -1.7 % | -3.0 % | +0.3 % |

Todos los puntos: -0.6 % de mediana, sin deriva según el umbral del 2 %.

### Veredicto

- **cap 10:** QPS@0.95 -0.7 % con 1 thread y -1.6 % con 8 contra el ancla del principio (-1.3 % y -1.0 % contra la del final); 0.99 alcanzable con rescoreLimit 200. No gana.
- **cap 8:** QPS@0.95 -1.6 % con 1 thread y -1.2 % con 8 contra el ancla del principio (-2.3 % y -0.6 % contra la del final); 0.99 alcanzable con rescoreLimit 200. No gana.
