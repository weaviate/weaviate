### Deriva entre las dos rondas

| brazo | threads | mediana de la 2ª ronda contra la 1ª, por punto | mínimo | máximo | |
|---|---|---|---|---|---|
| HNSW | 1 | +6.3 % | -1.1 % | +21.2 % | **deriva** |
| HNSW | 8 | +0.2 % | -0.7 % | +5.9 % | sin deriva |
| posting-edge | 1 | +0.5 % | -3.6 % | +2.6 % | sin deriva |
| posting-edge | 8 | -0.8 % | -1.1 % | -0.2 % | sin deriva |

### El gate: HNSW / posting-edge a igual recall

Banda: de 1.7765x a 2.0055x (1.87x–1.91x ±5 %, escrita de 1.78x a 2.01x). HNSW con rescoreLimit 512, posting-edge con 200.

| recall | threads | HNSW, 1ª / 2ª ronda | posting-edge, 1ª / 2ª | ratio, 1ª ronda | ratio, 2ª ronda | estimación rehecha (tarde / mañana) | v1.4, sin kernel (tarde / mañana) | gate |
|---|---|---|---|---|---|---|---|---|
| 0.95 | 1 | 2085 / 2197 | 1038 / 1056 | 2.010x | 2.081x | 1.95x / 1.94x | 2.14x / 2.10x | fuera, por encima / fuera, por encima |
| 0.95 | 8 | 14219 / 14228 | 7031 / 6982 | 2.022x | 2.038x | 2.00x / 1.93x | 2.20x / 2.09x | fuera, por encima / fuera, por encima |
| 0.99 | 1 | 344 / 353 | 333 / 336 | 1.033x | 1.052x | 1.48x / 1.18x | 1.72x / 1.36x | — |
| 0.99 | 8 | 2325 / 2341 | 2286 / 2262 | 1.017x | 1.035x | 1.57x / 1.16x | 1.82x / 1.34x | — |

### Contraste 1: el scan en caché y el ahorro por endpoint

Pase instrumentado de posting-edge, 1 thread, a igual recall. El build es el del ancla de v1.5.

| recall | corrida | ef | escaneados | scan, µs | ns por escaneado | endpoints | endpoints, µs | ns por endpoint | centroides, µs | total, µs |
|---|---|---|---|---|---|---|---|---|---|---|
| 0.95 | v1.5, sin kernel | 19 | 3303 | 167 | 50.5 | 6871 | 496 | 72.2 | 225 | 1168 |
| 0.95 | esta noche | 19 | 3314 | 116 | 34.9 | 6908 | 379 | 54.9 | 220 | 988 |
| 0.99 | v1.5, sin kernel | 146 | 13077 | 571 | 43.6 | 33017 | 2389 | 72.4 | 241 | 3972 |
| 0.99 | esta noche | 146 | 13085 | 371 | 28.4 | 33055 | 1829 | 55.3 | 237 | 3184 |

- **A 0.95:** ahorro de 15.6 ns por vector escaneado (microbenchmark: 15.2) y 17.3 ns por endpoint (microbenchmark: 16.4).
- **A 0.99:** ahorro de 15.3 ns por vector escaneado (microbenchmark: 15.2) y 17.0 ns por endpoint (microbenchmark: 16.4).

### Contraste 2: la ganancia de cada brazo y la aceleración a 8 threads

Posting-edge contra el ancla de v1.5 (el mismo build, la noche anterior) y contra el de v1.4 (el «hoy» de la estimación). HNSW contra los dos builds de v1.4. Primera ronda; entre paréntesis, la segunda.

| recall | threads | PE / ancla v1.5 | PE / v1.4 | estimado | HNSW / tarde | HNSW / mañana | estimado (tarde / mañana) |
|---|---|---|---|---|---|---|---|
| 0.95 | 1 | 1.159x (1.180x) | 1.193x (1.214x) | 1.16x | 1.122x (1.182x) | 1.141x (1.202x) | 1.06x / 1.07x |
| 0.95 | 8 | 1.127x (1.119x) | 1.178x (1.170x) | 1.16x | 1.084x (1.084x) | 1.141x (1.141x) | 1.06x / 1.07x |
| 0.99 | 1 | 1.260x (1.269x) | 1.277x (1.286x) | 1.22x | 0.769x (0.788x) | 0.970x (0.995x) | 1.05x / 1.06x |
| 0.99 | 8 | 1.200x (1.187x) | 1.291x (1.278x) | 1.22x | 0.721x (0.726x) | 0.984x (0.991x) | 1.05x / 1.06x |

Aceleración de 1 a 8 threads, QPS con 8 / QPS con 1 a igual recall, primera ronda (segunda):

| recall | HNSW esta noche | HNSW tarde | HNSW mañana | PE esta noche | PE ancla v1.5 | PE v1.4 |
|---|---|---|---|---|---|---|
| 0.95 | 6.82x (6.48x) | 7.06x | 6.82x | 6.78x (6.61x) | 6.97x | 6.86x |
| 0.99 | 6.75x (6.63x) | 7.20x | 6.66x | 6.86x (6.74x) | 7.20x | 6.78x |

### Contrastes 3 y 4: las distancias de HNSW y su build

ef al que cada build llega a cada recall: el primero de la malla y el interpolado (lineal en recall). Distancias por query a ese ef, de las cuentas de TestSearchWork a 10M (un tercer build). QPS sin el kernel, deducido: 1 / (1/QPS + distancias × 16.4 ns), 1 thread.

| recall | build | ef (malla) | ef interpolado | distancias | QPS, 1 thread | QPS sin el kernel |
|---|---|---|---|---|---|---|
| 0.95 | esta noche, 1ª ronda | 48 | 45.7 | 1716 | 2085 | 1970 |
| 0.95 | esta noche, 2ª ronda | 48 | 45.7 | 1716 | 2197 | 2069 |
| 0.95 | v1.4 tarde | 48 | 46.8 | 1747 | 1859 | 1859 (medido) |
| 0.95 | v1.4 mañana | 64 | 50.2 | 1838 | 1828 | 1828 (medido) |
| 0.99 | esta noche, 1ª ronda | 512 | 393.7 | 10186 | 344 | 326 |
| 0.99 | esta noche, 2ª ronda | 512 | 393.7 | 10186 | 353 | 334 |
| 0.99 | v1.4 tarde | 256 | 256.0 | 7000 | 448 | 448 (medido) |
| 0.99 | v1.4 mañana | 384 | 365.6 | 9542 | 355 | 355 (medido) |

A igual ef, 1 thread: recall y QPS de cada build, primera ronda.

| ef | recall esta noche / tarde / mañana | QPS esta noche (2ª) / tarde / mañana | esta noche / tarde, 1ª (2ª) |
|---|---|---|---|
| 10 | 0.6603 / 0.6558 / 0.6425 | 5230 (5566) / 4197 / 4343 | 1.246x (1.326x) |
| 16 | 0.8030 / 0.8059 / 0.7913 | 4121 (4381) / 3395 / 3936 | 1.214x (1.290x) |
| 24 | 0.8880 / 0.8923 / 0.8835 | 3371 (3417) / 3042 / 2750 | 1.108x (1.123x) |
| 32 | 0.9241 / 0.9246 / 0.9192 | 2754 (2725) / 2581 / 2537 | 1.067x (1.056x) |
| 48 | 0.9536 / 0.9517 / 0.9477 | 2006 (2132) / 1819 / 1897 | 1.103x (1.172x) |
| 64 | 0.9662 / 0.9637 / 0.9628 | 1410 (1708) / 1502 / 1489 | 0.939x (1.138x) |
| 96 | 0.9744 / 0.9743 / 0.9735 | 1182 (1241) / 953 / 980 | 1.240x (1.302x) |
| 128 | 0.9783 / 0.9781 / 0.9798 | 936 (943) / 759 / 833 | 1.233x (1.243x) |
| 192 | 0.9839 / 0.9842 / 0.9842 | 599 (673) / 580 / 604 | 1.032x (1.160x) |
| 256 | 0.9872 / 0.9900 / 0.9871 | 471 (520) / 448 / 476 | 1.050x (1.161x) |
| 384 | 0.9898 / 0.9925 / 0.9904 | 352 (362) / 310 / 341 | 1.135x (1.167x) |
| 512 | 0.9921 / 0.9934 / 0.9922 | 272 (272) / 259 / 252 | 1.049x (1.051x) |
| 768 | 0.9939 / 0.9949 / 0.9935 | 188 (202) / 187 / 188 | 1.008x (1.079x) |
| 1024 | 0.9947 / 0.9955 / 0.9947 | 147 (159) / 146 / 139 | 1.007x (1.094x) |

### El ef de centroides, sin cronometrar

| ef de centroides | recall a ef=20 | a ef=160 | pérdida frente a 64 | cargas (20 / 160) | endpoints (20 / 160) | entrada = centroide más cercano | centroides, µs (20 / 160) | total, µs (20 / 160) |
|---|---|---|---|---|---|---|---|---|
| 64 | 0.9543 | 0.9909 | 0.0000 | 13.1 / 55.2 | 7250 / 35604 | 0.995 | 224 / 240 | 1028 / 3431 |
| 48 | 0.9543 | 0.9909 | 0.0000 | 13.1 / 55.2 | 7261 / 35616 | 0.990 | 181 / 196 | 976 / 3373 |
| 32 | 0.9543 | 0.9909 | 0.0000 | 13.1 / 55.2 | 7302 / 35651 | 0.983 | 140 / 154 | 934 / 3345 |
| 16 | 0.9544 | 0.9909 | 0.0000 | 13.1 / 55.2 | 7336 / 35672 | 0.962 | 94 / 106 | 877 / 3281 |
| 8 | 0.9548 | 0.9910 | -0.0001 | 13.2 / 55.2 | 7445 / 35745 | 0.915 | 70 / 79 | 857 / 3231 |

### El ef de centroides 8, cronometrado, contra la curva principal (ef 64) del mismo proceso

| recall | threads | ef 64, 1ª / 2ª | ef 8, 1ª / 2ª | ganancia, 1ª ronda | 2ª ronda | HNSW / PE con ef 8, 1ª ronda (informativo) |
|---|---|---|---|---|---|---|
| 0.90 | 1 | 1263 / 1296 | 1589 / 1596 | +25.8 % | +23.2 % | 1.98x |
| 0.90 | 8 | 8612 / 8522 | 10870 / 10876 | +26.2 % | +27.6 % | 1.92x |
| 0.95 | 1 | 1038 / 1056 | 1244 / 1216 | +19.9 % | +15.2 % | 1.68x |
| 0.95 | 8 | 7031 / 6982 | 8533 / 8544 | +21.4 % | +22.4 % | 1.67x |
| 0.99 | 1 | 333 / 336 | 358 / 357 | +7.4 % | +6.3 % | 0.96x |
| 0.99 | 8 | 2286 / 2262 | 2448 / 2441 | +7.1 % | +7.9 % | 0.95x |

| threads | deriva del ef 8, 2ª ronda contra la 1ª: mediana | mínimo | máximo |
|---|---|---|---|
| 1 | -0.2 % | -2.6 % | +0.4 % |
| 8 | +0.1 % | -0.4 % | +0.4 % |

Trabajo a igual recall, 1 thread, del pase instrumentado:

| recall | ef de centroides | ef | cargas | rondas | endpoints | escaneados | centroides, µs | total, µs |
|---|---|---|---|---|---|---|---|---|
| 0.95 | 64 | 18.6 | 12.7 | 2.9 | 6908 | 3314 | 220 | 988 |
| 0.95 | 8 | 18.4 | 12.7 | 2.9 | 7041 | 3334 | 70 | 835 |
| 0.99 | 64 | 145.6 | 51.3 | 12.6 | 33055 | 13085 | 237 | 3184 |
| 0.99 | 8 | 143.8 | 50.9 | 12.5 | 32896 | 12981 | 79 | 3014 |
