### La celda nueva: corrección y lo que lee el store, sin cronometrar

| índice | frente a K = 64 tal cual, 5 000 queries por punto | segmentos: miembros | filas de aristas | bucket de miembros | bucket de filas de aristas | fichero de vectores |
|---|---|---|---|---|---|---|
| K = 64, tal cual | — | 2 | 1 | 228.76 MB | 40.26 MB | 6082.56 MB |
| K = 64, reorganizado | los mismos ids, distancias y trabajo | 1 | 1 | 215.07 MB | 40.26 MB | 6082.56 MB |

- **Escribir «K = 64, reorganizado»** llevó 104 s.

| índice | recall | ef | cargas | lecturas por carga | bytes por carga | por el modelo | real entre modelo |
|---|---|---|---|---|---|---|---|
| K = 64, tal cual | 0.95 | 21 | 19.56 | 2.92 | 67.34 KB | 63.95 KB | 1.053 |
| K = 64, tal cual | 0.99 | 166 | 73.93 | 2.92 | 67.01 KB | 63.64 KB | 1.053 |
| K = 64, reorganizado | 0.95 | 21 | 19.56 | 2.00 | 67.33 KB | 63.95 KB | 1.053 |
| K = 64, reorganizado | 0.99 | 166 | 73.93 | 2.00 | 67.00 KB | 63.64 KB | 1.053 |

### Lo que lee una carga en frío, fila a fila, sin cronometrar

Las 500 primeras queries, de una en una, con las filas de cada carga leídas una detrás de otra.

**Lo que cuenta el store,** en modo pread. Es lo mismo en frío que en caliente.

| índice | recall | cargas por query | fila de miembros: lecturas | bytes | fila de aristas: lecturas | bytes | las dos: lecturas | bytes |
|---|---|---|---|---|---|---|---|---|
| K = 64, tal cual | 0.95 | 18.77 | 1.92 | 55.72 KB | 1.00 | 11.58 KB | 2.92 | 67.30 KB |
| K = 64, tal cual | 0.99 | 73.49 | 1.92 | 55.52 KB | 1.00 | 11.54 KB | 2.92 | 67.06 KB |
| crecido de v5.0, tal cual | 0.95 | 18.58 | 1.95 | 55.60 KB | 1.96 | 16.71 KB | 3.91 | 72.31 KB |
| crecido de v5.0, tal cual | 0.99 | 71.31 | 1.95 | 55.26 KB | 1.96 | 16.61 KB | 3.91 | 71.87 KB |
| K = 64, reorganizado | 0.95 | 18.77 | 1.00 | 55.71 KB | 1.00 | 11.58 KB | 2.00 | 67.29 KB |
| K = 64, reorganizado | 0.99 | 73.49 | 1.00 | 55.51 KB | 1.00 | 11.54 KB | 2.00 | 67.04 KB |
| la celda 2 de v5.0 | 0.95 | 18.58 | 1.00 | 55.59 KB | 1.00 | 13.57 KB | 2.00 | 69.16 KB |
| la celda 2 de v5.0 | 0.99 | 71.31 | 1.00 | 55.25 KB | 1.00 | 13.52 KB | 2.00 | 68.77 KB |

| índice | segmentos: miembros | filas de aristas | bucket de miembros | bucket de filas de aristas |
|---|---|---|---|---|
| K = 64, tal cual | 2 | 1 | 228.76 MB | 40.26 MB |
| crecido de v5.0, tal cual | 2 | — (van en el de miembros) | 282.78 MB | — |
| K = 64, reorganizado | 1 | 1 | 215.07 MB | 40.26 MB |
| la celda 2 de v5.0 | 1 | — (van en el de miembros) | 263.22 MB | — |

**Lo que el sistema lee del disco para el proceso,** con el store leyendo por su mapping, tras tirar la caché antes de cada tanda de 10 queries. Un page-in es un fallo de página que fue al disco; los bytes llevan lo que el sistema lee por delante.

| índice | recall | se lee primero | fila de miembros: page-ins por carga | bytes | fila de aristas: page-ins | bytes | las dos: page-ins | bytes | el resto de la query: bytes |
|---|---|---|---|---|---|---|---|---|---|
| K = 64, tal cual | 0.95 | los miembros | 4.70 | 118.8 KB | 1.44 | 48.3 KB | 6.15 | 167.1 KB | 1313.1 KB |
| K = 64, tal cual | 0.95 | las aristas | 4.70 | 118.8 KB | 1.44 | 48.3 KB | 6.15 | 167.1 KB | 1392.3 KB |
| K = 64, tal cual | 0.99 | los miembros | 4.05 | 90.8 KB | 1.20 | 34.9 KB | 5.25 | 125.7 KB | 1464.1 KB |
| K = 64, tal cual | 0.99 | las aristas | 4.05 | 90.8 KB | 1.20 | 34.9 KB | 5.25 | 125.7 KB | 1462.3 KB |
| crecido de v5.0, tal cual | 0.95 | los miembros | 4.86 | 128.4 KB | 0.88 | 15.1 KB | 5.74 | 143.5 KB | 1426.6 KB |
| crecido de v5.0, tal cual | 0.95 | las aristas | 3.18 | 52.2 KB | 2.56 | 107.7 KB | 5.74 | 159.9 KB | 1323.4 KB |
| crecido de v5.0, tal cual | 0.99 | los miembros | 4.30 | 98.7 KB | 0.74 | 12.8 KB | 5.05 | 111.5 KB | 1313.4 KB |
| crecido de v5.0, tal cual | 0.99 | las aristas | 2.89 | 45.2 KB | 2.15 | 80.5 KB | 5.05 | 125.7 KB | 1311.4 KB |
| K = 64, reorganizado | 0.95 | los miembros | 4.07 | 93.1 KB | 1.44 | 48.3 KB | 5.51 | 141.3 KB | 1203.5 KB |
| K = 64, reorganizado | 0.95 | las aristas | 4.07 | 93.1 KB | 1.44 | 48.3 KB | 5.51 | 141.3 KB | 1203.5 KB |
| K = 64, reorganizado | 0.99 | los miembros | 3.66 | 80.5 KB | 1.20 | 34.9 KB | 4.86 | 115.4 KB | 1217.4 KB |
| K = 64, reorganizado | 0.99 | las aristas | 3.66 | 80.5 KB | 1.20 | 34.9 KB | 4.86 | 115.4 KB | 1217.5 KB |
| la celda 2 de v5.0 | 0.95 | los miembros | 4.13 | 94.0 KB | 0.73 | 14.2 KB | 4.86 | 108.2 KB | 1205.4 KB |
| la celda 2 de v5.0 | 0.95 | las aristas | 3.19 | 53.9 KB | 1.68 | 72.3 KB | 4.86 | 126.2 KB | 1205.0 KB |
| la celda 2 de v5.0 | 0.99 | los miembros | 3.78 | 81.4 KB | 0.63 | 12.2 KB | 4.40 | 93.6 KB | 1223.0 KB |
| la celda 2 de v5.0 | 0.99 | las aristas | 2.91 | 46.2 KB | 1.50 | 62.5 KB | 4.40 | 108.7 KB | 1223.1 KB |

- **El resto de la query** es su rescore, que lee los vectores con lecturas posicionales y no con page-ins.
- **Con las filas leídas a la vez,** como en las curvas, las dos lecturas de una carga se solapan: lo que se cuenta aquí es cuánto disco toca cada fila, no cuánto espera la ronda.

### La lectura

Es el formato candidato si, a 0.95 y con 1 thread, queda a 1.20 veces o menos del lote en frío y a 1.25 o menos en caliente. Por el p50 de la primera ronda, sobre el del lote de la misma sesión.

| ronda | | K = 64, reorganizado, ms | el lote | entre el lote | entra | la celda 2 de v5.0 | entre el lote | K = 64, tal cual | entre el lote |
|---|---|---|---|---|---|---|---|---|---|
| r1 | en frío | 4.39 | 3.65 | 1.20x | **no** | 4.00 | 1.10x | 4.96 | 1.36x |
| r1 | en caliente | 1.11 | 0.93 | 1.20x | **sí** | 1.25 | 1.34x | 1.13 | 1.22x |
| r2 | en frío | 4.45 | 3.61 | 1.23x | **no** | 4.03 | 1.12x | 5.01 | 1.39x |
| r2 | en caliente | 1.11 | 0.92 | 1.20x | **sí** | 1.25 | 1.36x | 1.13 | 1.23x |

- **Frente a la celda 2 de v5.0, en frío:** 1.10 veces, +9.8 %: un empate.
- **Frente a la celda 2 de v5.0, en caliente:** 0.89 veces, −10.8 %.

**No es el formato candidato, tal como se pre-registró: no cumple las dos mitades.**

### Latencia, primera ronda

**Recall 0.95, en frío:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| K = 64, reorganizado | ef 21 | 4.39 | 12.06 | 18.74 | 185 | 13.84 | 29.83 | 39.24 | 419 |
| la celda 2 de v5.0 | ef 21 | 4.00 | 9.66 | 15.91 | 207 | 12.32 | 25.59 | 35.20 | 491 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 21 | 3.65 | 8.20 | 13.18 | 232 | 12.08 | 22.72 | 30.89 | 528 |
| K = 64, tal cual (mía) | ef 21 | 4.96 | 13.54 | 20.66 | 162 | 18.30 | 37.72 | 49.21 | 339 |

**Recall 0.95, en caliente:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| K = 64, reorganizado | ef 21 | 1.11 | 3.85 | 5.76 | 661 | 1.53 | 5.09 | 7.79 | 3 906 |
| la celda 2 de v5.0 | ef 21 | 1.25 | 4.28 | 6.55 | 595 | 1.91 | 6.42 | 9.97 | 3 120 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 21 | 0.93 | 3.00 | 4.56 | 816 | 1.30 | 4.07 | 6.07 | 4 716 |
| K = 64, tal cual (mía) | ef 21 | 1.13 | 3.91 | 5.90 | 650 | 1.56 | 5.28 | 8.21 | 3 803 |

**Recall 0.99, en frío:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| K = 64, reorganizado | ef 166 | 11.10 | 35.09 | 52.57 | 70 | 28.34 | 69.12 | 82.11 | 198 |
| la celda 2 de v5.0 | ef 158 | 9.69 | 30.51 | 43.47 | 81 | 25.61 | 61.32 | 75.92 | 220 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 187 | 9.12 | 27.41 | 39.76 | 86 | 22.90 | 55.78 | 70.97 | 242 |
| K = 64, tal cual (mía) | ef 166 | 12.01 | 37.75 | 52.47 | 65 | 32.44 | 77.05 | 92.80 | 177 |

**Recall 0.99, en caliente:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| K = 64, reorganizado | ef 166 | 3.84 | 12.94 | 18.77 | 198 | 5.00 | 17.09 | 24.77 | 1 208 |
| la celda 2 de v5.0 | ef 158 | 4.30 | 14.39 | 21.03 | 177 | 6.61 | 21.98 | 31.75 | 932 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 187 | 3.23 | 10.91 | 15.83 | 233 | 4.32 | 14.54 | 20.94 | 1 405 |
| K = 64, tal cual (mía) | ef 166 | 3.88 | 13.10 | 19.24 | 195 | 5.20 | 17.77 | 25.42 | 1 162 |

**Cada índice entre la celda en lote,** por el p50:

| índice | estado | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |
|---|---|---|---|---|---|
| K = 64, reorganizado | en frío | 1.20x | 1.15x | 1.22x | 1.24x |
| K = 64, reorganizado | en caliente | 1.20x | 1.18x | 1.19x | 1.16x |
| la celda 2 de v5.0 | en frío | 1.10x | 1.02x | 1.06x | 1.12x |
| la celda 2 de v5.0 | en caliente | 1.34x | 1.47x | 1.33x | 1.53x |
| K = 64, tal cual (mía) | en frío | 1.36x | 1.51x | 1.32x | 1.42x |
| K = 64, tal cual (mía) | en caliente | 1.22x | 1.20x | 1.20x | 1.20x |

**De la latencia media a 0.95 con 1 thread, en frío,** y lo que cuesta traer cada fila, por carga:

| índice | media, ms | traer los bloques | scan y ruteo | rescore | cargas | fila de miembros, µs | fila de aristas, µs |
|---|---|---|---|---|---|---|---|
| K = 64, reorganizado | 5.41 | 2.86 | 0.82 | 1.59 | 18.77 | 365.2 | 202.8 |
| la celda 2 de v5.0 | 4.82 | 2.28 | 0.84 | 1.56 | 18.58 | 284.4 | 274.3 |
| escrito en lote (v4.0, 512 bits en RAM) | 4.30 | 1.93 | 0.66 | 1.58 | 17.83 | 259.5 | 216.2 |
| K = 64, tal cual (mía) | 6.17 | 3.38 | 0.82 | 1.82 | 18.77 | 439.2 | 198.9 |

**De la latencia media a 0.95 con 1 thread, en caliente,** y lo que cuesta traer cada fila, por carga:

| índice | media, ms | traer los bloques | scan y ruteo | rescore | cargas | fila de miembros, µs | fila de aristas, µs |
|---|---|---|---|---|---|---|---|
| K = 64, reorganizado | 1.51 | 0.42 | 0.84 | 0.14 | 19.56 | 15.8 | 9.3 |
| la celda 2 de v5.0 | 1.68 | 0.58 | 0.85 | 0.14 | 19.54 | 16.2 | 34.3 |
| escrito en lote (v4.0, 512 bits en RAM) | 1.22 | 0.31 | 0.67 | 0.14 | 18.59 | 14.2 | 7.0 |
| K = 64, tal cual (mía) | 1.54 | 0.45 | 0.84 | 0.14 | 19.56 | 18.0 | 9.5 |

### Las curvas: dos rondas, deriva y actividad

| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |
|---|---|---|---|---|---|
| K = 64, reorganizado | 12:30 a 12:36 | 12:55 a 13:01 | no | −2.2 % (0.99, 1 thread, en frío: 11.10 y 10.85 ms) | — |
| la celda 2 de v5.0 | 12:36 a 12:43 | 13:01 a 13:08 | no | +3.1 % (0.95, 8 threads, en frío: 12.32 y 12.71 ms) | — |
| escrito en lote (v4.0, 512 bits en RAM) | 12:43 a 12:48 | 13:08 a 13:13 | no | −3.4 % (0.99, 1 thread, en frío: 9.12 y 8.80 ms) | — |
| K = 64, tal cual (mía) | 12:48 a 12:55 | 13:13 a 13:20 | no | +1.1 % (0.95, 1 thread, en frío: 4.96 y 5.01 ms) | — |

- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.
- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.

### RAM residente al buscar

| índice | heap en uso tras abrir | huella del proceso |
|---|---|---|
| K = 64, reorganizado | 179.09 MB | 378.99 MB |
| la celda 2 de v5.0 | 179.38 MB | 379.94 MB |
| escrito en lote (v4.0, 512 bits en RAM) | 145.72 MB | 288.21 MB |
| K = 64, tal cual (mía) | 175.06 MB | 370.48 MB |

