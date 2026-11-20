### El índice de las curvas: lo que su crecimiento escribió, sin cronometrar

| índice | postings | cobertura | 0.95: ef | 0.99: ef | el store escribió | lo que ocupa al final | entre lo que ocupa | filas escritas | lápidas de filas |
|---|---|---|---|---|---|---|---|---|---|
| v5.0, un valor por cambio | 7 788 | completa | 21 | 158 | 9.43 GB | 567.7 MB | 16.6 veces | — | — |
| K = 64 de v5.1, filas en su bucket | 7 716 | completa | 21 | 166 | 7.74 GB | 553.9 MB | 14.0 veces | 84 537, 0.90 GB | — |
| **v5.3, filas junto a los miembros** | 7 762 | completa | 21 | 151 | 17.08 GB | 540.1 MB | 31.6 veces | 84 336, 0.90 GB | 76 994, 0.81 GB |

- **Los postings** cuentan los retirados: son los ids dados.
- **El crecimiento de v5.3 no se cronometra:** corre de día, con otras cosas en la máquina.

### El índice reorganizado: corrección y lo que lee el store, sin cronometrar

| índice | frente al índice tal cual, 5 000 queries por punto | segmentos del bucket de postings | bucket de postings | fichero de vectores |
|---|---|---|---|---|
| v5.3, tal cual | — | 1 | 255.28 MB | 6082.56 MB |
| v5.3, reorganizado | los mismos ids, distancias y trabajo | 1 | 255.28 MB | 6082.56 MB |

- **Escribir «v5.3, reorganizado»** llevó 43 s.

| índice | recall | ef | cargas | lecturas por carga | bytes por carga | por el modelo | real entre modelo |
|---|---|---|---|---|---|---|---|
| v5.3, tal cual | 0.95 | 21 | 19.50 | 2.00 | 67.03 KB | 63.66 KB | 1.053 |
| v5.3, tal cual | 0.99 | 151 | 69.28 | 2.00 | 66.73 KB | 63.37 KB | 1.053 |
| v5.3, reorganizado | 0.95 | 21 | 19.50 | 2.00 | 67.03 KB | 63.66 KB | 1.053 |
| v5.3, reorganizado | 0.99 | 151 | 69.28 | 2.00 | 66.73 KB | 63.37 KB | 1.053 |

### Lo que lee una carga en frío, fila a fila, sin cronometrar

Las 500 primeras queries, de una en una, con las filas de cada carga leídas una detrás de otra. Las cuatro filas de v5.0 y de v5.1 son las del control combinado, contadas el 20 por la mañana: no se vuelven a medir.

**Lo que cuenta el store,** en modo pread. Es lo mismo en frío que en caliente.

| índice | recall | cargas por query | fila de miembros: lecturas | bytes | fila de aristas: lecturas | bytes | las dos: lecturas | bytes |
|---|---|---|---|---|---|---|---|---|
| K = 64 de v5.1, tal cual | 0.95 | 18.77 | 1.92 | 55.72 KB | 1.00 | 11.58 KB | 2.92 | 67.30 KB |
| K = 64 de v5.1, tal cual | 0.99 | 73.49 | 1.92 | 55.52 KB | 1.00 | 11.54 KB | 2.92 | 67.06 KB |
| crecido de v5.0, tal cual | 0.95 | 18.58 | 1.95 | 55.60 KB | 1.96 | 16.71 KB | 3.91 | 72.31 KB |
| crecido de v5.0, tal cual | 0.99 | 71.31 | 1.95 | 55.26 KB | 1.96 | 16.61 KB | 3.91 | 71.87 KB |
| **v5.3, tal cual** | 0.95 | 18.73 | 1.00 | 55.63 KB | 1.00 | 11.55 KB | 2.00 | 67.18 KB |
| **v5.3, tal cual** | 0.99 | 68.91 | 1.00 | 55.40 KB | 1.00 | 11.52 KB | 2.00 | 66.91 KB |
| K = 64 de v5.1, reorganizado | 0.95 | 18.77 | 1.00 | 55.71 KB | 1.00 | 11.58 KB | 2.00 | 67.29 KB |
| K = 64 de v5.1, reorganizado | 0.99 | 73.49 | 1.00 | 55.51 KB | 1.00 | 11.54 KB | 2.00 | 67.04 KB |
| la celda 2 de v5.0 | 0.95 | 18.58 | 1.00 | 55.59 KB | 1.00 | 13.57 KB | 2.00 | 69.16 KB |
| la celda 2 de v5.0 | 0.99 | 71.31 | 1.00 | 55.25 KB | 1.00 | 13.52 KB | 2.00 | 68.77 KB |
| **v5.3, reorganizado** | 0.95 | 18.73 | 1.00 | 55.63 KB | 1.00 | 11.55 KB | 2.00 | 67.18 KB |
| **v5.3, reorganizado** | 0.99 | 68.91 | 1.00 | 55.40 KB | 1.00 | 11.52 KB | 2.00 | 66.91 KB |

| índice | segmentos: miembros | filas de aristas | bucket de miembros | bucket de filas de aristas |
|---|---|---|---|---|
| K = 64 de v5.1, tal cual | 2 | 1 | 228.76 MB | 40.26 MB |
| crecido de v5.0, tal cual | 2 | — (van en el de miembros) | 282.78 MB | — |
| **v5.3, tal cual** | 1 | — (van en el de miembros) | 255.28 MB | — |
| K = 64 de v5.1, reorganizado | 1 | 1 | 215.07 MB | 40.26 MB |
| la celda 2 de v5.0 | 1 | — (van en el de miembros) | 263.22 MB | — |
| **v5.3, reorganizado** | 1 | — (van en el de miembros) | 255.28 MB | — |

**Lo que el sistema lee del disco para el proceso,** con el store leyendo por su mapping, tras tirar la caché antes de cada tanda de 10 queries. Un page-in es un fallo de página que fue al disco; los bytes llevan lo que el sistema lee por delante.

| índice | recall | se lee primero | fila de miembros: page-ins por carga | bytes | fila de aristas: page-ins | bytes | las dos: page-ins | bytes | el resto de la query: bytes |
|---|---|---|---|---|---|---|---|---|---|
| K = 64 de v5.1, tal cual | 0.95 | los miembros | 4.70 | 118.8 KB | 1.44 | 48.3 KB | 6.15 | 167.1 KB | 1313.1 KB |
| K = 64 de v5.1, tal cual | 0.95 | las aristas | 4.70 | 118.8 KB | 1.44 | 48.3 KB | 6.15 | 167.1 KB | 1392.3 KB |
| K = 64 de v5.1, tal cual | 0.99 | los miembros | 4.05 | 90.8 KB | 1.20 | 34.9 KB | 5.25 | 125.7 KB | 1464.1 KB |
| K = 64 de v5.1, tal cual | 0.99 | las aristas | 4.05 | 90.8 KB | 1.20 | 34.9 KB | 5.25 | 125.7 KB | 1462.3 KB |
| crecido de v5.0, tal cual | 0.95 | los miembros | 4.86 | 128.4 KB | 0.88 | 15.1 KB | 5.74 | 143.5 KB | 1426.6 KB |
| crecido de v5.0, tal cual | 0.95 | las aristas | 3.18 | 52.2 KB | 2.56 | 107.7 KB | 5.74 | 159.9 KB | 1323.4 KB |
| crecido de v5.0, tal cual | 0.99 | los miembros | 4.30 | 98.7 KB | 0.74 | 12.8 KB | 5.05 | 111.5 KB | 1313.4 KB |
| crecido de v5.0, tal cual | 0.99 | las aristas | 2.89 | 45.2 KB | 2.15 | 80.5 KB | 5.05 | 125.7 KB | 1311.4 KB |
| **v5.3, tal cual** | 0.95 | los miembros | 4.13 | 93.3 KB | 0.63 | 12.6 KB | 4.75 | 105.9 KB | 1324.7 KB |
| **v5.3, tal cual** | 0.95 | las aristas | 3.19 | 53.7 KB | 1.56 | 70.8 KB | 4.75 | 124.5 KB | 1313.1 KB |
| **v5.3, tal cual** | 0.99 | los miembros | 3.77 | 81.5 KB | 0.55 | 10.6 KB | 4.32 | 92.1 KB | 1311.3 KB |
| **v5.3, tal cual** | 0.99 | las aristas | 2.91 | 46.4 KB | 1.41 | 61.8 KB | 4.32 | 108.2 KB | 1310.9 KB |
| K = 64 de v5.1, reorganizado | 0.95 | los miembros | 4.07 | 93.1 KB | 1.44 | 48.3 KB | 5.51 | 141.3 KB | 1203.5 KB |
| K = 64 de v5.1, reorganizado | 0.95 | las aristas | 4.07 | 93.1 KB | 1.44 | 48.3 KB | 5.51 | 141.3 KB | 1203.5 KB |
| K = 64 de v5.1, reorganizado | 0.99 | los miembros | 3.66 | 80.5 KB | 1.20 | 34.9 KB | 4.86 | 115.4 KB | 1217.4 KB |
| K = 64 de v5.1, reorganizado | 0.99 | las aristas | 3.66 | 80.5 KB | 1.20 | 34.9 KB | 4.86 | 115.4 KB | 1217.5 KB |
| la celda 2 de v5.0 | 0.95 | los miembros | 4.13 | 94.0 KB | 0.73 | 14.2 KB | 4.86 | 108.2 KB | 1205.4 KB |
| la celda 2 de v5.0 | 0.95 | las aristas | 3.19 | 53.9 KB | 1.68 | 72.3 KB | 4.86 | 126.2 KB | 1205.0 KB |
| la celda 2 de v5.0 | 0.99 | los miembros | 3.78 | 81.4 KB | 0.63 | 12.2 KB | 4.40 | 93.6 KB | 1223.0 KB |
| la celda 2 de v5.0 | 0.99 | las aristas | 2.91 | 46.2 KB | 1.50 | 62.5 KB | 4.40 | 108.7 KB | 1223.1 KB |
| **v5.3, reorganizado** | 0.95 | los miembros | 4.13 | 93.3 KB | 0.63 | 12.6 KB | 4.75 | 105.9 KB | 1202.0 KB |
| **v5.3, reorganizado** | 0.95 | las aristas | 3.19 | 53.7 KB | 1.56 | 70.8 KB | 4.75 | 124.5 KB | 1201.9 KB |
| **v5.3, reorganizado** | 0.99 | los miembros | 3.77 | 81.5 KB | 0.55 | 10.6 KB | 4.32 | 92.1 KB | 1223.7 KB |
| **v5.3, reorganizado** | 0.99 | las aristas | 2.91 | 46.4 KB | 1.41 | 61.8 KB | 4.32 | 108.2 KB | 1223.5 KB |

- **El resto de la query** es su rescore, que lee los vectores con lecturas posicionales y no con page-ins.
- **Con las filas leídas a la vez,** como en las curvas, las dos lecturas de una carga se solapan: lo que se cuenta aquí es cuánto disco toca cada fila, no cuánto espera la ronda.

### La lectura

Es el formato candidato si, reorganizado, a 0.95 y con 1 thread, queda a 1.20 veces o menos del lote en frío y a 1.25 o menos en caliente. Por el p50 de la primera ronda, sobre el del lote de la misma sesión. Sin reorganizar se dan las mismas dos razones, sin umbral.

| ronda | | v5.3, reorganizado, ms | el lote | entre el lote | entra | v5.3, tal cual | entre el lote | la celda 2 de v5.0 | entre el lote | K = 64 de v5.1, reorganizado | entre el lote |
|---|---|---|---|---|---|---|---|---|---|---|---|
| r1 | en frío | 4.06 | 3.69 | 1.10x | **sí** | 4.46 | 1.21x | 4.00 | 1.08x | 4.37 | 1.18x |
| r1 | en caliente | 1.12 | 0.92 | 1.21x | **sí** | 1.12 | 1.21x | 1.25 | 1.35x | 1.11 | 1.20x |
| r2 | en frío | 4.02 | 3.64 | 1.10x | **sí** | 4.32 | 1.19x | 3.97 | 1.09x | 4.25 | 1.17x |
| r2 | en caliente | 1.11 | 0.92 | 1.20x | **sí** | 1.12 | 1.22x | 1.25 | 1.35x | 1.11 | 1.20x |

- **Reorganizado, frente a la celda 2 de v5.0, en frío:** 1.02 veces, +1.6 %: un empate.
- **Reorganizado, frente a la celda 2 de v5.0, en caliente:** 0.89 veces, −10.6 %.
- **Reorganizado, frente a K = 64 de v5.1, reorganizado, en frío:** 0.93 veces, −7.0 %: un empate.
- **Reorganizado, frente a K = 64 de v5.1, reorganizado, en caliente:** 1.01 veces, +1.2 %: un empate.

**Es el formato candidato: cumple las dos mitades en la primera ronda.**

### Latencia, primera ronda

**Recall 0.95, en frío:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| v5.3, tal cual | ef 21 | 4.46 | 10.64 | 16.01 | 192 | 15.76 | 29.68 | 39.50 | 405 |
| v5.3, reorganizado | ef 21 | 4.06 | 10.18 | 15.63 | 206 | 11.99 | 24.73 | 34.37 | 500 |
| la celda 2 de v5.0 | ef 21 | 4.00 | 9.61 | 15.33 | 209 | 12.06 | 25.14 | 34.03 | 490 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 21 | 3.69 | 8.39 | 13.64 | 229 | 11.77 | 24.56 | 33.03 | 520 |
| K = 64 de v5.1, reorganizado (mía) | ef 21 | 4.37 | 11.63 | 18.39 | 188 | 14.24 | 28.41 | 39.39 | 429 |

**Recall 0.95, en caliente:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| v5.3, tal cual | ef 21 | 1.12 | 3.87 | 5.96 | 661 | 1.52 | 5.09 | 8.02 | 3 891 |
| v5.3, reorganizado | ef 21 | 1.12 | 3.87 | 5.87 | 661 | 1.53 | 5.11 | 7.73 | 3 916 |
| la celda 2 de v5.0 | ef 21 | 1.25 | 4.24 | 6.53 | 597 | 1.90 | 6.39 | 10.02 | 3 119 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 21 | 0.92 | 3.01 | 4.47 | 819 | 1.29 | 4.13 | 6.25 | 4 696 |
| K = 64 de v5.1, reorganizado (mía) | ef 21 | 1.11 | 3.84 | 5.73 | 662 | 1.51 | 5.11 | 7.60 | 3 954 |

**Recall 0.99, en frío:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| v5.3, tal cual | ef 151 | 9.71 | 29.46 | 41.18 | 81 | 26.34 | 63.82 | 76.55 | 211 |
| v5.3, reorganizado | ef 151 | 9.42 | 29.20 | 41.57 | 83 | 24.27 | 58.75 | 70.68 | 230 |
| la celda 2 de v5.0 | ef 158 | 9.74 | 30.62 | 45.19 | 79 | 25.64 | 62.75 | 77.38 | 217 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 187 | 9.01 | 27.91 | 39.99 | 86 | 22.56 | 56.39 | 69.83 | 240 |
| K = 64 de v5.1, reorganizado (mía) | ef 166 | 10.81 | 34.77 | 48.41 | 71 | 28.50 | 69.25 | 85.50 | 197 |

**Recall 0.99, en caliente:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| v5.3, tal cual | ef 151 | 3.58 | 12.36 | 18.12 | 209 | 4.72 | 16.47 | 24.33 | 1 260 |
| v5.3, reorganizado | ef 151 | 3.58 | 12.29 | 17.80 | 209 | 4.85 | 16.81 | 24.45 | 1 234 |
| la celda 2 de v5.0 | ef 158 | 4.31 | 14.61 | 21.18 | 177 | 6.47 | 21.62 | 31.04 | 950 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 187 | 3.24 | 10.97 | 15.94 | 233 | 4.28 | 14.47 | 21.16 | 1 414 |
| K = 64 de v5.1, reorganizado (mía) | ef 166 | 3.81 | 12.85 | 18.90 | 198 | 4.97 | 16.91 | 24.68 | 1 218 |

**Cada índice entre la celda en lote,** por el p50:

| índice | estado | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |
|---|---|---|---|---|---|
| v5.3, tal cual | en frío | 1.21x | 1.34x | 1.08x | 1.17x |
| v5.3, tal cual | en caliente | 1.21x | 1.18x | 1.11x | 1.10x |
| v5.3, reorganizado | en frío | 1.10x | 1.02x | 1.05x | 1.08x |
| v5.3, reorganizado | en caliente | 1.21x | 1.18x | 1.10x | 1.13x |
| la celda 2 de v5.0 | en frío | 1.08x | 1.02x | 1.08x | 1.14x |
| la celda 2 de v5.0 | en caliente | 1.35x | 1.48x | 1.33x | 1.51x |
| K = 64 de v5.1, reorganizado (mía) | en frío | 1.18x | 1.21x | 1.20x | 1.26x |
| K = 64 de v5.1, reorganizado (mía) | en caliente | 1.20x | 1.17x | 1.18x | 1.16x |

**De la latencia media a 0.95 con 1 thread, en frío,** y lo que cuesta traer cada fila, por carga:

| índice | media, ms | traer los bloques | scan y ruteo | rescore | cargas | fila de miembros, µs | fila de aristas, µs |
|---|---|---|---|---|---|---|---|
| v5.3, tal cual | 5.22 | 2.28 | 0.80 | 1.98 | 18.73 | 284.0 | 240.0 |
| v5.3, reorganizado | 4.85 | 2.30 | 0.80 | 1.60 | 18.73 | 286.5 | 242.3 |
| la celda 2 de v5.0 | 4.77 | 2.27 | 0.81 | 1.56 | 18.58 | 285.0 | 273.8 |
| escrito en lote (v4.0, 512 bits en RAM) | 4.37 | 1.96 | 0.66 | 1.62 | 17.83 | 262.3 | 218.3 |
| K = 64 de v5.1, reorganizado (mía) | 5.32 | 2.81 | 0.79 | 1.57 | 18.77 | 358.9 | 199.6 |

**De la latencia media a 0.95 con 1 thread, en caliente,** y lo que cuesta traer cada fila, por carga:

| índice | media, ms | traer los bloques | scan y ruteo | rescore | cargas | fila de miembros, µs | fila de aristas, µs |
|---|---|---|---|---|---|---|---|
| v5.3, tal cual | 1.51 | 0.43 | 0.83 | 0.14 | 19.50 | 15.6 | 10.6 |
| v5.3, reorganizado | 1.51 | 0.43 | 0.83 | 0.14 | 19.50 | 15.6 | 10.6 |
| la celda 2 de v5.0 | 1.68 | 0.57 | 0.85 | 0.14 | 19.54 | 16.3 | 34.7 |
| escrito en lote (v4.0, 512 bits en RAM) | 1.22 | 0.30 | 0.68 | 0.13 | 18.59 | 14.2 | 7.1 |
| K = 64 de v5.1, reorganizado (mía) | 1.51 | 0.42 | 0.84 | 0.14 | 19.56 | 15.9 | 9.6 |

### Las curvas: dos rondas, deriva y actividad

| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |
|---|---|---|---|---|---|
| v5.3, tal cual | 19:15 a 19:21 | 19:45 a 19:51 | no | +3.8 % (0.99, 8 threads, en caliente: 4.72 y 4.90 ms) | — |
| v5.3, reorganizado | 19:21 a 19:27 | 19:51 a 19:57 | no | +1.8 % (0.95, 8 threads, en frío: 11.99 y 12.20 ms) | — |
| la celda 2 de v5.0 | 19:27 a 19:34 | 19:57 a 20:03 | no | +2.9 % (0.95, 8 threads, en frío: 12.06 y 12.40 ms) | — |
| escrito en lote (v4.0, 512 bits en RAM) | 19:34 a 19:39 | 20:03 a 20:08 | no | −1.5 % (0.95, 1 thread, en frío: 3.69 y 3.64 ms) | — |
| K = 64 de v5.1, reorganizado (mía) | 19:39 a 19:45 | 20:08 a 20:14 | no | +2.7 % (0.99, 8 threads, en frío: 28.50 y 29.28 ms) | — |

- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.
- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.

### RAM residente al buscar

| índice | heap en uso tras abrir | huella del proceso |
|---|---|---|
| v5.3, tal cual | 175.51 MB | 372.11 MB |
| v5.3, reorganizado | 179.31 MB | 380.19 MB |
| la celda 2 de v5.0 | 180.03 MB | 380.76 MB |
| escrito en lote (v4.0, 512 bits en RAM) | 146.10 MB | 288.74 MB |
| K = 64 de v5.1, reorganizado (mía) | 179.28 MB | 379.04 MB |

