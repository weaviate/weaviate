### La lectura

Sustituye a v5.3 como formato si cumple las cuatro: reorganizado, a 0.95 y con 1 thread, queda a 1.20 veces o menos del lote en frío y a 1.25 o menos en caliente, por el p50 de la primera ronda sobre el del lote de la misma sesión; el store escribe 10 GB o menos al crecer; y sin compactar una carga lee 1.5 veces o menos lo que usa.

| condición | medido | hacía falta | |
|---|---|---|---|
| en frío, v5.4 reorganizado entre el lote | 4.11 ms entre 3.71: 1.11x | 1.20 o menos | **sí** |
| en caliente, v5.4 reorganizado entre el lote | 1.14 ms entre 0.92: 1.23x | 1.25 o menos | **sí** |
| lo que el store escribe al crecer | 11.78 GB | 10 GB o menos | **no** |
| sin compactar, lo que lee una carga entre lo que usa | 1.573 | 1.5 o menos | **no** |

**No sustituye a v5.3, tal como se pre-registró: no cumple las cuatro condiciones.**

| ronda | | v5.4, reorganizado, ms | el lote | entre el lote | v5.3, reorganizado | entre el lote | el índice de v5.3 sobre un Map | entre el lote |
|---|---|---|---|---|---|---|---|---|
| r1 | en frío | 4.11 | 3.71 | 1.11x | 4.10 | 1.11x | 4.05 | 1.09x |
| r1 | en caliente | 1.14 | 0.92 | 1.23x | 1.12 | 1.21x | 1.17 | 1.26x |
| r2 | en frío | 4.12 | 3.62 | 1.14x | 4.01 | 1.11x | 4.07 | 1.13x |
| r2 | en caliente | 1.17 | 0.90 | 1.29x | 1.11 | 1.23x | 1.16 | 1.28x |

- **v5.4 reorganizado, frente a v5.3 reorganizado, en frío:** 1.00 veces, +0.2 %: un empate.
- **v5.4 reorganizado, frente a v5.3 reorganizado, en caliente:** 1.02 veces, +2.0 %: un empate.
- **El índice de v5.3 sobre un Map, frente al mismo sobre su Set, en frío:** 0.99 veces, −1.2 %: un empate.
- **El índice de v5.3 sobre un Map, frente al mismo sobre su Set, en caliente:** 1.04 veces, +4.4 %: un empate.

### La puerta: el índice de v5.3, reorganizado sobre un Map, sin cronometrar

| índice | frente al índice tal cual, 5 000 queries por punto | segmentos del bucket de postings | bucket de postings | fichero de vectores |
|---|---|---|---|---|
| el índice de v5.3, tal cual (Set) | — | 1 | 255.28 MB | 6082.56 MB |
| el mismo, reorganizado sobre un Map | los mismos ids, distancias y trabajo | 1 | 261.24 MB | 6082.56 MB |

- **Escribir «el mismo, reorganizado sobre un Map»** llevó 121 s.

| índice | recall | ef | cargas | lecturas por carga | bytes por carga | por el modelo | real entre modelo |
|---|---|---|---|---|---|---|---|
| el índice de v5.3, tal cual (Set) | 0.95 | 21 | 19.50 | 2.00 | 67.03 KB | 63.66 KB | 1.053 |
| el índice de v5.3, tal cual (Set) | 0.99 | 151 | 69.28 | 2.00 | 66.73 KB | 63.37 KB | 1.053 |
| el mismo, reorganizado sobre un Map | 0.95 | 21 | 19.50 | 2.00 | 68.57 KB | 63.66 KB | 1.077 |
| el mismo, reorganizado sobre un Map | 0.99 | 151 | 69.28 | 2.00 | 68.26 KB | 63.37 KB | 1.077 |

**En caliente, sin cronometrar e indicativo:** cada índice en su proceso, alternados, con la pasada de QPS mediana de cada proceso. El Set es «v5.3, reorganizado»; el Map tiene sus mismos postings.

| pasada | Set: p50 a 0.95, 1 thread, ms | Map | Map entre Set | Set: fila de miembros, µs por carga | Map | Set: fila de aristas, µs | Map |
|---|---|---|---|---|---|---|---|
| 1 | 1.744 | 1.772 | 1.016x | 18.8 | 25.7 | 12.3 | 10.1 |
| 2 | 1.425 | 1.503 | 1.055x | 18.5 | 25.8 | 12.3 | 10.4 |
| 3 | 1.363 | 1.859 | 1.365x | 18.8 | 25.9 | 12.2 | 10.8 |

| por la mediana de las pasadas | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |
|---|---|---|---|---|
| Set, p50 en ms | 1.425 | 1.909 | 4.397 | 5.941 |
| Map, p50 en ms | 1.772 | 2.053 | 4.683 | 7.483 |
| Map entre Set | 1.244x | 1.075x | 1.065x | 1.260x |

**La regla de la puerta:**

- **Corrección:** devuelve los mismos ids, distancias y trabajo que el índice de v5.3.
- **Lo que lee el store, compactado, a 0.95:** 1.077 veces lo que usa, con el límite en 1.10: sigue.
- **En caliente, el p50 a 0.95 con 1 thread sobre el Map entre el de sobre el Set:** 1.244, +24.4 %, con el límite en 1.05: **para**.

**La puerta para la corrida.**

### La validación: el índice junto a su gemelo, sin cronometrar

- **Al acabar:** 990 000 vectores, 7 794 postings y 3 687 splits; cobertura completa; frente al gemelo, idéntico.
- **Cerrado y abierto otra vez:** lo mismo que el gemelo en las queries de sus puntos.
- **Checkpoints:** 5, del 20 al 100 %; en todos, cobertura completa e índice idéntico.

**Lo que el store lee por carga,** en los tres estados en que la validación lo deja:

| estado del store | índice | recall | segmentos | lecturas por carga | bytes por carga | lo que usa el gemelo | real entre lo que usa |
|---|---|---|---|---|---|---|---|
| como lo dejan los ciclos | v5.0, un valor por cambio | 0.95 | 3 | 4.81 | 92.5 KB | 63.5 KB | 1.457 |
| como lo dejan los ciclos | v5.0, un valor por cambio | 0.99 | 3 | 4.81 | 91.9 KB | 63.2 KB | 1.454 |
| como lo dejan los ciclos | K = 64 de v5.1, filas en su bucket | 0.95 | 3 | 2.36 | 54.2 KB | 62.7 KB | 0.863 |
| como lo dejan los ciclos | K = 64 de v5.1, filas en su bucket | 0.99 | 3 | 2.35 | 53.9 KB | 62.5 KB | 0.862 |
| como lo dejan los ciclos | v5.3, en un Set | 0.95 | 4 | 5.67 | 143.3 KB | 63.7 KB | 2.249 |
| como lo dejan los ciclos | v5.3, en un Set | 0.99 | 4 | 5.65 | 141.5 KB | 63.3 KB | 2.235 |
| como lo dejan los ciclos | **v5.4, en un Map** | 0.95 | 5 | 6.53 | 87.0 KB | 63.0 KB | 1.381 |
| como lo dejan los ciclos | **v5.4, en un Map** | 0.99 | 5 | 6.51 | 86.6 KB | 62.7 KB | 1.381 |
| volcado, sin compactar | v5.0, un valor por cambio | 0.95 | 4 | 6.59 | 94.3 KB | 63.5 KB | 1.487 |
| volcado, sin compactar | v5.0, un valor por cambio | 0.99 | 4 | 6.58 | 93.8 KB | 63.2 KB | 1.483 |
| volcado, sin compactar | K = 64 de v5.1, filas en su bucket | 0.95 | 4 | 4.13 | 66.4 KB | 62.7 KB | 1.058 |
| volcado, sin compactar | K = 64 de v5.1, filas en su bucket | 0.99 | 4 | 4.12 | 66.1 KB | 62.5 KB | 1.058 |
| volcado, sin compactar | v5.3, en un Set | 0.95 | 5 | 6.88 | 166.4 KB | 63.7 KB | 2.613 |
| volcado, sin compactar | v5.3, en un Set | 0.99 | 5 | 6.87 | 164.6 KB | 63.3 KB | 2.600 |
| volcado, sin compactar | **v5.4, en un Map** | 0.95 | 6 | 8.25 | 99.1 KB | 63.0 KB | 1.573 |
| volcado, sin compactar | **v5.4, en un Map** | 0.99 | 6 | 8.22 | 98.6 KB | 62.7 KB | 1.573 |
| compactado | v5.0, un valor por cambio | 0.95 | 2 | 3.76 | 70.1 KB | 63.5 KB | 1.105 |
| compactado | v5.0, un valor por cambio | 0.99 | 2 | 3.76 | 69.9 KB | 63.2 KB | 1.105 |
| compactado | K = 64 de v5.1, filas en su bucket | 0.95 | 2 | 2.76 | 66.3 KB | 62.7 KB | 1.057 |
| compactado | K = 64 de v5.1, filas en su bucket | 0.99 | 2 | 2.76 | 66.1 KB | 62.5 KB | 1.057 |
| compactado | v5.3, en un Set | 0.95 | 1 | 2.00 | 67.3 KB | 63.7 KB | 1.057 |
| compactado | v5.3, en un Set | 0.99 | 1 | 2.00 | 66.9 KB | 63.3 KB | 1.057 |
| compactado | **v5.4, en un Map** | 0.95 | 1 | 2.00 | 68.2 KB | 63.0 KB | 1.082 |
| compactado | **v5.4, en un Map** | 0.99 | 1 | 2.00 | 67.8 KB | 62.7 KB | 1.081 |

- **En el primer estado el store cuenta de menos** en todos: lo que está en el memtable no pasa por él.

### El índice de las curvas: lo que su crecimiento escribió, sin cronometrar

| índice | postings | cobertura | 0.95: ef | 0.99: ef | el store escribió | lo que ocupa al final | entre lo que ocupa | filas escritas | lápidas de filas | lápidas de clave |
|---|---|---|---|---|---|---|---|---|---|---|
| v5.0, un valor por cambio | 7 788 | completa | 21 | 158 | 9.43 GB | 567.7 MB | 16.6 veces | — | — | — |
| K = 64 de v5.1, filas en su bucket | 7 716 | completa | 21 | 166 | 7.74 GB | 553.9 MB | 14.0 veces | 84 537, 0.90 GB | — | — |
| v5.3, filas junto a los miembros, en un Set | 7 762 | completa | 21 | 151 | 17.08 GB | 540.1 MB | 31.6 veces | 84 336, 0.90 GB | 76 994, 0.81 GB | — |
| **v5.4, filas junto a los miembros, en un Map** | 7 792 | completa | 21 | 164 | 11.78 GB | 585.7 MB | 20.1 veces | 84 498, 0.90 GB | — | 1 335 116, 8.0 MB |

**Por dónde lo escribió,** en GB. El bucket de postings es el Set o el Map; los demás son Replace:

| índice | postings: log de escritura | postings: compactación | postings: lo demás | los demás buckets |
|---|---|---|---|---|
| v5.0, un valor por cambio | 1.33 | 6.17 | 0.00 | 1.93 |
| K = 64 de v5.1, filas en su bucket | 0.53 | 2.84 | 0.00 | 4.37 |
| v5.3, filas junto a los miembros, en un Set | 2.25 | 13.02 | 0.00 | 1.80 |
| **v5.4, filas junto a los miembros, en un Map** | 1.54 | 8.44 | 0.00 | 1.80 |

- **Los postings** cuentan los retirados: son los ids dados.
- **El crecimiento no se cronometra:** corre de día, con otras cosas en la máquina.

### El índice reorganizado: corrección y lo que lee el store, sin cronometrar

| índice | frente al índice tal cual, 5 000 queries por punto | segmentos del bucket de postings | bucket de postings | fichero de vectores |
|---|---|---|---|---|
| v5.4, tal cual | — | 2 | 300.84 MB | 6082.56 MB |
| v5.4, reorganizado | los mismos ids, distancias y trabajo | 1 | 261.26 MB | 6082.56 MB |

- **Escribir «v5.4, reorganizado»** llevó 36 s.

| índice | recall | ef | cargas | lecturas por carga | bytes por carga | por el modelo | real entre modelo |
|---|---|---|---|---|---|---|---|
| v5.4, tal cual | 0.95 | 21 | 19.60 | 3.39 | 79.66 KB | 63.59 KB | 1.253 |
| v5.4, tal cual | 0.99 | 164 | 73.32 | 3.39 | 79.30 KB | 63.27 KB | 1.253 |
| v5.4, reorganizado | 0.95 | 21 | 19.60 | 2.00 | 68.49 KB | 63.59 KB | 1.077 |
| v5.4, reorganizado | 0.99 | 164 | 73.32 | 2.00 | 68.15 KB | 63.27 KB | 1.077 |

### Lo que lee una carga en frío, fila a fila, sin cronometrar

Las 500 primeras queries, de una en una, con las filas de cada carga leídas una detrás de otra. Las filas de v5.0, de v5.1 y de v5.3 son las que se contaron el 20: no se vuelven a medir.

**Lo que cuenta el store,** en modo pread. Es lo mismo en frío que en caliente.

| índice | recall | cargas por query | fila de miembros: lecturas | bytes | fila de aristas: lecturas | bytes | las dos: lecturas | bytes |
|---|---|---|---|---|---|---|---|---|
| K = 64 de v5.1, tal cual | 0.95 | 18.77 | 1.92 | 55.72 KB | 1.00 | 11.58 KB | 2.92 | 67.30 KB |
| K = 64 de v5.1, tal cual | 0.99 | 73.49 | 1.92 | 55.52 KB | 1.00 | 11.54 KB | 2.92 | 67.06 KB |
| crecido de v5.0, tal cual | 0.95 | 18.58 | 1.95 | 55.60 KB | 1.96 | 16.71 KB | 3.91 | 72.31 KB |
| crecido de v5.0, tal cual | 0.99 | 71.31 | 1.95 | 55.26 KB | 1.96 | 16.61 KB | 3.91 | 71.87 KB |
| v5.3, tal cual (en un segmento) | 0.95 | 18.73 | 1.00 | 55.63 KB | 1.00 | 11.55 KB | 2.00 | 67.18 KB |
| v5.3, tal cual (en un segmento) | 0.99 | 68.91 | 1.00 | 55.40 KB | 1.00 | 11.52 KB | 2.00 | 66.91 KB |
| **v5.4, tal cual** | 0.95 | 18.82 | 1.39 | 57.15 KB | 1.99 | 22.80 KB | 3.39 | 79.95 KB |
| **v5.4, tal cual** | 0.99 | 72.77 | 1.39 | 56.75 KB | 2.00 | 22.66 KB | 3.39 | 79.41 KB |
| K = 64 de v5.1, reorganizado | 0.95 | 18.77 | 1.00 | 55.71 KB | 1.00 | 11.58 KB | 2.00 | 67.29 KB |
| K = 64 de v5.1, reorganizado | 0.99 | 73.49 | 1.00 | 55.51 KB | 1.00 | 11.54 KB | 2.00 | 67.04 KB |
| la celda 2 de v5.0 | 0.95 | 18.58 | 1.00 | 55.59 KB | 1.00 | 13.57 KB | 2.00 | 69.16 KB |
| la celda 2 de v5.0 | 0.99 | 71.31 | 1.00 | 55.25 KB | 1.00 | 13.52 KB | 2.00 | 68.77 KB |
| v5.3, reorganizado | 0.95 | 18.73 | 1.00 | 55.63 KB | 1.00 | 11.55 KB | 2.00 | 67.18 KB |
| v5.3, reorganizado | 0.99 | 68.91 | 1.00 | 55.40 KB | 1.00 | 11.52 KB | 2.00 | 66.91 KB |
| **v5.4, reorganizado** | 0.95 | 18.82 | 1.00 | 57.15 KB | 1.00 | 11.55 KB | 2.00 | 68.69 KB |
| **v5.4, reorganizado** | 0.99 | 72.77 | 1.00 | 56.74 KB | 1.00 | 11.47 KB | 2.00 | 68.22 KB |

| índice | segmentos: miembros | filas de aristas | bucket de miembros | bucket de filas de aristas |
|---|---|---|---|---|
| K = 64 de v5.1, tal cual | 2 | 1 | 228.76 MB | 40.26 MB |
| crecido de v5.0, tal cual | 2 | — (van en el de miembros) | 282.78 MB | — |
| v5.3, tal cual (en un segmento) | 1 | — (van en el de miembros) | 255.28 MB | — |
| **v5.4, tal cual** | 2 | — (van en el de miembros) | 300.84 MB | — |
| K = 64 de v5.1, reorganizado | 1 | 1 | 215.07 MB | 40.26 MB |
| la celda 2 de v5.0 | 1 | — (van en el de miembros) | 263.22 MB | — |
| v5.3, reorganizado | 1 | — (van en el de miembros) | 255.28 MB | — |
| **v5.4, reorganizado** | 1 | — (van en el de miembros) | 261.26 MB | — |

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
| v5.3, tal cual (en un segmento) | 0.95 | los miembros | 4.13 | 93.3 KB | 0.63 | 12.6 KB | 4.75 | 105.9 KB | 1324.7 KB |
| v5.3, tal cual (en un segmento) | 0.95 | las aristas | 3.19 | 53.7 KB | 1.56 | 70.8 KB | 4.75 | 124.5 KB | 1313.1 KB |
| v5.3, tal cual (en un segmento) | 0.99 | los miembros | 3.77 | 81.5 KB | 0.55 | 10.6 KB | 4.32 | 92.1 KB | 1311.3 KB |
| v5.3, tal cual (en un segmento) | 0.99 | las aristas | 2.91 | 46.4 KB | 1.41 | 61.8 KB | 4.32 | 108.2 KB | 1310.9 KB |
| **v5.4, tal cual** | 0.95 | los miembros | 4.54 | 113.2 KB | 1.71 | 42.5 KB | 6.25 | 155.8 KB | 1373.8 KB |
| **v5.4, tal cual** | 0.95 | las aristas | 3.28 | 57.0 KB | 2.97 | 117.7 KB | 6.25 | 174.7 KB | 1313.5 KB |
| **v5.4, tal cual** | 0.99 | los miembros | 4.11 | 95.3 KB | 1.45 | 32.7 KB | 5.55 | 128.0 KB | 1312.8 KB |
| **v5.4, tal cual** | 0.99 | las aristas | 2.98 | 49.3 KB | 2.57 | 95.3 KB | 5.55 | 144.6 KB | 1311.1 KB |
| K = 64 de v5.1, reorganizado | 0.95 | los miembros | 4.07 | 93.1 KB | 1.44 | 48.3 KB | 5.51 | 141.3 KB | 1203.5 KB |
| K = 64 de v5.1, reorganizado | 0.95 | las aristas | 4.07 | 93.1 KB | 1.44 | 48.3 KB | 5.51 | 141.3 KB | 1203.5 KB |
| K = 64 de v5.1, reorganizado | 0.99 | los miembros | 3.66 | 80.5 KB | 1.20 | 34.9 KB | 4.86 | 115.4 KB | 1217.4 KB |
| K = 64 de v5.1, reorganizado | 0.99 | las aristas | 3.66 | 80.5 KB | 1.20 | 34.9 KB | 4.86 | 115.4 KB | 1217.5 KB |
| la celda 2 de v5.0 | 0.95 | los miembros | 4.13 | 94.0 KB | 0.73 | 14.2 KB | 4.86 | 108.2 KB | 1205.4 KB |
| la celda 2 de v5.0 | 0.95 | las aristas | 3.19 | 53.9 KB | 1.68 | 72.3 KB | 4.86 | 126.2 KB | 1205.0 KB |
| la celda 2 de v5.0 | 0.99 | los miembros | 3.78 | 81.4 KB | 0.63 | 12.2 KB | 4.40 | 93.6 KB | 1223.0 KB |
| la celda 2 de v5.0 | 0.99 | las aristas | 2.91 | 46.2 KB | 1.50 | 62.5 KB | 4.40 | 108.7 KB | 1223.1 KB |
| v5.3, reorganizado | 0.95 | los miembros | 4.13 | 93.3 KB | 0.63 | 12.6 KB | 4.75 | 105.9 KB | 1202.0 KB |
| v5.3, reorganizado | 0.95 | las aristas | 3.19 | 53.7 KB | 1.56 | 70.8 KB | 4.75 | 124.5 KB | 1201.9 KB |
| v5.3, reorganizado | 0.99 | los miembros | 3.77 | 81.5 KB | 0.55 | 10.6 KB | 4.32 | 92.1 KB | 1223.7 KB |
| v5.3, reorganizado | 0.99 | las aristas | 2.91 | 46.4 KB | 1.41 | 61.8 KB | 4.32 | 108.2 KB | 1223.5 KB |
| **v5.4, reorganizado** | 0.95 | los miembros | 4.21 | 95.2 KB | 0.61 | 12.2 KB | 4.83 | 107.4 KB | 1196.9 KB |
| **v5.4, reorganizado** | 0.95 | las aristas | 3.28 | 57.4 KB | 1.55 | 69.4 KB | 4.83 | 126.8 KB | 1196.9 KB |
| **v5.4, reorganizado** | 0.99 | los miembros | 3.83 | 82.4 KB | 0.53 | 10.5 KB | 4.36 | 92.9 KB | 1216.2 KB |
| **v5.4, reorganizado** | 0.99 | las aristas | 2.98 | 49.4 KB | 1.39 | 60.3 KB | 4.36 | 109.7 KB | 1216.2 KB |

- **El resto de la query** es su rescore, que lee los vectores con lecturas posicionales y no con page-ins.
- **Con las filas leídas a la vez,** como en las curvas, las dos lecturas de una carga se solapan: lo que se cuenta aquí es cuánto disco toca cada fila, no cuánto espera la ronda.

### Latencia, primera ronda

**Recall 0.95, en frío:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| v5.4, reorganizado | ef 21 | 4.11 | 10.01 | 15.21 | 203 | 12.25 | 24.98 | 35.83 | 496 |
| v5.3, reorganizado | ef 21 | 4.10 | 10.33 | 15.79 | 204 | 12.21 | 25.52 | 34.45 | 496 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 21 | 3.71 | 8.39 | 13.20 | 230 | 11.85 | 23.53 | 30.89 | 528 |
| el índice de v5.3, reorganizado sobre un Map (mía) | ef 21 | 4.05 | 10.03 | 15.53 | 208 | 12.18 | 25.65 | 35.08 | 492 |

**Recall 0.95, en caliente:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| v5.4, reorganizado | ef 21 | 1.14 | 3.86 | 6.02 | 649 | 1.64 | 5.56 | 8.59 | 3 610 |
| v5.3, reorganizado | ef 21 | 1.12 | 3.87 | 5.96 | 664 | 1.55 | 5.22 | 7.84 | 3 862 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 21 | 0.92 | 2.98 | 4.44 | 820 | 1.29 | 4.25 | 6.38 | 4 647 |
| el índice de v5.3, reorganizado sobre un Map (mía) | ef 21 | 1.17 | 4.02 | 6.18 | 638 | 1.64 | 5.56 | 8.41 | 3 626 |

**Recall 0.99, en frío:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| v5.4, reorganizado | ef 164 | 9.90 | 30.91 | 45.19 | 78 | 25.66 | 61.46 | 76.26 | 217 |
| v5.3, reorganizado | ef 151 | 9.41 | 29.40 | 42.59 | 83 | 25.03 | 60.80 | 73.54 | 225 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 187 | 8.91 | 27.60 | 39.19 | 86 | 23.02 | 57.13 | 70.99 | 234 |
| el índice de v5.3, reorganizado sobre un Map (mía) | ef 151 | 9.46 | 30.04 | 43.40 | 82 | 24.09 | 59.03 | 72.37 | 228 |

**Recall 0.99, en caliente:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| v5.4, reorganizado | ef 164 | 3.90 | 13.32 | 19.58 | 194 | 5.52 | 18.83 | 27.69 | 1 099 |
| v5.3, reorganizado | ef 151 | 3.63 | 12.36 | 17.85 | 209 | 4.73 | 16.36 | 24.03 | 1 263 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 187 | 3.23 | 10.99 | 16.01 | 233 | 4.35 | 14.53 | 21.60 | 1 390 |
| el índice de v5.3, reorganizado sobre un Map (mía) | ef 151 | 3.75 | 12.72 | 18.49 | 201 | 5.25 | 18.30 | 26.11 | 1 147 |

**Cada índice entre la celda en lote,** por el p50:

| índice | estado | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |
|---|---|---|---|---|---|
| v5.4, reorganizado | en frío | 1.11x | 1.03x | 1.11x | 1.11x |
| v5.4, reorganizado | en caliente | 1.23x | 1.27x | 1.21x | 1.27x |
| v5.3, reorganizado | en frío | 1.11x | 1.03x | 1.06x | 1.09x |
| v5.3, reorganizado | en caliente | 1.21x | 1.19x | 1.12x | 1.09x |
| el índice de v5.3, reorganizado sobre un Map (mía) | en frío | 1.09x | 1.03x | 1.06x | 1.05x |
| el índice de v5.3, reorganizado sobre un Map (mía) | en caliente | 1.26x | 1.27x | 1.16x | 1.21x |

**De la latencia media a 0.95 con 1 thread, en frío,** y lo que cuesta traer cada fila, por carga:

| índice | media, ms | traer los bloques | scan y ruteo | rescore | cargas | fila de miembros, µs | fila de aristas, µs |
|---|---|---|---|---|---|---|---|
| v5.4, reorganizado | 4.93 | 2.38 | 0.80 | 1.61 | 18.82 | 299.4 | 238.4 |
| v5.3, reorganizado | 4.90 | 2.35 | 0.81 | 1.59 | 18.73 | 290.6 | 245.1 |
| escrito en lote (v4.0, 512 bits en RAM) | 4.35 | 1.97 | 0.65 | 1.59 | 17.83 | 265.7 | 219.6 |
| el índice de v5.3, reorganizado sobre un Map (mía) | 4.80 | 2.31 | 0.80 | 1.54 | 18.73 | 294.8 | 235.5 |

**De la latencia media a 0.95 con 1 thread, en caliente,** y lo que cuesta traer cada fila, por carga:

| índice | media, ms | traer los bloques | scan y ruteo | rescore | cargas | fila de miembros, µs | fila de aristas, µs |
|---|---|---|---|---|---|---|---|
| v5.4, reorganizado | 1.54 | 0.46 | 0.83 | 0.13 | 19.60 | 21.4 | 9.6 |
| v5.3, reorganizado | 1.51 | 0.42 | 0.83 | 0.14 | 19.50 | 15.8 | 10.8 |
| escrito en lote (v4.0, 512 bits en RAM) | 1.22 | 0.30 | 0.67 | 0.14 | 18.59 | 14.2 | 7.0 |
| el índice de v5.3, reorganizado sobre un Map (mía) | 1.57 | 0.48 | 0.84 | 0.14 | 19.50 | 21.7 | 9.5 |

### Las curvas: dos rondas, deriva y actividad

| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |
|---|---|---|---|---|---|
| v5.4, reorganizado | 17:21 a 17:27 | 17:44 a 17:50 | no | −3.2 % (0.95, 8 threads, en frío: 12.25 y 11.86 ms) | — |
| v5.3, reorganizado | 17:27 a 17:33 | 17:50 a 17:56 | no | +3.1 % (0.99, 8 threads, en caliente: 4.73 y 4.88 ms) | — |
| escrito en lote (v4.0, 512 bits en RAM) | 17:33 a 17:38 | 17:56 a 18:01 | no | −2.6 % (0.95, 1 thread, en frío: 3.71 y 3.62 ms) | — |
| el índice de v5.3, reorganizado sobre un Map (mía) | 17:38 a 17:44 | 18:01 a 18:07 | no | −2.1 % (0.95, 8 threads, en frío: 12.18 y 11.93 ms) | — |

- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.
- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.

### RAM residente al buscar

| índice | heap en uso tras abrir | huella del proceso |
|---|---|---|
| v5.4, reorganizado | 179.45 MB | 380.68 MB |
| v5.3, reorganizado | 179.23 MB | 379.50 MB |
| escrito en lote (v4.0, 512 bits en RAM) | 145.87 MB | 288.06 MB |
| el índice de v5.3, reorganizado sobre un Map (mía) | 179.26 MB | 379.65 MB |

