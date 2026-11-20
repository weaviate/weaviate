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

### La sesión cronometrada

Sin cronometrar todavía.
