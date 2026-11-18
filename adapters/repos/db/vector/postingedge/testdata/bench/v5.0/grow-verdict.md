### Validación: del 10 % al 100 %, en disco, junto al gemelo en memoria

- **Dataset:** dbpedia-openai-1000k-angular.bin, 990 000 vectores, 5 000 queries, k = 10.
- **Camino:** split con T = 360, ef de inserción 160, códigos de ruteo de 512 bits en RAM, 16 vecinos.
- **El store:** sus ciclos de volcado y compactación se llaman cada 1 000 inserts; lee en modo pread, que es donde cuenta.
- **Compactación mientras crece:** por niveles, la que `lsmkv` tiene por defecto: lo reemplazado se acumula entre compactaciones.

| checkpoint | vivos | postings | splits | cobertura, leyendo el store | frente al gemelo |
|---|---|---|---|---|---|
| 10 % | 99 000 | 420 | 0 | completa | idéntico |
| 20 % | 198 000 | 1 232 | 406 | completa | idéntico |
| 40 % | 396 000 | 2 838 | 1 209 | completa | idéntico |
| 60 % | 594 000 | 4 478 | 2 029 | completa | idéntico |
| 80 % | 792 000 | 6 132 | 2 856 | completa | idéntico |
| 100 % | 990 000 | 7 746 | 3 663 | completa | idéntico |
| 100 %, compactado | 990 000 | 7 746 | 3 663 | completa | idéntico |

- **Cobertura completa e índice idéntico al gemelo en todos los checkpoints.**
- **En cada insert,** además, el mismo posting, las mismas aristas y las mismas cuentas que el gemelo: la corrida para en la primera diferencia.
- **Cerrado y abierto otra vez desde sus ficheros,** devuelve lo mismo que el gemelo en todas las queries, en los dos puntos.

### Coste por insert: lecturas

Por insert, de media en el tramo. Las del store son las que cuenta el store; lo que aún está en la memtable no es una lectura.

| tramo | cargas | rondas | búsqueda: lecturas del store | bytes | vectores del rescore | inversas: lecturas de distancias | bytes | split: lecturas del store | bytes | vectores |
|---|---|---|---|---|---|---|---|---|---|---|
| del 10 al 20 % | 51.0 | 12.5 | 150.9 | 3.46 MB | 64 | 7.8 | 0.29 KB | 0.014 | 0.41 KB | 1.49 |
| del 20 al 40 % | 57.6 | 14.2 | 263.7 | 4.67 MB | 64 | 8.4 | 0.32 KB | 0.022 | 0.48 KB | 1.46 |
| del 40 al 60 % | 63.2 | 15.6 | 345.1 | 5.31 MB | 64 | 8.8 | 0.34 KB | 0.026 | 0.50 KB | 1.50 |
| del 60 al 80 % | 67.2 | 16.5 | 389.4 | 6.01 MB | 64 | 9.0 | 0.35 KB | 0.029 | 0.55 KB | 1.51 |
| del 80 al 100 % | 70.3 | 17.3 | 437.7 | 5.96 MB | 64 | 9.2 | 0.37 KB | 0.028 | 0.49 KB | 1.47 |
| toda la corrida | 63.1 | 15.5 | 335.9 | 5.26 MB | 64 | 8.7 | 0.34 KB | 0.025 | 0.49 KB | 1.48 |

**Por carga, en la búsqueda de un insert,** con el store como lo van dejando sus ciclos:

| tramo | lecturas del store por carga | bytes del store por carga | lo que la carga devuelve | el store entre lo devuelto |
|---|---|---|---|---|
| del 10 al 20 % | 2.96 | 67.77 KB | 61.97 KB | 1.094 |
| del 20 al 40 % | 4.58 | 81.07 KB | 62.74 KB | 1.292 |
| del 40 al 60 % | 5.46 | 84.04 KB | 63.54 KB | 1.323 |
| del 60 al 80 % | 5.80 | 89.52 KB | 63.72 KB | 1.405 |
| del 80 al 100 % | 6.23 | 84.86 KB | 63.87 KB | 1.329 |
| toda la corrida | 5.33 | 83.47 KB | 63.36 KB | 1.317 |

- **Lo que la carga devuelve** son sus miembros y sus listas vivas, 2 filas. Lo que el store lee para dárselo lleva además el marco de cada valor, las listas reemplazadas y sus lápidas, y no lleva lo que aún está en la memtable.

- **La búsqueda de un insert devuelve 126.1 filas,** 4.00 MB: las de la memtable también. El store cuenta 335.9 lecturas.
- **Las inversas no leen ninguna fila:** 0 listas releídas y 0 vectores exactos de más en toda la corrida.
- **Las lecturas de distancias que cuenta el store** son 5.11 por insert: el resto las sirve la memtable.
- **Un split,** cada 243 inserts: lee las dos filas de su posting y los vectores exactos de sus miembros, 361 de media.

### Coste por insert: escrituras

Lo que se entrega al store o a sus ficheros, por insert, en valores y en bytes de valor.

| tramo | miembro | bytes | aristas propias | bytes | inversas: aristas | en postings | valores | bytes | splits, repartidos | bytes | total |
|---|---|---|---|---|---|---|---|---|---|---|---|
| del 10 al 20 % | 4 | 6.43 KB | 2 | 0.07 KB | 4.13 | 2.77 | 12.38 | 0.54 KB | 4.47 | 0.41 KB | 7.45 KB |
| del 20 al 40 % | 4 | 6.43 KB | 2 | 0.07 KB | 4.46 | 3.10 | 13.38 | 0.59 KB | 4.40 | 0.41 KB | 7.50 KB |
| del 40 al 60 % | 4 | 6.43 KB | 2 | 0.08 KB | 4.63 | 3.29 | 13.89 | 0.62 KB | 4.49 | 0.42 KB | 7.55 KB |
| del 60 al 80 % | 4 | 6.43 KB | 2 | 0.08 KB | 4.73 | 3.41 | 14.20 | 0.64 KB | 4.53 | 0.43 KB | 7.58 KB |
| del 80 al 100 % | 4 | 6.43 KB | 2 | 0.08 KB | 4.82 | 3.50 | 14.47 | 0.66 KB | 4.42 | 0.42 KB | 7.59 KB |
| toda la corrida | 4 | 6.43 KB | 2 | 0.08 KB | 4.60 | 3.26 | 13.80 | 0.62 KB | 4.46 | 0.42 KB | 7.54 KB |

- **Miembro:** su valor en la fila (0.21 KB), su entrada del mapa (4 B), su código de ruteo (0.07 KB) y su vector exacto (6.14 KB).
- **Aristas propias:** su lista (40 B de media) y sus distancias (36 B).
- **Una arista inversa** son tres valores: la lista nueva (46 B), la lápida de la vieja (46 B) y las distancias (42 B).
- **Un split** escribe las dos filas de cada hijo, 88.48 KB, y 361 entradas del mapa.
- **Sin el vector exacto, un insert entrega al store 1.35 KB.**

### El store: tamaño y amplificación de escritura

| | postings | mapa | códigos de ruteo | distancias de aristas | total del store | segmentos del bucket de postings |
|---|---|---|---|---|---|---|
| 10 % | 25.84 MB | 6.22 MB | 12.95 MB | 8.84 MB | 53.85 MB | 1 |
| 20 % | 68.85 MB | 13.85 MB | 22.75 MB | 42.38 MB | 147.84 MB | 2 |
| 40 % | 192.51 MB | 28.98 MB | 51.13 MB | 58.61 MB | 331.23 MB | 4 |
| 60 % | 256.76 MB | 44.29 MB | 75.12 MB | 89.53 MB | 465.71 MB | 2 |
| 80 % | 249.45 MB | 59.69 MB | 103.50 MB | 109.73 MB | 522.36 MB | 2 |
| 100 % | 441.26 MB | 74.85 MB | 127.49 MB | 174.54 MB | 818.14 MB | 3 |
| **100 %, compactado** | 267.18 MB | 62.17 MB | 129.49 MB | 93.50 MB | **552.33 MB** | 2 |

- **Compactación final,** forzada: 3 rondas.
- **Lo que el store escribió en sus ficheros,** en toda la corrida con su compactación final: 8 873.97 MB. Por operación: compaction 5 497.30 MB, appendWAL 1 848.61 MB, flushMemtable 1 541.66 MB, segmentMetadata 31.31 MB.
  - Lo de antes de empezar a insertar, 44.91 MB, no cuenta: es de cuando se abrió la base.
- **Amplificación de escritura:** 17.80 veces lo que el store creció (498.48 MB), y 7.38 veces lo que se le entregó (1 202.09 MB).
- **El bucket de postings compactado:** 267.18 MB, frente a los 243.0 MB de la celda de 512 bits en lote de v4.0.
- **El grafo de centroides:** 2.39 MB de log, y 47.59 MB de vectores de centroide, con los 3 663 postings retirados.

### Lo que lee una búsqueda sobre el índice crecido, antes y después de compactar

Por query y por carga, sobre las 5 000 queries. Los puntos son los del gemelo, que es el mismo índice. El modelo son 208 B por miembro y 4 B por arista de los postings que carga.

| estado del store | recall | ef | recall medido | cargas | rondas | segmentos | lecturas por query | bytes | por carga: lecturas | bytes | por el modelo | real entre modelo |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| como lo dejan los ciclos | 0.95 | 21 | 0.9504 | 19.63 | 4.66 | 3 | 94.5 | 1.81 MB | 4.81 | 92.45 KB | 63.45 KB | 1.457 |
| como lo dejan los ciclos | 0.99 | 158 | 0.9900 | 72.09 | 17.77 | 3 | 346.4 | 6.62 MB | 4.81 | 91.89 KB | 63.20 KB | 1.454 |
| volcado, sin compactar | 0.95 | 21 | 0.9504 | 19.63 | 4.66 | 4 | 129.3 | 1.85 MB | 6.59 | 94.34 KB | 63.45 KB | 1.487 |
| volcado, sin compactar | 0.99 | 158 | 0.9900 | 72.09 | 17.77 | 4 | 474.3 | 6.76 MB | 6.58 | 93.75 KB | 63.20 KB | 1.483 |
| compactado | 0.95 | 21 | 0.9504 | 19.63 | 4.66 | 2 | 73.8 | 1.38 MB | 3.76 | 70.14 KB | 63.45 KB | 1.105 |
| compactado | 0.99 | 158 | 0.9900 | 72.09 | 17.77 | 2 | 270.8 | 5.04 MB | 3.76 | 69.86 KB | 63.20 KB | 1.105 |

- **En el primer estado el store cuenta de menos:** lo que está en la memtable no pasa por él.

**Lo que pesa la basura:** el estado volcado frente al compactado, por carga.

| recall | lecturas por carga: volcado | compactado | bytes por carga: volcado | compactado | de más |
|---|---|---|---|---|---|
| 0.95 | 6.59 | 3.76 | 94.34 KB | 70.14 KB | +34.5 % |
| 0.99 | 6.58 | 3.76 | 93.75 KB | 69.86 KB | +34.2 % |

### Criterio 1: frente al índice crecido en memoria

| a 0.95, compactado, por query | bytes | sobre el modelo |
|---|---|---|
| el modelo: 208 B por miembro y 4 B por arista | 1 245.47 KB | |
| el id del miembro en su lista de aristas, 4 B | 20.10 KB | +1.6 % |
| el marco de cada valor del Set, 9 B, dos valores por miembro | 90.46 KB | +7.3 % |
| la cabecera y la clave de cada fila, 17 B, dos filas por carga | 0.67 KB | +0.1 % |
| lo demás | 20.08 KB | +1.6 % |
| **lo que lee el store** | **1 376.78 KB** | **+10.5 %** |

**Criterio 1, NO se cumple, por lo que el store deja sin compactar encima del formato, y no por el algoritmo:** a 0.95, compactado, el store lee 1.105 veces lo que el gemelo en memoria por el modelo; el límite es 1.10.

- **El algoritmo no pone nada:** las cargas son las del gemelo, 19.63 por query, y en cada una de las 5 000 queries el índice de disco devuelve sus mismos ids, distancias y trabajo.
- **El formato solo son 1.089 veces:** el marco del Set, el id de cada lista y las cabeceras de fila.
- **Lo demás, un 1.6 %, es lo que la compactación del store deja:** el bucket queda en 2 segmentos y una carga lee 3.76 filas en vez de 2. `lsmkv` no junta dos segmentos de tamaños muy distintos, y lo que el segundo reemplaza del primero se lee de los dos, con sus lápidas.
- **A 0.99,** 1.105 veces.

### Inserts por segundo

Sin cronometrar todavía.

### Latencia de búsqueda: el índice crecido frente al escrito en lote

Sin cronometrar todavía.

### Lo que vale y lo que no

- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre índices, medidos igual y en la misma sesión.
- **Un escritor, y ninguna búsqueda durante los inserts.**
- **El índice validado y el cronometrado son dos corridas:** la misma secuencia de inserts, con otro grafo de centroides.
- **Sin medir:** el halo en este camino, el refresco, los deletes, la durabilidad y Linux.
