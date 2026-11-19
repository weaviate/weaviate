### Corrección: con todo volcado, lo mismo que el gemelo en memoria

- **Dataset:** dbpedia-openai-1000k-angular.bin, 990 000 vectores, 5 000 queries, k = 10. Del 10 % al 100 %.
- **Camino:** el de v5.0: split con T = 360, ef de inserción 160, códigos de ruteo de 512 bits en RAM, 16 vecinos.
- **Compactación mientras crece:** por niveles, como en la validación de v5.0.
- **En cada checkpoint** se escribe primero lo pendiente de todos los postings. La cobertura y la comparación con el gemelo leen después el store.

| K | checkpoint | vivos | postings | splits | esperaban: miembros | en postings | cobertura, leyendo el store | frente al gemelo |
|---|---|---|---|---|---|---|---|---|
| 16 | 10 % | 99 000 | 420 | 0 | 0 | 0 | completa | idéntico |
| 16 | 20 % | 198 000 | 1 238 | 409 | 6 062 | 771 | completa | idéntico |
| 16 | 40 % | 396 000 | 2 840 | 1 210 | 11 634 | 1 491 | completa | idéntico |
| 16 | 60 % | 594 000 | 4 512 | 2 046 | 17 336 | 2 240 | completa | idéntico |
| 16 | 80 % | 792 000 | 6 066 | 2 823 | 22 655 | 2 979 | completa | idéntico |
| 16 | 100 % | 990 000 | 7 768 | 3 674 | 29 697 | 3 778 | completa | idéntico |
| 16 | 100 %, compactado | 990 000 | 7 768 | 3 674 | 0 | 0 | completa | idéntico |
| 64 | 10 % | 99 000 | 420 | 0 | 0 | 0 | completa | idéntico |
| 64 | 20 % | 198 000 | 1 236 | 408 | 22 703 | 808 | completa | idéntico |
| 64 | 40 % | 396 000 | 2 876 | 1 228 | 44 986 | 1 611 | completa | idéntico |
| 64 | 60 % | 594 000 | 4 502 | 2 041 | 68 172 | 2 402 | completa | idéntico |
| 64 | 80 % | 792 000 | 6 112 | 2 846 | 90 649 | 3 204 | completa | idéntico |
| 64 | 100 % | 990 000 | 7 818 | 3 699 | 114 938 | 4 017 | completa | idéntico |
| 64 | 100 %, compactado | 990 000 | 7 818 | 3 699 | 0 | 0 | completa | idéntico |

- **K = 16:** cobertura completa e índice idéntico al gemelo en todos los checkpoints, con todo volcado. Cerrado y abierto otra vez, devuelve lo mismo que el gemelo en las 5 000 queries, en los dos puntos.
- **K = 64:** cobertura completa e índice idéntico al gemelo en todos los checkpoints, con todo volcado. Cerrado y abierto otra vez, devuelve lo mismo que el gemelo en las 5 000 queries, en los dos puntos.
- **En cada insert,** además, el mismo posting, las mismas aristas y las mismas cuentas que el gemelo, con lo pendiente sin volcar: la corrida para en la primera diferencia.

### Lecturas por carga mientras crece

En la búsqueda de un insert, con el store como lo van dejando sus ciclos. Las cuenta el store: lo que está en la memtable no es una lectura. Una carga pide dos filas, la de miembros y la de aristas.

| tramo | v5.0, un valor por cambio: lecturas por carga | K = 16: lecturas por carga | K = 64: lecturas por carga | K = 16 entre v5.0 | K = 64 entre v5.0 |
|---|---|---|---|---|---|
| del 10 al 20 % | 2.96 | 1.16 | 1.24 | 0.39 | 0.42 |
| del 20 al 40 % | 4.58 | 2.18 | 2.20 | 0.48 | 0.48 |
| del 40 al 60 % | 5.46 | 2.78 | 2.84 | 0.51 | 0.52 |
| del 60 al 80 % | 5.80 | 3.17 | 3.21 | 0.55 | 0.55 |
| del 80 al 100 % | 6.23 | 3.40 | 3.43 | 0.55 | 0.55 |
| toda la corrida | 5.33 | 2.76 | 2.80 | 0.52 | 0.53 |

| tramo | v5.0, un valor por cambio: bytes del store por carga | K = 16: bytes del store por carga | K = 64: bytes del store por carga | v5.0, un valor por cambio: lo que la carga devuelve | K = 16: lo que la carga devuelve | K = 64: lo que la carga devuelve |
|---|---|---|---|---|---|---|
| del 10 al 20 % | 67.77 KB | 46.39 KB | 46.88 KB | 61.97 KB | 62.13 KB | 61.86 KB |
| del 20 al 40 % | 81.07 KB | 53.71 KB | 54.19 KB | 62.74 KB | 62.91 KB | 62.78 KB |
| del 40 al 60 % | 84.04 KB | 59.87 KB | 59.89 KB | 63.54 KB | 63.77 KB | 63.37 KB |
| del 60 al 80 % | 89.52 KB | 61.82 KB | 62.07 KB | 63.72 KB | 63.94 KB | 63.83 KB |
| del 80 al 100 % | 84.86 KB | 63.23 KB | 63.38 KB | 63.87 KB | 64.44 KB | 64.24 KB |
| toda la corrida | 83.47 KB | 58.70 KB | 58.94 KB | 63.36 KB | 63.65 KB | 63.44 KB |

- **El contador del store no distingue buckets.** Con aristas consolidadas la fila de aristas es un solo valor: como mucho una lectura por carga. Lo demás son lecturas de la fila de miembros.

**Primera mitad del criterio: las lecturas por carga del último tramo, a la mitad o menos de las de v5.0.**

| | lecturas por carga, del 80 al 100 % | entre las de v5.0 | a la mitad o menos |
|---|---|---|---|
| v5.0 | 6.23 | | |
| K = 16 | 3.40 | 0.545 | **no** |
| K = 64 | 3.43 | 0.551 | **no** |

- **La mitad son 3.11 lecturas por carga.**

### Lecturas por carga al final, en los tres estados del store

Por carga, sobre las queries de cada punto. Los puntos son los del gemelo de cada corrida. El modelo son 208 B por miembro y 4 B por arista de los postings que carga.

| estado del store | recall | índice | ef | cargas | segmentos del bucket de postings | lecturas por carga | bytes por carga | por el modelo | real entre modelo |
|---|---|---|---|---|---|---|---|---|---|
| como lo dejan los ciclos | 0.95 | v5.0, un valor por cambio | 21 | 19.63 | 3 | 4.81 | 92.45 KB | 63.45 KB | 1.457 |
| como lo dejan los ciclos | 0.95 | K = 16 | 20 | 19.04 | 3 | 2.34 | 54.19 KB | 63.37 KB | 0.855 |
| como lo dejan los ciclos | 0.95 | K = 64 | 21 | 19.62 | 3 | 2.36 | 54.18 KB | 62.75 KB | 0.863 |
| como lo dejan los ciclos | 0.99 | v5.0, un valor por cambio | 158 | 72.09 | 3 | 4.81 | 91.89 KB | 63.20 KB | 1.454 |
| como lo dejan los ciclos | 0.99 | K = 16 | 156 | 71.62 | 3 | 2.34 | 53.91 KB | 63.11 KB | 0.854 |
| como lo dejan los ciclos | 0.99 | K = 64 | 165 | 74.69 | 3 | 2.35 | 53.93 KB | 62.54 KB | 0.862 |
| volcado, sin compactar | 0.95 | v5.0, un valor por cambio | 21 | 19.63 | 4 | 6.59 | 94.34 KB | 63.45 KB | 1.487 |
| volcado, sin compactar | 0.95 | K = 16 | 20 | 19.04 | 4 | 4.26 | 67.02 KB | 63.37 KB | 1.058 |
| volcado, sin compactar | 0.95 | K = 64 | 21 | 19.62 | 4 | 4.13 | 66.37 KB | 62.75 KB | 1.058 |
| volcado, sin compactar | 0.99 | v5.0, un valor por cambio | 158 | 72.09 | 4 | 6.58 | 93.75 KB | 63.20 KB | 1.483 |
| volcado, sin compactar | 0.99 | K = 16 | 156 | 71.62 | 4 | 4.25 | 66.75 KB | 63.11 KB | 1.058 |
| volcado, sin compactar | 0.99 | K = 64 | 165 | 74.69 | 4 | 4.12 | 66.15 KB | 62.54 KB | 1.058 |
| compactado | 0.95 | v5.0, un valor por cambio | 21 | 19.63 | 2 | 3.76 | 70.14 KB | 63.45 KB | 1.105 |
| compactado | 0.95 | K = 16 | 20 | 19.04 | 1 | 2.00 | 66.98 KB | 63.37 KB | 1.057 |
| compactado | 0.95 | K = 64 | 21 | 19.62 | 2 | 2.76 | 66.35 KB | 62.75 KB | 1.057 |
| compactado | 0.99 | v5.0, un valor por cambio | 158 | 72.09 | 2 | 3.76 | 69.86 KB | 63.20 KB | 1.105 |
| compactado | 0.99 | K = 16 | 156 | 71.62 | 1 | 2.00 | 66.71 KB | 63.11 KB | 1.057 |
| compactado | 0.99 | K = 64 | 165 | 74.69 | 2 | 2.76 | 66.12 KB | 62.54 KB | 1.057 |

- **En el primer estado el store cuenta de menos:** lo que está en la memtable no pasa por él.
- **Las corridas no comparten el grafo de centroides,** que sale con otros niveles en cada una: sus puntos y sus cargas no son exactamente los mismos.
- **K = 16, compactado, a 0.95:** sobre el modelo, el id y el grado de cada miembro en su fila de aristas +2.0 %, el marco del Set en la fila de miembros +3.6 %, cabeceras y claves +0.1 %, y lo demás −0.0 %. En total +5.7 %.
- **K = 64, compactado, a 0.95:** sobre el modelo, el id y el grado de cada miembro en su fila de aristas +2.0 %, el marco del Set en la fila de miembros +3.6 %, cabeceras y claves +0.1 %, y lo demás +0.0 %. En total +5.7 %.

### Escrituras: lo que un insert entrega, y la amplificación

Por insert, en toda la corrida de validación, en bytes de valor. El vector exacto, 6.14 KB, va a su fichero y no se cuenta.

| índice | miembro: fila, mapa y código de ruteo | aristas propias | inversas | filas de aristas enteras | sus distancias | splits: filas de miembros y mapa | total al store |
|---|---|---|---|---|---|---|---|
| v5.0, un valor por cambio | 0.28 KB | 0.08 KB | 0.62 KB | 0.00 KB | 0.00 KB | 0.37 KB | **1.35 KB** |
| K = 16 | 0.28 KB | 0.00 KB | 0.00 KB | 3.84 KB | 0.22 KB | 0.32 KB | **4.66 KB** |
| K = 64 | 0.28 KB | 0.00 KB | 0.00 KB | 1.06 KB | 0.20 KB | 0.32 KB | **1.87 KB** |

| K | filas de aristas escritas | por K cambios | por splits | por checkpoints | por insert | bytes de una fila | filas leídas para reescribir | lápidas de fila |
|---|---|---|---|---|---|---|---|---|
| 16 | 314 606 | 295 999 | 7 348 | 11 259 | 0.353 | 10.87 KB | 307 258 | 3 674 |
| 64 | 89 151 | 69 711 | 7 398 | 12 042 | 0.100 | 10.62 KB | 81 753 | 3 699 |

- **Por checkpoints:** las filas que se escriben al volcar lo pendiente en cada uno, cinco veces en la corrida. La corrida cronometrada solo las escribe al cerrar.

**El store, al acabar, compactado para siempre:**

| índice | postings | filas de aristas | mapa | códigos de ruteo | distancias de aristas | total del store | lo que el store escribió | entre lo que creció | entre lo entregado |
|---|---|---|---|---|---|---|---|---|---|
| v5.0, un valor por cambio | 267.18 MB | — | 62.17 MB | 129.49 MB | 93.50 MB | 552.33 MB | 8 873.97 MB | 17.80x | 7.38x |
| K = 16 | 215.07 MB | 40.49 MB | 62.17 MB | 129.49 MB | 93.49 MB | 540.71 MB | 16 416.80 MB | 33.67x | 3.95x |
| K = 64 | 215.26 MB | 40.44 MB | 62.17 MB | 129.49 MB | 93.44 MB | 540.79 MB | 8 316.11 MB | 17.05x | 5.00x |

- **Lo que el store escribió** son su log, sus volcados y sus compactaciones, con la compactación final, sin lo de abrir la base.

### RAM de lo pendiente

Los miembros con cambios esperando y los bytes de sus listas y de sus distancias. No incluye lo que Go gasta en guardarlos.

| K | corrida | pico: miembros | bytes | en postings | bytes por posting con algo pendiente | por posting vivo |
|---|---|---|---|---|---|---|
| 16 | validación | 29 730 | 2.39 MB | 3 779 | 0.63 KB | 0.58 KB |
| 64 | validación | 115 105 | 9.14 MB | 4 022 | 2.27 KB | 2.22 KB |

- **En la validación** cada checkpoint lo vacía: su pico es el de un tramo. **En la cronometrada** solo se vacía al cerrar.

### Inserts por segundo

Sin cronometrar todavía.

### Latencia de búsqueda: frente al crecido de v5.0 y a la celda en lote

Sin cronometrar todavía.

### Lo que vale y lo que no

- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre índices, medidos igual y en la misma sesión.
- **Las lecturas por carga se comparan con la validación de v5.0,** que es otra corrida: el mismo protocolo, la misma secuencia de inserts y la misma compactación, con otro grafo de centroides.
- **Un escritor, y ninguna búsqueda durante los inserts.**
- **El índice validado y el cronometrado de cada K son dos corridas.**
- **Sin medir:** la caída con cambios pendientes y su reparación, el halo, el refresco, los deletes y Linux.
