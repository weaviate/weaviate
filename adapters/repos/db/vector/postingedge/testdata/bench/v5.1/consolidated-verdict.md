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
| 16 | cronometrada | 29 364 | 2.36 MB | 3 746 | 0.63 KB | 0.58 KB |
| 64 | validación | 115 105 | 9.14 MB | 4 022 | 2.27 KB | 2.22 KB |
| 64 | cronometrada | 113 817 | 8.96 MB | 3 979 | 2.25 KB | 2.20 KB |

- **En la validación** cada checkpoint lo vacía: su pico es el de un tramo. **En la cronometrada** solo se vacía al cerrar.

**En la corrida cronometrada, al acabar cada tramo:**

| K | | miembros esperando | bytes | en postings | heap en uso del proceso, tras recolectar |
|---|---|---|---|---|---|
| 16 | al 20 % | 5 943 | 0.44 MB | 761 | 6 423.20 MB |
| 16 | al 40 % | 11 911 | 0.91 MB | 1 515 | 6 524.55 MB |
| 16 | al 60 % | 17 702 | 1.37 MB | 2 294 | 6 580.49 MB |
| 16 | al 80 % | 23 021 | 1.81 MB | 2 963 | 6 674.70 MB |
| 16 | al 100 % | 29 169 | 2.32 MB | 3 767 | 6 750.49 MB |
| 64 | al 20 % | 23 245 | 1.69 MB | 813 | 6 439.99 MB |
| 64 | al 40 % | 46 340 | 3.51 MB | 1 615 | 6 541.21 MB |
| 64 | al 60 % | 70 159 | 5.38 MB | 2 405 | 6 613.43 MB |
| 64 | al 80 % | 90 831 | 7.06 MB | 3 192 | 6 724.34 MB |
| 64 | al 100 % | 113 674 | 8.95 MB | 3 993 | 6 783.33 MB |

- **El heap del proceso** tiene además los códigos de ruteo, el mapa, los centroides y su grafo.

### Inserts por segundo

Un escritor, del 10 % al 100 %, con el store leyendo por su mapping, sus ciclos dentro del tiempo y la compactación forzada. La de v5.0 es su corrida de la noche del 18 al 19: otra sesión.

| tramo | v5.0, un valor por cambio: inserts por segundo | K = 16: inserts por segundo | K = 64: inserts por segundo |
|---|---|---|---|
| del 10 al 20 % | 213 | 285 | 238 |
| del 20 al 40 % | 195 | 216 | 204 |
| del 40 al 60 % | 176 | 190 | 184 |
| del 60 al 80 % | 171 | 181 | 175 |
| del 80 al 100 % | 163 | 176 | 171 |
| **todo** | **179** | **197** | **187** |

- **v5.0, un valor por cambio:** un insert son 5.59 ms de media, repartidos así: la búsqueda de aristas 93.3 %, escribir el miembro 2.1 %, las aristas inversas 1.1 %, los splits 1.2 %, los ciclos del store 0.4 %, el grafo de centroides 1.9 %.
  - El store escribió 9 430.90 MB, 16.61 veces lo que ocupa al final (567.68 MB), con su compactación final.
  - Su punto de 0.95: ef 21, recall 0.9509.
  - Su punto de 0.99: ef 158, recall 0.9901.
- **K = 16:** un insert son 5.07 ms de media, repartidos así: la búsqueda de aristas 91.5 %, escribir el miembro 2.7 %, las aristas inversas 0.9 %, los splits 1.7 %, las filas de aristas 0.6 %, los ciclos del store 0.6 %, el grafo de centroides 2.0 %.
  - Al cerrar se escribieron las 3 767 filas que esperaban, en 0.2 s, que van en el tiempo.
  - El tramo: de 00:34 a 01:51, con la base y lo de después dentro. Teclado o trackpad durante el tramo: no.
  - Cobertura del índice que deja: completa.
  - El store escribió 14 462.39 MB, 26.01 veces lo que ocupa al final (555.95 MB), con su compactación final.
  - Su punto de 0.95: ef 21, recall 0.9508.
  - Su punto de 0.99: ef 166, recall 0.9900.
- **K = 64:** un insert son 5.34 ms de media, repartidos así: la búsqueda de aristas 92.2 %, escribir el miembro 2.5 %, las aristas inversas 0.9 %, los splits 1.9 %, las filas de aristas 0.3 %, los ciclos del store 0.3 %, el grafo de centroides 1.9 %.
  - Al cerrar se escribieron las 3 993 filas que esperaban, en 0.3 s, que van en el tiempo.
  - El tramo: de 01:51 a 03:12, con la base y lo de después dentro. Teclado o trackpad durante el tramo: no.
  - Cobertura del índice que deja: completa.
  - El store escribió 7 738.50 MB, 13.97 veces lo que ocupa al final (553.94 MB), con su compactación final.
  - Su punto de 0.95: ef 21, recall 0.9513.
  - Su punto de 0.99: ef 166, recall 0.9900.
- **Sin umbral.** Los inserts por segundo de v5.0 y de v5.1 no son de la misma sesión.

### Latencia de búsqueda: frente al crecido de v5.0 y a la celda en lote

Primera ronda. En caliente: la pasada de QPS mediano de las cronometradas. En frío: cada tanda, tras tirar la caché y reabrir el índice.

- **Cómo se tira la caché:** `ok=0; for try in 1 2 3 4 5; do diskutil unmount '/Volumes/v40bench' > /dev/null 2>&1 && ok=1 && break; sleep 2; done; [ $ok = 1 ] && diskutil mount 'disk3s7' > /dev/null`

**Recall 0.95, en caliente:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| K = 16 | ef 21 | 1.10 | 3.85 | 5.76 | 668 | 1.56 | 5.18 | 7.77 | 3 875 |
| K = 64 | ef 21 | 1.12 | 3.85 | 5.75 | 660 | 1.55 | 5.14 | 7.63 | 3 855 |
| crecido de v5.0 | ef 21 | 1.33 | 4.44 | 6.78 | 566 | 2.05 | 6.98 | 10.72 | 2 903 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 21 | 0.91 | 2.97 | 4.48 | 829 | 1.30 | 4.15 | 6.31 | 4 684 |

**Recall 0.99, en caliente:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| K = 16 | ef 166 | 3.82 | 12.77 | 18.57 | 199 | 5.21 | 17.59 | 26.16 | 1 166 |
| K = 64 | ef 166 | 3.82 | 12.92 | 18.59 | 198 | 5.13 | 17.33 | 25.60 | 1 180 |
| crecido de v5.0 | ef 158 | 4.53 | 15.31 | 22.22 | 168 | 7.09 | 23.18 | 33.90 | 875 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 187 | 3.20 | 10.74 | 15.50 | 236 | 4.24 | 14.49 | 20.88 | 1 422 |

**Recall 0.95, en frío:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| K = 16 | ef 21 | 5.48 | 13.65 | 22.25 | 152 | 18.58 | 35.57 | 49.45 | 336 |
| K = 64 | ef 21 | 5.25 | 13.93 | 21.11 | 155 | 18.79 | 36.66 | 49.24 | 334 |
| crecido de v5.0 | ef 21 | 4.86 | 11.22 | 18.64 | 172 | 17.19 | 33.16 | 47.84 | 360 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 21 | 3.72 | 8.32 | 13.48 | 230 | 12.00 | 23.32 | 31.88 | 524 |

**Recall 0.99, en frío:**

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| K = 16 | ef 166 | 12.15 | 37.60 | 56.62 | 64 | 33.40 | 76.57 | 91.27 | 177 |
| K = 64 | ef 166 | 12.15 | 38.13 | 53.46 | 64 | 32.94 | 78.33 | 92.87 | 175 |
| crecido de v5.0 | ef 158 | 11.20 | 35.64 | 47.87 | 69 | 31.12 | 75.68 | 90.08 | 185 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 187 | 9.07 | 27.46 | 40.12 | 86 | 23.06 | 56.77 | 70.93 | 239 |

**Cada índice entre la celda en lote,** por el p50:

| índice | estado | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |
|---|---|---|---|---|---|
| K = 16 | en caliente | 1.21x | 1.20x | 1.19x | 1.23x |
| K = 16 | en frío | 1.47x | 1.55x | 1.34x | 1.45x |
| K = 64 | en caliente | 1.22x | 1.19x | 1.19x | 1.21x |
| K = 64 | en frío | 1.41x | 1.57x | 1.34x | 1.43x |
| crecido de v5.0 | en caliente | 1.45x | 1.58x | 1.42x | 1.67x |
| crecido de v5.0 | en frío | 1.31x | 1.43x | 1.24x | 1.35x |

**Cada índice entre el crecido de v5.0,** por el p50:

| índice | estado | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |
|---|---|---|---|---|---|
| K = 16 | en caliente | 0.83x | 0.76x | 0.84x | 0.74x |
| K = 16 | en frío | 1.13x | 1.08x | 1.08x | 1.07x |
| K = 64 | en caliente | 0.84x | 0.75x | 0.84x | 0.72x |
| K = 64 | en frío | 1.08x | 1.09x | 1.08x | 1.06x |
| escrito en lote (v4.0, 512 bits en RAM) | en caliente | 0.69x | 0.63x | 0.71x | 0.60x |
| escrito en lote (v4.0, 512 bits en RAM) | en frío | 0.76x | 0.70x | 0.81x | 0.74x |

**Lo que cuesta traer cada fila (enmienda 1).** Por carga, en microsegundos, en la primera ronda: cada lectura de fila desde que se pide al store hasta que queda decodificada en el posting. En el índice que crece, la de aristas lleva además el juntar sus listas por miembro.

*En caliente, 1 thread:*

| recall | índice | cargas | traer los bloques, de pared, por carga | fila de miembros | entre la del lote | fila de aristas | entre la del lote | las dos filas | entre las del lote |
|---|---|---|---|---|---|---|---|---|---|
| 0.95 | K = 16 | 19.52 | 22.0 | 18.0 | 1.26x | 9.7 | 1.37x | 27.7 | 1.30x |
| 0.95 | K = 64 | 19.56 | 22.8 | 18.1 | 1.27x | 9.5 | 1.34x | 27.6 | 1.30x |
| 0.95 | crecido de v5.0 | 19.54 | 34.8 | 18.7 | 1.31x | 39.2 | 5.54x | 57.9 | 2.72x |
| 0.95 | escrito en lote (v4.0, 512 bits en RAM) | 18.59 | 16.3 | 14.2 |  | 7.1 |  | 21.3 |  |
| 0.99 | K = 16 | 74.11 | 21.8 | 17.4 | 1.25x | 9.1 | 1.36x | 26.5 | 1.28x |
| 0.99 | K = 64 | 73.93 | 22.0 | 17.7 | 1.26x | 9.2 | 1.37x | 26.8 | 1.30x |
| 0.99 | crecido de v5.0 | 71.91 | 33.6 | 18.8 | 1.34x | 39.4 | 5.89x | 58.2 | 2.81x |
| 0.99 | escrito en lote (v4.0, 512 bits en RAM) | 76.30 | 15.7 | 14.0 |  | 6.7 |  | 20.7 |  |

*En caliente, 8 threads:*

| recall | índice | cargas | traer los bloques, de pared, por carga | fila de miembros | entre la del lote | fila de aristas | entre la del lote | las dos filas | entre las del lote |
|---|---|---|---|---|---|---|---|---|---|
| 0.95 | K = 16 | 19.52 | 35.8 | 27.2 | 1.43x | 11.8 | 1.36x | 39.0 | 1.41x |
| 0.95 | K = 64 | 19.56 | 36.6 | 27.6 | 1.45x | 12.1 | 1.40x | 39.7 | 1.44x |
| 0.95 | crecido de v5.0 | 19.54 | 66.3 | 28.1 | 1.48x | 51.3 | 5.93x | 79.5 | 2.87x |
| 0.95 | escrito en lote (v4.0, 512 bits en RAM) | 18.59 | 25.2 | 19.0 |  | 8.7 |  | 27.7 |  |
| 0.99 | K = 16 | 74.11 | 35.4 | 28.0 | 1.38x | 12.5 | 1.39x | 40.5 | 1.39x |
| 0.99 | K = 64 | 73.93 | 34.7 | 28.0 | 1.39x | 12.2 | 1.35x | 40.2 | 1.37x |
| 0.99 | crecido de v5.0 | 71.91 | 65.9 | 28.9 | 1.43x | 52.2 | 5.80x | 81.2 | 2.78x |
| 0.99 | escrito en lote (v4.0, 512 bits en RAM) | 76.30 | 24.7 | 20.2 |  | 9.0 |  | 29.2 |  |

*En frío, 1 thread:*

| recall | índice | cargas | traer los bloques, de pared, por carga | fila de miembros | entre la del lote | fila de aristas | entre la del lote | las dos filas | entre las del lote |
|---|---|---|---|---|---|---|---|---|---|
| 0.95 | K = 16 | 18.71 | 182.6 | 443.4 | 1.70x | 198.9 | 0.92x | 642.3 | 1.35x |
| 0.95 | K = 64 | 18.77 | 183.0 | 444.2 | 1.71x | 198.9 | 0.92x | 643.1 | 1.35x |
| 0.95 | crecido de v5.0 | 18.58 | 160.7 | 372.3 | 1.43x | 391.4 | 1.80x | 763.7 | 1.60x |
| 0.95 | escrito en lote (v4.0, 512 bits en RAM) | 17.83 | 109.4 | 260.2 |  | 217.2 |  | 477.4 |  |
| 0.99 | K = 16 | 73.70 | 140.6 | 333.3 | 1.52x | 136.6 | 0.78x | 469.9 | 1.19x |
| 0.99 | K = 64 | 73.49 | 141.0 | 335.8 | 1.53x | 137.5 | 0.78x | 473.3 | 1.20x |
| 0.99 | crecido de v5.0 | 71.31 | 130.8 | 296.5 | 1.35x | 300.8 | 1.71x | 597.3 | 1.51x |
| 0.99 | escrito en lote (v4.0, 512 bits en RAM) | 75.86 | 93.1 | 219.6 |  | 176.0 |  | 395.6 |  |

- **No son partes del tiempo de pared:** las filas de una ronda se piden a la vez. Lo que la ronda espera lo marca su fila más lenta.
- **Con 8 threads las lecturas compiten por CPU** y sus tiempos se inflan: la lectura es con 1 thread.
- **En frío** el tiempo de una fila es sobre todo esperar al disco.

**Segunda mitad del criterio: la latencia en frío a 0.95, a 1.20 veces o menos del lote.**

| índice | p50 en frío, 1 thread, a 0.95, ms | entre el lote | a 1.20 o menos | de su media en frío: bloques | scan y ruteo | rescore |
|---|---|---|---|---|---|---|
| K = 16 | 5.48 | 1.47x | **no** | 3.42 | 0.77 | 2.27 |
| K = 64 | 5.25 | 1.41x | **no** | 3.43 | 0.77 | 2.08 |
| crecido de v5.0 | 4.86 | 1.31x | no | 2.99 | 0.79 | 1.88 |
| escrito en lote (v4.0, 512 bits en RAM) | 3.72 |  |  | 1.95 | 0.64 | 1.62 |

- **Los puntos no son el mismo ef:** cada índice va en el suyo, el menor que llega al recall.
- **Por debajo de un 10 % entre dos índices, es empate.**

### Veredicto del pre-registro

| K | lecturas por carga a la mitad o menos | latencia en frío a 1.20 o menos | sirve |
|---|---|---|---|
| 16 | no | no | **no** |
| 64 | no | no | **no** |

**No sirve, tal como se pre-registró:** ninguna K cumple las dos cosas.

### Las curvas: dos rondas, deriva y actividad

| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |
|---|---|---|---|---|---|
| K = 16 | 03:12 a 03:18 | 03:37 a 03:43 | no | −3.6 % (0.95, 1 thread, en frío: 5.48 y 5.28 ms) | — |
| K = 64 | 03:18 a 03:25 | 03:43 a 03:50 | no | +8.3 % (0.95, 8 threads, en caliente: 1.55 y 1.68 ms) | — |
| crecido de v5.0 | 03:25 a 03:31 | 03:50 a 03:57 | no | +6.2 % (0.99, 8 threads, en frío: 31.12 y 33.04 ms) | — |
| escrito en lote (v4.0, 512 bits en RAM) | 03:31 a 03:37 | 03:57 a 04:02 | no | −1.8 % (0.95, 1 thread, en frío: 3.72 y 3.65 ms) | — |

- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.
- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.

### RAM residente al buscar

| índice | heap en uso tras abrir | huella del proceso |
|---|---|---|
| K = 16 | 175.73 MB | 371.31 MB |
| K = 64 | 175.09 MB | 370.69 MB |
| crecido de v5.0 | 175.48 MB | 371.25 MB |
| escrito en lote (v4.0, 512 bits en RAM) | 145.71 MB | 288.16 MB |

- **Tras abrir no espera nada:** lo pendiente solo existe mientras se insertan vectores.

### Lo que vale y lo que no

- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre índices, medidos igual y en la misma sesión.
- **Las lecturas por carga se comparan con la validación de v5.0,** que es otra corrida: el mismo protocolo, la misma secuencia de inserts y la misma compactación, con otro grafo de centroides.
- **Un escritor, y ninguna búsqueda durante los inserts.**
- **El índice validado y el cronometrado de cada K son dos corridas.**
- **Sin medir:** la caída con cambios pendientes y su reparación, el halo, el refresco, los deletes y Linux.
