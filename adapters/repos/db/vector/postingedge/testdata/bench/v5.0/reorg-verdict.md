### Las dos celdas: corrección y lecturas, sin cronometrar

| índice | frente al crecido tal cual, 5 000 queries por punto | segmentos del bucket de postings | bucket de postings | fichero de vectores |
|---|---|---|---|---|
| crecido, tal cual | — | 2 | 282.78 MB | 6082.56 MB |
| celda 1: un segmento | los mismos ids, distancias y trabajo | 1 | 263.22 MB | 6082.56 MB |
| celda 2: un segmento y vectores en orden de posting | los mismos ids, distancias y trabajo | 1 | 263.22 MB | 6082.56 MB |

- **Escribir la celda 1** llevó 6 s.
- **Escribir la celda 2** llevó 40 s.

**Lo que el store lee por carga,** en modo pread:

| índice | recall | ef | cargas | lecturas por carga | bytes por carga | por el modelo | real entre modelo |
|---|---|---|---|---|---|---|---|
| crecido, tal cual | 0.95 | 21 | 19.54 | 3.90 | 71.89 KB | 63.15 KB | 1.138 |
| crecido, tal cual | 0.99 | 158 | 71.91 | 3.90 | 71.60 KB | 62.91 KB | 1.138 |
| celda 1: un segmento | 0.95 | 21 | 19.54 | 2.00 | 68.79 KB | 63.15 KB | 1.089 |
| celda 1: un segmento | 0.99 | 158 | 71.91 | 2.00 | 68.53 KB | 62.91 KB | 1.089 |
| celda 2: un segmento y vectores en orden de posting | 0.95 | 21 | 19.54 | 2.00 | 68.79 KB | 63.15 KB | 1.089 |
| celda 2: un segmento y vectores en orden de posting | 0.99 | 158 | 71.91 | 2.00 | 68.53 KB | 62.91 KB | 1.089 |

- **El criterio 1 con el bucket en un segmento, a 1M:** 1.089 veces el modelo a 0.95, con el límite en 1.10. El formato solo, por su aritmética, son 1.089.

### Recall 0.95, en caliente

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| crecido, tal cual | ef 21 | 1.32 | 4.46 | 6.84 | 566 | 2.05 | 6.95 | 10.73 | 2 901 |
| celda 1: un segmento | ef 21 | 1.23 | 4.14 | 6.35 | 610 | 1.89 | 6.31 | 10.09 | 3 142 |
| celda 2: un segmento y vectores en orden de posting | ef 21 | 1.22 | 4.13 | 6.28 | 610 | 1.89 | 6.27 | 9.53 | 3 178 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 21 | 0.91 | 2.95 | 4.41 | 830 | 1.28 | 4.03 | 6.00 | 4 790 |

### Recall 0.99, en caliente

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| crecido, tal cual | ef 158 | 4.53 | 15.23 | 22.21 | 168 | 7.08 | 23.44 | 33.67 | 871 |
| celda 1: un segmento | ef 158 | 4.19 | 13.97 | 20.43 | 182 | 6.42 | 21.08 | 30.86 | 956 |
| celda 2: un segmento y vectores en orden de posting | ef 158 | 4.15 | 13.90 | 20.58 | 183 | 6.41 | 21.08 | 30.59 | 960 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 187 | 3.19 | 10.70 | 15.66 | 237 | 4.20 | 14.28 | 20.98 | 1 430 |

### Recall 0.95, en frío

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| crecido, tal cual | ef 21 | 4.96 | 11.51 | 19.13 | 169 | 17.86 | 33.13 | 47.04 | 347 |
| celda 1: un segmento | ef 21 | 4.45 | 10.12 | 15.77 | 191 | 13.84 | 28.88 | 40.42 | 433 |
| celda 2: un segmento y vectores en orden de posting | ef 21 | 4.00 | 9.39 | 15.19 | 209 | 11.96 | 25.02 | 35.81 | 494 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 21 | 3.73 | 8.39 | 13.75 | 226 | 11.94 | 23.82 | 31.12 | 523 |

### Recall 0.99, en frío

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| crecido, tal cual | ef 158 | 11.17 | 35.39 | 50.83 | 69 | 31.10 | 75.65 | 88.99 | 183 |
| celda 1: un segmento | ef 158 | 10.18 | 31.38 | 44.60 | 77 | 27.98 | 68.36 | 83.53 | 201 |
| celda 2: un segmento y vectores en orden de posting | ef 158 | 9.66 | 30.34 | 43.10 | 81 | 25.59 | 62.38 | 75.74 | 221 |
| escrito en lote (v4.0, 512 bits en RAM) | ef 187 | 9.01 | 27.80 | 39.59 | 86 | 23.30 | 56.95 | 71.16 | 238 |

### Cada índice entre el escrito en lote

Por el p50.

| índice | estado | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |
|---|---|---|---|---|---|
| crecido, tal cual | en caliente | 1.45x | 1.60x | 1.42x | 1.69x |
| crecido, tal cual | en frío | 1.33x | 1.50x | 1.24x | 1.33x |
| celda 1: un segmento | en caliente | 1.35x | 1.48x | 1.32x | 1.53x |
| celda 1: un segmento | en frío | 1.19x | 1.16x | 1.13x | 1.20x |
| celda 2: un segmento y vectores en orden de posting | en caliente | 1.34x | 1.48x | 1.30x | 1.53x |
| celda 2: un segmento y vectores en orden de posting | en frío | 1.07x | 1.00x | 1.07x | 1.10x |

### La lectura del usuario

**La celda 2 queda a 1.07 veces del lote, en frío a 0.95: dentro del 1.20.** Lo que falta es reorganización en segundo plano, y el formato vale. p50 de 4.00 ms frente a 3.73.

**Lo que queda entre cada índice y el lote, por fase,** en la latencia media a 0.95 con 1 thread, en ms:

| índice | estado | total | traer los bloques | scan y ruteo | rescore |
|---|---|---|---|---|---|
| escrito en lote (v4.0, 512 bits en RAM) | en frío | 4.42 | 2.01 | 0.64 | 1.63 |
| crecido, tal cual, de más | en frío | +1.50 | +1.00 | +0.14 | +0.35 |
| celda 1: un segmento, de más | en frío | +0.82 | +0.30 | +0.15 | +0.36 |
| celda 2: un segmento y vectores en orden de posting, de más | en frío | +0.35 | +0.27 | +0.14 | −0.06 |
| escrito en lote (v4.0, 512 bits en RAM) | en caliente | 1.20 | 0.30 | 0.66 | 0.14 |
| crecido, tal cual, de más | en caliente | +0.56 | +0.37 | +0.17 | +0.00 |
| celda 1: un segmento, de más | en caliente | +0.44 | +0.26 | +0.15 | +0.00 |
| celda 2: un segmento y vectores en orden de posting, de más | en caliente | +0.43 | +0.26 | +0.15 | +0.00 |

**Lo que recupera cada celda,** de la distancia entre el crecido tal cual y el lote, a 0.95 con 1 thread:

| celda | estado | del p50 | de traer los bloques | del rescore |
|---|---|---|---|---|
| celda 1: un segmento | en frío | 42 % | 70 % | −4 % |
| celda 1: un segmento | en caliente | 24 % | 29 % | sin distancia que cerrar |
| celda 2: un segmento y vectores en orden de posting | en frío | 78 % | 73 % | 118 % |
| celda 2: un segmento y vectores en orden de posting | en caliente | 24 % | 29 % | sin distancia que cerrar |

- **La celda 1 sola,** en frío: p50 de 4.45 ms donde el crecido tal cual tarda 4.96 y el lote 3.73: 1.19 veces el lote.

### Las curvas: dos rondas, deriva y actividad

| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |
|---|---|---|---|---|---|
| crecido, tal cual | 08:32 a 08:39 | 08:57 a 09:04 | no | +1.8 % (0.99, 1 thread, en frío: 11.17 y 11.37 ms) | — |
| celda 1: un segmento | 08:39 a 08:46 | 09:04 a 09:11 | no | +10.3 % (0.95, 8 threads, en frío: 13.84 y 15.27 ms) | **deriva** |
| celda 2: un segmento y vectores en orden de posting | 08:46 a 08:52 | 09:11 a 09:18 | no | +4.1 % (0.95, 8 threads, en frío: 11.96 y 12.45 ms) | — |
| escrito en lote (v4.0, 512 bits en RAM) | 08:52 a 08:57 | 09:18 a 09:23 | no | −1.9 % (0.95, 1 thread, en frío: 3.73 y 3.65 ms) | — |

- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.
- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.

### Lo que vale y lo que no

- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre curvas de la misma sesión.
- **Las celdas son el mismo índice con sus bytes en otro sitio:** no miden una reorganización en segundo plano, sino lo que dejaría.
- **El formato de las filas no cambia en ninguna celda:** el marco por valor del Set sigue ahí.
