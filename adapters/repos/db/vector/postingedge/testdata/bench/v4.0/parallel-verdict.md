### Estado

- **Ronda 1:** hfresh-best95, hfresh-best95-par, hfresh-best99, hfresh-best99-par, r512, halo512, de 2026-11-19 07:10 a 07:47.
- **Ronda 2:** hfresh-best95, hfresh-best95-par, hfresh-best99, hfresh-best99-par, r512, halo512, de 2026-11-19 07:47 a 08:24.
- **En paralelo:** 8 lecturas de posting en vuelo. En serie, una.
- **Cómo se tira la caché:** `ok=0; for try in 1 2 3 4 5; do diskutil unmount '/Volumes/v40bench' > /dev/null 2>&1 && ok=1 && break; sleep 2; done; [ $ok = 1 ] && diskutil mount 'disk3s7' > /dev/null`

### Recall 0.95, en caliente

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS | de la latencia media con 1 thread, leer postings o bloques, ms |
|---|---|---|---|---|---|---|---|---|---|---|
| HFresh, la mejor a 0.95, en serie | searchProbe 185, rescoreLimit 100 | 3.38 | 4.35 | 4.95 | 290 | 8.97 | 12.17 | 13.43 | 882 | 1.15 |
| HFresh, la mejor a 0.95, en paralelo | searchProbe 185, rescoreLimit 100 | 2.85 | 3.73 | 4.50 | 342 | 8.73 | 12.73 | 14.74 | 886 | 0.51 |
| 512 bits en RAM | ef 21 | 0.89 | 2.88 | 4.38 | 848 | 1.27 | 4.01 | 5.85 | 4 821 | 0.28 |
| halo de 512 bits | ef 21 | 1.27 | 4.83 | 7.21 | 559 | 2.06 | 7.75 | 11.55 | 2 772 | 0.51 |

### Recall 0.95, en frío

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS | de la latencia media con 1 thread, leer postings o bloques, ms |
|---|---|---|---|---|---|---|---|---|---|---|
| HFresh, la mejor a 0.95, en serie | searchProbe 185, rescoreLimit 100 | 46.80 | 56.13 | 58.52 | 22 | 77.84 | 97.26 | 99.52 | 95 | 40.48 |
| HFresh, la mejor a 0.95, en paralelo | searchProbe 185, rescoreLimit 100 | 14.28 | 17.13 | 18.94 | 69 | 73.63 | 109.7 | 135.3 | 101 | 8.27 |
| 512 bits en RAM | ef 21 | 3.54 | 8.21 | 13.27 | 236 | 11.43 | 23.00 | 30.01 | 538 | 1.87 |
| halo de 512 bits | ef 21 | 4.80 | 14.29 | 23.13 | 160 | 16.03 | 35.30 | 49.55 | 357 | 3.53 |

### Recall 0.99, en caliente

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS | de la latencia media con 1 thread, leer postings o bloques, ms |
|---|---|---|---|---|---|---|---|---|---|---|
| HFresh, la mejor a 0.99, en serie | searchProbe 928, rescoreLimit 350 | 16.06 | 18.13 | 18.92 | 62 | 42.14 | 56.77 | 70.26 | 186 | 5.97 |
| HFresh, la mejor a 0.99, en paralelo | searchProbe 928, rescoreLimit 350 | 12.73 | 15.18 | 16.14 | 77 | 41.00 | 54.15 | 61.19 | 193 | 2.51 |
| 512 bits en RAM | ef 187 | 3.16 | 10.57 | 15.41 | 238 | 4.16 | 14.25 | 20.26 | 1 443 | 1.14 |
| halo de 512 bits | ef 187 | 4.86 | 17.51 | 25.30 | 151 | 7.93 | 28.09 | 40.73 | 752 | 2.04 |

### Recall 0.99, en frío

| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS | de la latencia media con 1 thread, leer postings o bloques, ms |
|---|---|---|---|---|---|---|---|---|---|---|
| HFresh, la mejor a 0.99, en serie | searchProbe 928, rescoreLimit 350 | 178.4 | 249.6 | 257.6 | 6 | 242.7 | 346.6 | 357.3 | 31 | 153.3 |
| HFresh, la mejor a 0.99, en paralelo | searchProbe 928, rescoreLimit 350 | 47.89 | 66.22 | 70.49 | 20 | 228.5 | 346.2 | 370.3 | 32 | 28.37 |
| 512 bits en RAM | ef 187 | 8.89 | 27.71 | 39.23 | 87 | 23.55 | 57.58 | 71.93 | 234 | 6.96 |
| halo de 512 bits | ef 187 | 14.02 | 49.25 | 74.84 | 52 | 39.90 | 107.6 | 133.1 | 133 | 13.31 |

### En paralelo entre en serie

El p50 de HFresh con sus postings leídos en paralelo entre el suyo leyéndolos en serie, en la misma sesión.

| | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |
|---|---|---|---|---|
| en caliente | 0.84x (2.85 y 3.38 ms) | 0.97x (8.73 y 8.97 ms) | 0.79x (12.73 y 16.06 ms) | 0.97x (41.00 y 42.14 ms) |
| en frío | 0.31x (14.28 y 46.80 ms) | 0.95x (73.63 y 77.84 ms) | 0.27x (47.89 y 178.4 ms) | 0.94x (228.5 y 242.7 ms) |

### Posting-edge entre HFresh

El p50 de la celda entre el de HFresh, en serie y en paralelo. Menos de un 10 % de diferencia es un empate.

| celda | estado | HFresh | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |
|---|---|---|---|---|---|---|
| 512 bits en RAM | en caliente | en serie | 0.26x, menos | 0.14x, menos | 0.20x, menos | 0.10x, menos |
| 512 bits en RAM | en caliente | en paralelo | 0.31x, menos | 0.15x, menos | 0.25x, menos | 0.10x, menos |
| 512 bits en RAM | en frío | en serie | 0.08x, menos | 0.15x, menos | 0.05x, menos | 0.10x, menos |
| 512 bits en RAM | en frío | en paralelo | 0.25x, menos | 0.16x, menos | 0.19x, menos | 0.10x, menos |
| halo de 512 bits | en caliente | en serie | 0.38x, menos | 0.23x, menos | 0.30x, menos | 0.19x, menos |
| halo de 512 bits | en caliente | en paralelo | 0.44x, menos | 0.24x, menos | 0.38x, menos | 0.19x, menos |
| halo de 512 bits | en frío | en serie | 0.10x, menos | 0.21x, menos | 0.08x, menos | 0.16x, menos |
| halo de 512 bits | en frío | en paralelo | 0.34x, menos | 0.22x, menos | 0.29x, menos | 0.17x, menos |

### Los criterios 2 y 3 de v4.0, leídos contra HFresh en paralelo

A título informativo: los criterios pre-registrados eran contra el HFresh de main, que lee en serie, y su veredicto no se toca.

- **HFresh, la mejor a 0.95, en frío con 1 thread:** p50 de 14.28 ms en paralelo, 46.80 en serie. Lo esperado antes de medir eran unos 12 ms.
- **Criterio 2, la celda de 512 bits en RAM: se cumpliría.** p50 de 3.54 ms frente a 14.28 de HFresh en paralelo (0.25x): menos latencia.
- **Criterio 3, el halo de 512 bits: se cumpliría.** p50 de 4.80 ms frente a 14.28 de HFresh en paralelo (0.34x): menos latencia.

### Las curvas: dos rondas, deriva y actividad

| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |
|---|---|---|---|---|---|
| HFresh, la mejor a 0.95, en serie | 07:10 a 07:14 | 07:47 a 07:51 | no | −0.8 % (recall 0.9500, 8 threads, en caliente: 8.97 y 8.90 ms) | — |
| HFresh, la mejor a 0.95, en paralelo | 07:14 a 07:17 | 07:51 a 07:54 | sí, en la ronda 1 | +1.2 % (recall 0.9500, 1 thread, en caliente: 2.85 y 2.89 ms) | **actividad** |
| HFresh, la mejor a 0.99, en serie | 07:17 a 07:27 | 07:54 a 08:05 | sí, en la ronda 1 | +1.9 % (recall 0.9900, 1 thread, en frío: 178.4 y 181.8 ms) | **actividad** |
| HFresh, la mejor a 0.99, en paralelo | 07:27 a 07:36 | 08:05 a 08:13 | no | +0.9 % (recall 0.9900, 1 thread, en frío: 47.89 y 48.31 ms) | — |
| 512 bits en RAM | 07:36 a 07:41 | 08:13 a 08:18 | no | +3.8 % (0.95, 1 thread, en frío: 3.54 y 3.68 ms) | — |
| halo de 512 bits | 07:41 a 07:47 | 08:18 a 08:24 | no | −1.6 % (0.99, 1 thread, en frío: 14.02 y 13.80 ms) | — |

- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.
- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.

### Lo que vale y lo que no

- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre curvas de la misma sesión.
- **HFresh en paralelo no es el HFresh de main:** es su búsqueda con una sola cosa cambiada, cuántos postings pide a la vez.
- **Un solo grado de paralelismo,** y sin medir en Linux.
