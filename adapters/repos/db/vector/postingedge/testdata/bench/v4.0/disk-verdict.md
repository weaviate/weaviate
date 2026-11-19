### Estado

- **Dataset:** dbpedia-openai-1000k-angular.bin, 990000 vectores de 1536 dimensiones, 5000 queries, k = 10.
- **Celdas de posting-edge:** código completo en RAM, 512 bits en RAM, 384 bits en RAM, halo de 512 bits, halo de 512 bits, podado.
- **Cronometradas en la ronda 1:** full, r512, r384, halo512, halo512p, hfresh-best95, hfresh-best99, hfresh-default, de 2026-11-18 10:50 a 11:34.
- **Cronometradas en la ronda 2:** full, r512, r384, halo512, halo512p, hfresh-best95, hfresh-best99, hfresh-default, de 2026-11-18 11:34 a 12:21.
- **Cronometradas en la repetición de noche:** full, r512, halo512, halo512p, hfresh-default, de 2026-11-18 23:00 a 23:27.
- **HFresh:** construido, cronometrado.
- **Acceso del store:** mmap. **Presupuesto de concurrencia por query:** 8. **GOMAXPROCS:** 10.
- **Cómo se tira la caché antes de cada tanda en frío:** `ok=0; for try in 1 2 3 4 5; do diskutil unmount '/Volumes/v40bench' > /dev/null 2>&1 && ok=1 && break; sleep 2; done; [ $ok = 1 ] && diskutil mount 'disk3s7' > /dev/null`

### Corrección: cada celda en disco frente a su equivalente en memoria

| celda | recall objetivo | ef | recall | queries comparadas | resultado |
|---|---|---|---|---|---|
| código completo en RAM | 0.95 | 20 | 0.9506 | 5000 | los mismos ids, distancias y trabajo |
| código completo en RAM | 0.99 | 284 | 0.9901 | 5000 | los mismos ids, distancias y trabajo |
| 512 bits en RAM | 0.95 | 21 | 0.9513 | 5000 | los mismos ids, distancias y trabajo |
| 512 bits en RAM | 0.99 | 187 | 0.9900 | 5000 | los mismos ids, distancias y trabajo |
| 384 bits en RAM | 0.95 | 21 | 0.9500 | 5000 | los mismos ids, distancias y trabajo |
| 384 bits en RAM | 0.99 | 164 | 0.9901 | 5000 | los mismos ids, distancias y trabajo |
| halo de 512 bits | 0.95 | 21 | 0.9513 | 5000 | los mismos ids, distancias y trabajo |
| halo de 512 bits | 0.99 | 187 | 0.9900 | 5000 | los mismos ids, distancias y trabajo |
| halo de 512 bits, podado | 0.95 | 25 | 0.9503 | 5000 | los mismos ids, distancias y trabajo |
| halo de 512 bits, podado | 0.99 | 239 | 0.9900 | 5000 | los mismos ids, distancias y trabajo |

La escritura para si alguna query difiere: lo que hay en el manifiesto pasó.

### Criterio 1: lecturas reales frente al modelo de v3.0

Por query, del store en modo pread, que es donde cuenta. El rescore, aparte.

| celda | recall | cargas | rondas | lecturas del store | bytes del store | modelo de v3.0 | real entre modelo | miembros | aristas | halo | rescore: lecturas | bytes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| código completo en RAM | 0.95 | 10.14 | 2.29 | 20.3 | 0.64 MB | 0.64 MB | 1.005 | 0.54 MB | 0.10 MB | — | 64 | 0.39 MB |
| código completo en RAM | 0.99 | 52.61 | 12.91 | 105.2 | 3.27 MB | 3.26 MB | 1.005 | 2.74 MB | 0.53 MB | — | 64 | 0.39 MB |
| 512 bits en RAM | 0.95 | 18.59 | 4.40 | 37.2 | 1.16 MB | 1.16 MB | 1.005 | 0.97 MB | 0.19 MB | — | 64 | 0.39 MB |
| 512 bits en RAM | 0.99 | 76.30 | 18.83 | 152.6 | 4.73 MB | 4.71 MB | 1.005 | 3.96 MB | 0.77 MB | — | 64 | 0.39 MB |
| 384 bits en RAM | 0.95 | 23.68 | 5.67 | 47.4 | 1.48 MB | 1.47 MB | 1.005 | 1.24 MB | 0.24 MB | — | 64 | 0.39 MB |
| 384 bits en RAM | 0.99 | 91.10 | 22.53 | 182.2 | 5.64 MB | 5.62 MB | 1.005 | 4.73 MB | 0.91 MB | — | 64 | 0.39 MB |
| halo de 512 bits | 0.95 | 18.59 | 4.40 | 55.8 | 3.40 MB | 3.39 MB | 1.002 | 0.97 MB | 0.19 MB | 2.24 MB | 64 | 0.39 MB |
| halo de 512 bits | 0.99 | 76.30 | 18.83 | 228.9 | 13.86 MB | 13.84 MB | 1.002 | 3.96 MB | 0.77 MB | 9.13 MB | 64 | 0.39 MB |
| halo de 512 bits, podado | 0.95 | 20.22 | 4.81 | 60.7 | 2.74 MB | 2.74 MB | 1.002 | 1.06 MB | 0.12 MB | 1.57 MB | 64 | 0.39 MB |
| halo de 512 bits, podado | 0.99 | 87.18 | 21.55 | 261.5 | 11.70 MB | 11.67 MB | 1.002 | 4.51 MB | 0.50 MB | 6.69 MB | 64 | 0.39 MB |

**Se cumple:** la celda que más se separa lee 1.005 veces su modelo, y el margen es de 0.75 a 1.25.

### HFresh real

- **Configuración por defecto:** réplicas 4, posting máximo de 48 KB (227 entradas, mínimo 75), searchProbe 256, rescoreLimit 350, distancia cosine.
- **Códigos:** RQ de 1 bit. Una entrada de posting son 209 B: id, versión y código.
- **Rescore:** los mejores candidatos, 350 por defecto, con su vector exacto del bucket `objects`.
- **El build:** 28466 postings y 5194013 entradas, 5.25 por vector. Inserts en 115 s, mantenimiento en 82 s, 422 compactaciones.
- **Segmentos por bucket:** hfresh_postings_hfresh 2, hfresh_shared_hfresh 1, objects 34, vectors_compressed 2.
- **Con su configuración por defecto da un recall de 0.9674.**

**Criterio 4: hasta qué recall llega.** Recall por searchProbe, con cada rescoreLimit; el de por defecto es 350:

| searchProbe | rescoreLimit 64 | rescoreLimit 100 | rescoreLimit 200 | rescoreLimit 350 | queries |
|---|---|---|---|---|---|
| 16 | 0.8322 | 0.8384 | 0.8408 | 0.8410 | 5000 |
| 32 | 0.8725 | 0.8804 | 0.8843 | 0.8847 | 5000 |
| 64 | 0.8996 | 0.9091 | 0.9140 | 0.9148 | 5000 |
| 128 | 0.9275 | 0.9383 | 0.9444 | 0.9456 | 5000 |
| 256 (por defecto) | 0.9471 | 0.9588 | 0.9659 | 0.9674 | 5000 |
| 512 | 0.9606 | 0.9731 | 0.9807 | 0.9824 | 5000 |
| 1024 | 0.9683 | 0.9814 | 0.9893 | 0.9911 | 5000 |
| 2048 | 0.9729 | 0.9863 | 0.9945 | 0.9963 | 5000 |
| 4096 | 0.9748 | 0.9881 | — | 0.9983 | 5000 |
| 8192 | 0.9750 | 0.9890 | — | 0.9988 | 1000 |
| 16384 | 0.9750 | 0.9889 | — | 0.9988 | 1000 |
| 28466 (todos los postings) | 0.9751 | 0.9890 | — | 0.9989 | 1000 |

- **Recall máximo con el rescoreLimit por defecto: 0.9989.**
- Un hueco es un searchProbe que no hizo falta medir: el barrido de un rescoreLimit para cuando llega a 0.99.

**Las candidatas: el menor searchProbe que llega a cada recall, por rescoreLimit.** Se elige la que menos lecturas hace.

| recall | rescoreLimit | searchProbe | recall medido | lecturas por query | postings: lecturas | bytes | rescore: lecturas | bytes | |
|---|---|---|---|---|---|---|---|---|---|
| 0.95 | 64 | 284 | 0.9500 | 386.7 | 322.7 | 11.55 MB | 64.0 | 0.40 MB |  |
| 0.99 | 64 | no llega | — | — | — | — | — | — | |
| 0.95 | 100 | 185 | 0.9500 | 310.2 | 210.2 | 7.52 MB | 100.0 | 0.62 MB | **la mejor** |
| 0.99 | 100 | no llega | — | — | — | — | — | — | |
| 0.95 | 200 | 151 | 0.9501 | 371.5 | 171.5 | 6.14 MB | 200.0 | 1.24 MB |  |
| 0.99 | 200 | 1080 | 0.9900 | 1424.9 | 1224.9 | 44.04 MB | 200.0 | 1.24 MB |  |
| 0.95 | 350 | 145 | 0.9501 | 514.7 | 164.7 | 5.90 MB | 350.0 | 2.16 MB |  |
| 0.99 | 350 | 928 | 0.9900 | 1402.9 | 1052.9 | 37.84 MB | 350.0 | 2.16 MB | **la mejor** |
| por defecto: 0.9674 | 350 | 256 | 0.9674 | 640.9 | 290.9 | 10.41 MB | 350.0 | 2.16 MB | |


### Las curvas: dos rondas, deriva y actividad de usuario

Una curva es un índice cronometrado en un proceso propio. La medida es la primera ronda; la segunda detecta deriva.

| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca | repetición de noche |
|---|---|---|---|---|---|---|
| código completo en RAM | 10:50 a 10:54 | 11:34 a 11:39 | sí, en la ronda 1 | −7.1 % (0.95, 8 threads, en caliente: 1.15 y 1.07 ms) | **actividad** | 23:00 a 23:05 |
| 512 bits en RAM | 10:54 a 10:59 | 11:39 a 11:44 | sí, en la ronda 1 | −14.4 % (0.99, 8 threads, en caliente: 5.04 y 4.32 ms) | **deriva y actividad** | 23:05 a 23:10 |
| 384 bits en RAM | 10:59 a 11:04 | 11:44 a 11:50 | no | −4.4 % (0.99, 8 threads, en frío: 25.19 y 24.08 ms) | — | — |
| halo de 512 bits | 11:04 a 11:10 | 11:50 a 11:56 | no | −13.1 % (0.99, 8 threads, en caliente: 9.33 y 8.11 ms) | **deriva** | 23:10 a 23:16 |
| halo de 512 bits, podado | 11:10 a 11:16 | 11:56 a 12:02 | sí, en la ronda 1 | −14.9 % (0.95, 8 threads, en caliente: 2.43 y 2.07 ms) | **deriva y actividad** | 23:16 a 23:22 |
| HFresh, la mejor a 0.95 | 11:21 a 11:24 | 12:07 a 12:11 | no | −1.6 % (recall 0.9500, 1 thread, en caliente: 3.47 y 3.41 ms) | — | — |
| HFresh, la mejor a 0.99 | 11:24 a 11:34 | 12:11 a 12:21 | no | +3.2 % (recall 0.9900, 8 threads, en frío: 234.85 y 242.48 ms) | — | — |
| HFresh por defecto | 11:16 a 11:21 | 12:02 a 12:07 | sí, en la ronda 1 | −9.1 % (recall 0.9674, 1 thread, en caliente: 5.51 y 5.01 ms) | **actividad** | 23:22 a 23:27 |

- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.
- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.
- **Curvas marcadas: código completo en RAM, 512 bits en RAM, halo de 512 bits, halo de 512 bits, podado, HFresh por defecto.** Todas repetidas.
- **Una curva repetida de noche se lee de su repetición,** que viene de otra sesión que las curvas con las que se compara.

**Las medianas de cada curva en cada ronda, en ms:**

| curva | punto | threads | estado | ronda 1 | ronda 2 | diferencia | repetición de noche |
|---|---|---|---|---|---|---|---|
| código completo en RAM | 0.95 | 1 | caliente | 0.75 | 0.70 | −6.5 % | 0.71 |
| código completo en RAM | 0.95 | 1 | frío | 3.27 | 3.13 | −4.4 % | 3.17 |
| código completo en RAM | 0.95 | 8 | caliente | 1.15 | 1.07 | −7.1 % | 1.07 |
| código completo en RAM | 0.95 | 8 | frío | 10.37 | 10.06 | −3.0 % | 9.80 |
| código completo en RAM | 0.99 | 1 | caliente | 3.13 | 2.93 | −6.6 % | 2.95 |
| código completo en RAM | 0.99 | 1 | frío | 8.54 | 8.12 | −5.0 % | 8.04 |
| código completo en RAM | 0.99 | 8 | caliente | 4.06 | 3.94 | −2.9 % | 3.94 |
| código completo en RAM | 0.99 | 8 | frío | 20.20 | 20.37 | +0.8 % | 20.06 |
| 512 bits en RAM | 0.95 | 1 | caliente | 0.92 | 0.92 | −0.1 % | 0.91 |
| 512 bits en RAM | 0.95 | 1 | frío | 3.63 | 3.60 | −0.9 % | 3.55 |
| 512 bits en RAM | 0.95 | 8 | caliente | 1.32 | 1.30 | −1.9 % | 1.30 |
| 512 bits en RAM | 0.95 | 8 | frío | 11.64 | 11.54 | −0.9 % | 11.89 |
| 512 bits en RAM | 0.99 | 1 | caliente | 3.28 | 3.20 | −2.3 % | 3.22 |
| 512 bits en RAM | 0.99 | 1 | frío | 8.93 | 8.77 | −1.9 % | 8.84 |
| 512 bits en RAM | 0.99 | 8 | caliente | 5.04 | 4.32 | **−14.4 %** | 4.26 |
| 512 bits en RAM | 0.99 | 8 | frío | 22.53 | 22.11 | −1.9 % | 22.26 |
| 384 bits en RAM | 0.95 | 1 | caliente | 0.99 | 0.97 | −2.4 % | — |
| 384 bits en RAM | 0.95 | 1 | frío | 3.87 | 3.92 | +1.2 % | — |
| 384 bits en RAM | 0.95 | 8 | caliente | 1.44 | 1.39 | −3.8 % | — |
| 384 bits en RAM | 0.95 | 8 | frío | 12.63 | 12.70 | +0.5 % | — |
| 384 bits en RAM | 0.99 | 1 | caliente | 3.34 | 3.28 | −1.9 % | — |
| 384 bits en RAM | 0.99 | 1 | frío | 9.91 | 9.69 | −2.2 % | — |
| 384 bits en RAM | 0.99 | 8 | caliente | 4.71 | 4.61 | −2.1 % | — |
| 384 bits en RAM | 0.99 | 8 | frío | 25.19 | 24.08 | −4.4 % | — |
| halo de 512 bits | 0.95 | 1 | caliente | 1.27 | 1.28 | +0.4 % | 1.27 |
| halo de 512 bits | 0.95 | 1 | frío | 4.85 | 4.87 | +0.5 % | 4.90 |
| halo de 512 bits | 0.95 | 8 | caliente | 2.15 | 2.12 | −1.8 % | 2.14 |
| halo de 512 bits | 0.95 | 8 | frío | 16.40 | 15.83 | −3.5 % | 15.81 |
| halo de 512 bits | 0.99 | 1 | caliente | 5.02 | 4.88 | −2.8 % | 4.87 |
| halo de 512 bits | 0.99 | 1 | frío | 14.60 | 14.15 | −3.1 % | 14.16 |
| halo de 512 bits | 0.99 | 8 | caliente | 9.33 | 8.11 | **−13.1 %** | 8.21 |
| halo de 512 bits | 0.99 | 8 | frío | 41.58 | 39.73 | −4.4 % | 40.31 |
| halo de 512 bits, podado | 0.95 | 1 | caliente | 1.39 | 1.35 | −2.7 % | 1.34 |
| halo de 512 bits, podado | 0.95 | 1 | frío | 4.77 | 4.57 | −4.1 % | 4.68 |
| halo de 512 bits, podado | 0.95 | 8 | caliente | 2.43 | 2.07 | **−14.9 %** | 2.08 |
| halo de 512 bits, podado | 0.95 | 8 | frío | 15.48 | 14.83 | −4.2 % | 14.93 |
| halo de 512 bits, podado | 0.99 | 1 | caliente | 5.66 | 5.53 | −2.4 % | 5.57 |
| halo de 512 bits, podado | 0.99 | 1 | frío | 16.00 | 14.68 | −8.3 % | 14.83 |
| halo de 512 bits, podado | 0.99 | 8 | caliente | 10.06 | 8.60 | **−14.5 %** | 8.61 |
| halo de 512 bits, podado | 0.99 | 8 | frío | 39.80 | 37.55 | −5.6 % | 37.39 |
| HFresh, la mejor a 0.95 | recall 0.9500 | 1 | caliente | 3.47 | 3.41 | −1.6 % | — |
| HFresh, la mejor a 0.95 | recall 0.9500 | 1 | frío | 46.81 | 46.16 | −1.4 % | — |
| HFresh, la mejor a 0.95 | recall 0.9500 | 8 | caliente | 9.16 | 9.04 | −1.3 % | — |
| HFresh, la mejor a 0.95 | recall 0.9500 | 8 | frío | 76.34 | 77.01 | +0.9 % | — |
| HFresh, la mejor a 0.99 | recall 0.9900 | 1 | caliente | 16.33 | 16.09 | −1.4 % | — |
| HFresh, la mejor a 0.99 | recall 0.9900 | 1 | frío | 178.98 | 180.68 | +1.0 % | — |
| HFresh, la mejor a 0.99 | recall 0.9900 | 8 | caliente | 41.84 | 41.75 | −0.2 % | — |
| HFresh, la mejor a 0.99 | recall 0.9900 | 8 | frío | 234.85 | 242.48 | +3.2 % | — |
| HFresh por defecto | recall 0.9674 | 1 | caliente | 5.51 | 5.01 | −9.1 % | 4.98 |
| HFresh por defecto | recall 0.9674 | 1 | frío | 69.52 | 67.91 | −2.3 % | 70.33 |
| HFresh por defecto | recall 0.9674 | 8 | caliente | 14.09 | 13.91 | −1.3 % | 13.95 |
| HFresh por defecto | recall 0.9674 | 8 | frío | 130.91 | 133.08 | +1.7 % | 134.40 |

### Latencia y QPS, en caliente

Tras una pasada sin cronometrar. La pasada de QPS mediano de las cronometradas.

**Recall 0.95, 1 thread:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|
| código completo en RAM (repetida de noche) | ef 20 | 0.71 | 1.67 | 2.13 | 1223 | bloques 0.17, scan y ruteo 0.43, rescore 0.13, centroides 0.06 |
| 512 bits en RAM (repetida de noche) | ef 21 | 0.91 | 2.95 | 4.49 | 832 | bloques 0.30, scan y ruteo 0.66, rescore 0.14, centroides 0.06 |
| 384 bits en RAM | ef 21 | 0.99 | 3.55 | 5.93 | 728 | bloques 0.37, scan y ruteo 0.74, rescore 0.14, centroides 0.07 |
| halo de 512 bits (repetida de noche) | ef 21 | 1.27 | 4.81 | 7.26 | 562 | bloques 0.51, scan y ruteo 1.01, rescore 0.14, centroides 0.07 |
| halo de 512 bits, podado (repetida de noche) | ef 25 | 1.34 | 5.26 | 7.98 | 521 | bloques 0.48, scan y ruteo 1.18, rescore 0.14, centroides 0.06 |
| HFresh, la mejor a 0.95 | searchProbe 185, rescoreLimit 100 | 3.47 | 4.43 | 5.07 | 283 | postings 1.17, scan 1.32, rescore 0.29, centroides 0.70 |
| HFresh por defecto, recall 0.9674 (repetida de noche) | searchProbe 256, rescoreLimit 350 | 4.98 | 6.29 | 6.91 | 197 | postings 1.60, scan 1.82, rescore 0.63, centroides 0.91 |

**Recall 0.95, 8 threads:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|
| código completo en RAM (repetida de noche) | ef 20 | 1.07 | 2.38 | 3.08 | 6604 | bloques 0.26, scan y ruteo 0.53, rescore 0.30, centroides 0.08 |
| 512 bits en RAM (repetida de noche) | ef 21 | 1.30 | 4.11 | 6.25 | 4703 | bloques 0.49, scan y ruteo 0.79, rescore 0.28, centroides 0.08 |
| 384 bits en RAM | ef 21 | 1.44 | 5.06 | 8.33 | 4047 | bloques 0.66, scan y ruteo 0.89, rescore 0.28, centroides 0.08 |
| halo de 512 bits (repetida de noche) | ef 21 | 2.14 | 7.97 | 11.83 | 2691 | bloques 1.24, scan y ruteo 1.28, rescore 0.30, centroides 0.09 |
| halo de 512 bits, podado (repetida de noche) | ef 25 | 2.08 | 7.83 | 11.82 | 2738 | bloques 1.02, scan y ruteo 1.47, rescore 0.29, centroides 0.08 |
| HFresh, la mejor a 0.95 | searchProbe 185, rescoreLimit 100 | 9.16 | 12.35 | 13.67 | 863 | postings 3.70, scan 3.79, rescore 0.77, centroides 0.92 |
| HFresh por defecto, recall 0.9674 (repetida de noche) | searchProbe 256, rescoreLimit 350 | 13.95 | 18.41 | 21.13 | 572 | postings 5.20, scan 5.13, rescore 2.26, centroides 1.22 |

**Recall 0.99, 1 thread:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|
| código completo en RAM (repetida de noche) | ef 284 | 2.95 | 7.55 | 9.34 | 291 | bloques 0.82, scan y ruteo 2.34, rescore 0.14, centroides 0.07 |
| 512 bits en RAM (repetida de noche) | ef 187 | 3.22 | 10.88 | 16.02 | 234 | bloques 1.19, scan y ruteo 2.78, rescore 0.14, centroides 0.07 |
| 384 bits en RAM | ef 164 | 3.34 | 12.15 | 18.17 | 218 | bloques 1.36, scan y ruteo 2.91, rescore 0.14, centroides 0.07 |
| halo de 512 bits (repetida de noche) | ef 187 | 4.87 | 17.56 | 25.23 | 150 | bloques 2.07, scan y ruteo 4.24, rescore 0.15, centroides 0.08 |
| halo de 512 bits, podado (repetida de noche) | ef 239 | 5.57 | 18.89 | 27.11 | 137 | bloques 2.01, scan y ruteo 4.94, rescore 0.15, centroides 0.08 |
| HFresh, la mejor a 0.99 | searchProbe 928, rescoreLimit 350 | 16.33 | 18.47 | 19.34 | 61 | postings 6.07, scan 6.36, rescore 0.72, centroides 3.15 |
| HFresh por defecto, recall 0.9674 (repetida de noche) | searchProbe 256, rescoreLimit 350 | 4.98 | 6.29 | 6.91 | 197 | postings 1.60, scan 1.82, rescore 0.63, centroides 0.91 |

**Recall 0.99, 8 threads:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|
| código completo en RAM (repetida de noche) | ef 284 | 3.94 | 10.14 | 12.61 | 1736 | bloques 1.29, scan y ruteo 2.89, rescore 0.25, centroides 0.09 |
| 512 bits en RAM (repetida de noche) | ef 187 | 4.26 | 14.46 | 21.45 | 1403 | bloques 1.95, scan y ruteo 3.29, rescore 0.25, centroides 0.08 |
| 384 bits en RAM | ef 164 | 4.71 | 17.23 | 25.34 | 1235 | bloques 2.45, scan y ruteo 3.54, rescore 0.25, centroides 0.09 |
| halo de 512 bits (repetida de noche) | ef 187 | 8.21 | 28.99 | 42.11 | 729 | bloques 5.12, scan y ruteo 5.31, rescore 0.29, centroides 0.10 |
| halo de 512 bits, podado (repetida de noche) | ef 239 | 8.61 | 28.45 | 40.66 | 720 | bloques 4.45, scan y ruteo 6.11, rescore 0.28, centroides 0.09 |
| HFresh, la mejor a 0.99 | searchProbe 928, rescoreLimit 350 | 41.84 | 54.34 | 62.10 | 191 | postings 16.67, scan 18.79, rescore 2.26, centroides 4.05 |
| HFresh por defecto, recall 0.9674 (repetida de noche) | searchProbe 256, rescoreLimit 350 | 13.95 | 18.41 | 21.13 | 572 | postings 5.20, scan 5.13, rescore 2.26, centroides 1.22 |

**Cada celda frente a la mejor configuración de HFresh para ese recall, en caliente:** su p50 entre el de HFresh. Menos de un 10 % de diferencia es un empate.

| celda | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |
|---|---|---|---|---|
| código completo en RAM (repetida de noche) | 0.21x, menos | 0.12x, menos | 0.18x, menos | 0.09x, menos |
| 512 bits en RAM (repetida de noche) | 0.26x, menos | 0.14x, menos | 0.20x, menos | 0.10x, menos |
| 384 bits en RAM | 0.29x, menos | 0.16x, menos | 0.20x, menos | 0.11x, menos |
| halo de 512 bits (repetida de noche) | 0.37x, menos | 0.23x, menos | 0.30x, menos | 0.20x, menos |
| halo de 512 bits, podado (repetida de noche) | 0.39x, menos | 0.23x, menos | 0.34x, menos | 0.21x, menos |

### Latencia y QPS, en frío

Cada tanda, tras tirar la caché y reabrir el índice, con el índice de los segmentos ya leído.

**Recall 0.95, 1 thread:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|
| código completo en RAM (repetida de noche) | ef 20 | 3.17 | 5.22 | 5.92 | 300 | bloques 1.18, scan y ruteo 0.43, rescore 1.61, centroides 0.07 |
| 512 bits en RAM (repetida de noche) | ef 21 | 3.55 | 8.16 | 13.43 | 235 | bloques 1.90, scan y ruteo 0.64, rescore 1.58, centroides 0.07 |
| 384 bits en RAM | ef 21 | 3.87 | 9.84 | 16.92 | 208 | bloques 2.37, scan y ruteo 0.71, rescore 1.58, centroides 0.08 |
| halo de 512 bits (repetida de noche) | ef 21 | 4.90 | 14.37 | 24.00 | 157 | bloques 3.63, scan y ruteo 0.99, rescore 1.59, centroides 0.08 |
| halo de 512 bits, podado (repetida de noche) | ef 25 | 4.68 | 13.56 | 23.25 | 166 | bloques 3.10, scan y ruteo 1.17, rescore 1.59, centroides 0.08 |
| HFresh, la mejor a 0.95 | searchProbe 185, rescoreLimit 100 | 46.81 | 55.61 | 57.78 | 22 | postings 40.27, scan 1.40, rescore 3.76, centroides 0.83 |
| HFresh por defecto, recall 0.9674 (repetida de noche) | searchProbe 256, rescoreLimit 350 | 70.33 | 83.82 | 86.08 | 14 | postings 55.04, scan 1.92, rescore 10.88, centroides 1.10 |

**Recall 0.95, 8 threads:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|
| código completo en RAM (repetida de noche) | ef 20 | 9.80 | 16.32 | 18.96 | 679 | bloques 3.61, scan y ruteo 0.50, rescore 6.04, centroides 0.09 |
| 512 bits en RAM (repetida de noche) | ef 21 | 11.89 | 22.31 | 30.62 | 532 | bloques 5.73, scan y ruteo 0.72, rescore 5.98, centroides 0.09 |
| 384 bits en RAM | ef 21 | 12.63 | 25.67 | 34.34 | 479 | bloques 7.01, scan y ruteo 0.78, rescore 5.72, centroides 0.10 |
| halo de 512 bits (repetida de noche) | ef 21 | 15.81 | 35.47 | 51.99 | 351 | bloques 10.89, scan y ruteo 1.15, rescore 5.67, centroides 0.11 |
| halo de 512 bits, podado (repetida de noche) | ef 25 | 14.93 | 34.21 | 44.66 | 384 | bloques 9.53, scan y ruteo 1.34, rescore 5.58, centroides 0.10 |
| HFresh, la mejor a 0.95 | searchProbe 185, rescoreLimit 100 | 76.34 | 96.13 | 101.19 | 96 | postings 59.35, scan 2.38, rescore 15.50, centroides 0.97 |
| HFresh por defecto, recall 0.9674 (repetida de noche) | searchProbe 256, rescoreLimit 350 | 134.40 | 177.37 | 182.12 | 54 | postings 79.95, scan 3.29, rescore 53.66, centroides 1.30 |

**Recall 0.99, 1 thread:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|
| código completo en RAM (repetida de noche) | ef 284 | 8.04 | 17.74 | 21.33 | 111 | bloques 4.97, scan y ruteo 2.34, rescore 1.57, centroides 0.08 |
| 512 bits en RAM (repetida de noche) | ef 187 | 8.84 | 27.16 | 41.80 | 87 | bloques 6.88, scan y ruteo 2.79, rescore 1.58, centroides 0.08 |
| 384 bits en RAM | ef 164 | 9.91 | 32.04 | 49.22 | 77 | bloques 8.30, scan y ruteo 2.85, rescore 1.59, centroides 0.09 |
| halo de 512 bits (repetida de noche) | ef 187 | 14.16 | 50.10 | 79.42 | 51 | bloques 13.58, scan y ruteo 4.11, rescore 1.56, centroides 0.09 |
| halo de 512 bits, podado (repetida de noche) | ef 239 | 14.83 | 46.63 | 67.00 | 53 | bloques 12.11, scan y ruteo 4.91, rescore 1.59, centroides 0.09 |
| HFresh, la mejor a 0.99 | searchProbe 928, rescoreLimit 350 | 178.98 | 251.76 | 259.49 | 6 | postings 153.89, scan 7.23, rescore 10.82, centroides 3.51 |
| HFresh por defecto, recall 0.9674 (repetida de noche) | searchProbe 256, rescoreLimit 350 | 70.33 | 83.82 | 86.08 | 14 | postings 55.04, scan 1.92, rescore 10.88, centroides 1.10 |

**Recall 0.99, 8 threads:**

| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |
|---|---|---|---|---|---|---|
| código completo en RAM (repetida de noche) | ef 284 | 20.06 | 40.89 | 48.29 | 312 | bloques 13.81, scan y ruteo 2.65, rescore 4.91, centroides 0.10 |
| 512 bits en RAM (repetida de noche) | ef 187 | 22.26 | 54.42 | 67.90 | 246 | bloques 17.61, scan y ruteo 3.04, rescore 4.73, centroides 0.09 |
| 384 bits en RAM | ef 164 | 25.19 | 63.56 | 81.35 | 213 | bloques 20.99, scan y ruteo 3.33, rescore 4.66, centroides 0.10 |
| halo de 512 bits (repetida de noche) | ef 187 | 40.31 | 110.07 | 136.05 | 131 | bloques 37.23, scan y ruteo 4.94, rescore 4.68, centroides 0.11 |
| halo de 512 bits, podado (repetida de noche) | ef 239 | 37.39 | 96.87 | 120.15 | 145 | bloques 32.52, scan y ruteo 5.76, rescore 4.57, centroides 0.11 |
| HFresh, la mejor a 0.99 | searchProbe 928, rescoreLimit 350 | 234.85 | 347.37 | 355.92 | 31 | postings 182.15, scan 11.17, rescore 42.21, centroides 4.16 |
| HFresh por defecto, recall 0.9674 (repetida de noche) | searchProbe 256, rescoreLimit 350 | 134.40 | 177.37 | 182.12 | 54 | postings 79.95, scan 3.29, rescore 53.66, centroides 1.30 |

**Cada celda frente a la mejor configuración de HFresh para ese recall, en frío:** su p50 entre el de HFresh. Menos de un 10 % de diferencia es un empate.

| celda | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |
|---|---|---|---|---|
| código completo en RAM (repetida de noche) | 0.07x, menos | 0.13x, menos | 0.04x, menos | 0.09x, menos |
| 512 bits en RAM (repetida de noche) | 0.08x, menos | 0.16x, menos | 0.05x, menos | 0.09x, menos |
| 384 bits en RAM | 0.08x, menos | 0.17x, menos | 0.06x, menos | 0.11x, menos |
| halo de 512 bits (repetida de noche) | 0.10x, menos | 0.21x, menos | 0.08x, menos | 0.17x, menos |
| halo de 512 bits, podado (repetida de noche) | 0.10x, menos | 0.20x, menos | 0.08x, menos | 0.16x, menos |

**Las tandas en frío:**

| índice | recall | threads | tandas | queries por tanda | cargas que repiten posting | p50 de cada mitad, ms | reapertura, ms |
|---|---|---|---|---|---|---|---|
| código completo en RAM | 0.95 | 1 | 50 | 10 | 2 % | 3.15 y 3.18 | 182 |
| código completo en RAM | 0.95 | 8 | 32 | 16 | 2 % | 9.99 y 9.62 | 185 |
| código completo en RAM | 0.99 | 1 | 50 | 10 | 7 % | 8.28 y 7.89 | 182 |
| código completo en RAM | 0.99 | 8 | 32 | 16 | 10 % | 19.56 y 20.56 | 185 |
| 512 bits en RAM | 0.95 | 1 | 50 | 10 | 3 % | 3.46 y 3.72 | 100 |
| 512 bits en RAM | 0.95 | 8 | 32 | 16 | 4 % | 11.89 y 11.85 | 103 |
| 512 bits en RAM | 0.99 | 1 | 50 | 10 | 10 % | 8.60 y 9.11 | 107 |
| 512 bits en RAM | 0.99 | 8 | 32 | 16 | 15 % | 21.44 y 23.47 | 106 |
| 384 bits en RAM | 0.95 | 1 | 50 | 10 | 4 % | 3.72 y 3.98 | 81 |
| 384 bits en RAM | 0.95 | 8 | 32 | 16 | 5 % | 12.43 y 12.73 | 82 |
| 384 bits en RAM | 0.99 | 1 | 50 | 10 | 11 % | 9.67 y 10.06 | 84 |
| 384 bits en RAM | 0.99 | 8 | 32 | 16 | 17 % | 23.04 y 26.30 | 83 |
| halo de 512 bits | 0.95 | 1 | 50 | 10 | 3 % | 4.52 y 5.20 | 58 |
| halo de 512 bits | 0.95 | 8 | 32 | 16 | 4 % | 15.66 y 16.01 | 59 |
| halo de 512 bits | 0.99 | 1 | 50 | 10 | 10 % | 13.11 y 14.86 | 58 |
| halo de 512 bits | 0.99 | 8 | 32 | 16 | 15 % | 39.02 y 42.46 | 59 |
| halo de 512 bits, podado | 0.95 | 1 | 50 | 10 | 3 % | 4.48 y 4.94 | 54 |
| halo de 512 bits, podado | 0.95 | 8 | 32 | 16 | 4 % | 14.82 y 15.02 | 54 |
| halo de 512 bits, podado | 0.99 | 1 | 50 | 10 | 11 % | 14.15 y 15.08 | 56 |
| halo de 512 bits, podado | 0.99 | 8 | 32 | 16 | 17 % | 36.68 y 38.32 | 60 |
| HFresh, la mejor a 0.95 | 0.9500 | 1 | 50 | 10 | — | 46.72 y 46.96 | 421 |
| HFresh, la mejor a 0.95 | 0.9500 | 8 | 32 | 16 | — | 75.49 y 76.99 | 423 |
| HFresh, la mejor a 0.99 | 0.9900 | 1 | 50 | 10 | — | 178.28 y 179.10 | 423 |
| HFresh, la mejor a 0.99 | 0.9900 | 8 | 32 | 16 | — | 236.83 y 232.03 | 420 |
| HFresh por defecto | 0.9674 | 1 | 50 | 10 | — | 70.02 y 70.58 | 436 |
| HFresh por defecto | 0.9674 | 8 | 32 | 16 | — | 132.88 y 135.70 | 438 |

**La primera query de cada tanda frente a las demás,** con 1 thread. La primera es la que menos encuentra en la caché:

| índice | recall | primera: p50, ms | media | las demás: p50, ms | media | rescore de la primera, ms | de las demás |
|---|---|---|---|---|---|---|---|
| código completo en RAM | 0.95 | 3.81 | 4.02 | 3.13 | 3.25 | 1.60 | 1.61 |
| código completo en RAM | 0.99 | 10.18 | 10.83 | 7.86 | 8.85 | 1.59 | 1.57 |
| 512 bits en RAM | 0.95 | 4.55 | 5.43 | 3.47 | 4.11 | 1.60 | 1.58 |
| 512 bits en RAM | 0.99 | 12.71 | 15.21 | 8.60 | 11.04 | 1.62 | 1.58 |
| 384 bits en RAM | 0.95 | 4.85 | 5.90 | 3.78 | 4.68 | 1.59 | 1.58 |
| 384 bits en RAM | 0.99 | 14.73 | 18.13 | 9.64 | 12.39 | 1.60 | 1.59 |
| halo de 512 bits | 0.95 | 6.54 | 8.19 | 4.72 | 6.15 | 1.64 | 1.58 |
| halo de 512 bits | 0.99 | 23.80 | 26.89 | 13.70 | 18.63 | 1.59 | 1.55 |
| halo de 512 bits, podado | 0.95 | 5.93 | 7.55 | 4.59 | 5.84 | 1.58 | 1.60 |
| halo de 512 bits, podado | 0.99 | 21.70 | 25.53 | 13.81 | 18.10 | 1.61 | 1.59 |
| HFresh, la mejor a 0.95 | 0.9500 | 55.61 | 55.52 | 46.48 | 45.32 | 4.22 | 3.71 |
| HFresh, la mejor a 0.99 | 0.9900 | 250.82 | 251.36 | 169.95 | 167.20 | 13.12 | 10.56 |
| HFresh por defecto | 0.9674 | 83.78 | 83.67 | 69.54 | 67.46 | 13.26 | 10.62 |

### Criterios 2 y 3: en frío, frente a HFresh, a 0.95

Se leen contra la mejor configuración de HFresh a 0.95 (enmienda 4), por el p50 con 1 thread. Menos de un 10 % de diferencia, sobre HFresh, es un empate (enmienda 5): no cumple «menos latencia» y sí cumple «no más lento».

- **HFresh, la mejor a 0.95** (searchProbe 185, rescoreLimit 100, recall 0.9500), en frío con 1 thread: p50 46.81 ms, p95 55.61, p99 57.78. Sus dos mitades de tandas: 46.72 y 46.96 ms.
- **HFresh por defecto** (repetida de noche) (searchProbe 256, rescoreLimit 350, recall 0.9674): p50 70.33 ms, p95 83.82, p99 86.08.

- **Criterio 2, la celda de 512 bits en RAM: se cumple.** p50 de 3.55 ms frente a 46.81 de HFresh (0.08x): menos latencia. Llega a 0.99 con ef 187. Además: sus mitades de tandas, 3.46 y 3.72 ms; p95 8.16 frente a 55.61; p99 13.43 frente a 57.78; con 8 threads, p50 11.89 frente a 76.34 (0.16x, menos). **Ojo:** las dos curvas vienen de sesiones distintas (repetición de noche y ronda 1).
- **Criterio 3, el halo: se cumple, con halo de 512 bits y halo de 512 bits, podado.**
  - Halo de 512 bits: p50 de 4.90 ms frente a 46.81 de HFresh (0.10x): menos latencia. Además: sus mitades de tandas, 4.52 y 5.20 ms; p95 14.37 frente a 55.61; p99 24.00 frente a 57.78; con 8 threads, p50 15.81 frente a 76.34 (0.21x, menos). **Ojo:** las dos curvas vienen de sesiones distintas (repetición de noche y ronda 1).
  - Halo de 512 bits, podado: p50 de 4.68 ms frente a 46.81 de HFresh (0.10x): menos latencia. Además: sus mitades de tandas, 4.48 y 4.94 ms; p95 13.56 frente a 55.61; p99 23.25 frente a 57.78; con 8 threads, p50 14.93 frente a 76.34 (0.20x, menos). **Ojo:** las dos curvas vienen de sesiones distintas (repetición de noche y ronda 1).

### RAM residente y disco

| índice | heap en uso tras abrir | huella del proceso | disco: store | vectores exactos | lo residente, en ficheros | total |
|---|---|---|---|---|---|---|
| código completo en RAM | 272.52 MB | 541.77 MB | 242.97 MB | 6082.56 MB | 232.47 MB | 6558.00 MB |
| 512 bits en RAM | 145.94 MB | 287.74 MB | 242.97 MB | 6082.56 MB | 105.75 MB | 6431.28 MB |
| 384 bits en RAM | 129.92 MB | 255.98 MB | 242.97 MB | 6082.56 MB | 89.91 MB | 6415.44 MB |
| halo de 512 bits | 74.56 MB | 145.15 MB | 663.42 MB | 6082.56 MB | 34.47 MB | 6780.45 MB |
| halo de 512 bits, podado | 74.58 MB | 145.11 MB | 494.74 MB | 6082.56 MB | 34.47 MB | 6611.77 MB |
| HFresh | 163.27 MB | 338.26 MB | 1296.08 MB | 6221.75 MB | 7.77 MB | 7525.60 MB |

- **Las celdas de posting-edge comparten un solo fichero de vectores,** enlazado: en disco está una vez.
- **El heap es el de Go tras abrir el índice y recolectar.** La caché del sistema no cuenta.
- **En HFresh, «vectores exactos» es su bucket `objects`,** y «store» sus otros buckets.

### Lo que vale y lo que no

- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre índices, medidos igual.
- **Es una sesión de dos rondas.** La medida es la primera; la segunda dice qué curvas derivaron.
- **Menos de un 10 % entre dos índices es un empate.**
- **El p99 en frío sale de las 5 queries más lentas de 500.**
