# Índice posting+edges: notas para el diseño de la versión en disco

**Estado:** semilla del design doc de disco. Tiene dos secciones: los tres layouts posibles del posting en disco, y el detalle del tercero, que es lo que dejó decidido el benchmark en memoria (v1.2, Fase B, 2026-11-11). El resto del diseño de disco no está escrito aquí.

Las cifras son de `bench-report.md`: dbpedia-openai 1M (1536d), S=236, P=256, 1 thread, todo en RAM. Son cuentas de trabajo y recall, que no dependen de dónde vivan los postings. Los tiempos de I/O no están medidos en ninguna parte de este documento.

## Los layouts del posting en disco

### El baseline: HFresh hoy

Los postings de HFresh llevan códigos RQ1, no vectores crudos. Una query lee postings y los escanea por código, y el rescore final son lecturas aleatorias al store de vectores, una por candidato (rescoreLimit 350 por defecto).

Así que una query en disco paga dos términos distintos:

1. **Cargas de posting:** lecturas de un posting entero cada una, que salen en lotes paralelos, una ronda por lote.
2. **Rescore:** una lectura aleatoria por candidato, de un vector exacto cada una.

El diseño posting+edges ataca el primero. Qué pasa con el segundo depende del layout.

### Las tres opciones

| | (a) solo códigos | (b) códigos y vectores exactos inline | (c) códigos capados, sidecar completo y refinado |
|---|---|---|---|
| qué lleva el posting en disco | id, código completo y aristas de cada miembro | lo de (a) más el vector exacto de cada miembro | id, código corto y aristas |
| qué queda en RAM | el sidecar de códigos completos y los centroides | lo mismo | lo mismo |
| cargas de posting | las de este índice | las mismas, cada una ~30 veces más pesada | las mismas, cada una entre 2.6 y 4.3 veces más ligera |
| rescore | rescoreLimit lecturas aleatorias | ninguna lectura: el vector vino con su posting | rescoreLimit lecturas aleatorias, con el rescoreLimit del código completo |
| estado | es el layout de HFresh y el del índice en memoria | sin construir | la celda desacoplada lo mide sin el refinado; el refinado está sin construir |

**(a) Solo códigos, rescore aleatorio. Es el layout actual.**

- Hereda de HFresh el término de rescore tal cual: una lectura aleatoria por candidato.
- Mejora solo las cargas, del orden de 70 veces menos que HFresh. Esa cifra es de contexto, no está medida en este benchmark.
- Medido aquí, a 1536d: 10 cargas a recall 0.95 y 53 a 0.99, en 2.3 y 12.9 rondas, más 64 lecturas aleatorias de rescore. A 768d el rescore son 200 o 400 lecturas, porque el código completo ya es de 768 bits.

**(b) Códigos y vectores exactos en el posting.**

- Elimina el rescore aleatorio: todo candidato del rescore es miembro de un posting que la query ya cargó, así que su vector exacto ya está leído.
- A cambio cada carga lleva los vectores exactos de todos los miembros. A 1536d un miembro pasa de 208 B (id y código) a 6 352 B: unas 30 veces más bytes por carga secuencial.
- El tamaño de posting se vuelve en contra. En (a), un S mayor compra menos cargas a cambio de pocos bytes más. En (b) cada vector escaneado de más cuesta 6 KB, y la Fase C dice que los vectores escaneados crecen con S: 13 235 a recall 0.99 con S=236, y 30 814 con S=2048.

**(c) Códigos capados en el posting, sidecar completo en RAM, refinado antes del rescore.**

- El posting lleva un código corto, que es lo que se lee de disco: con 256 bits un miembro son 48 B y con 512 bits 80 B, frente a los 208 B de (a).
- El routing sigue con el código completo, que vive en el sidecar residente.
- El refinado re-estima con el código completo los mejores candidatos del scan antes del rescore. Es lo que reduce las lecturas aleatorias: sin él, un código de 256 bits en el scan necesita 3 200 candidatos exactos para un techo de recall de 0.994, y el código completo llega a 0.996 con 64.
- Aquí es donde encuentran su papel la celda desacoplada y los códigos capados, que en el modo memoria pierden: en RAM un código corto ahorra poco, y en disco es lo que pesa cada carga.
- El detalle está en la sección siguiente.

### Qué lee cada layout por query

Con S=236 y el código completo de 1536d, del perfil de la Fase C. Los bytes de posting cuentan el id y el código de cada vector escaneado (208 B), sin las aristas, que son 36 B más por miembro en los tres layouts.

| recall | cargas | rondas | vectores escaneados | (a) bytes de posting | (a) lecturas de rescore | (b) bytes de posting | (b) lecturas de rescore |
|---|---|---|---|---|---|---|---|
| 0.95 | 10 | 2.3 | 2 563 | 0.5 MB | 64 | 16 MB | 0 |
| 0.99 | 53 | 12.9 | 13 235 | 2.8 MB | 64 | 84 MB | 0 |

Para (c) las cargas y las rondas son las mismas si el refinado funciona, y los bytes de posting bajan a 0.12 MB y 0.64 MB con 256 bits, o a 0.21 MB y 1.1 MB con 512. Sin refinado, lo medido son 1.5 veces las cargas del control.

### El comparador de (a) contra (b): candidatos por posting cargado

(b) lee el vector exacto de todos los miembros de cada posting cargado para usar solo los que llegan al rescore. Lo que decide entre los dos layouts es cuántos son esos por posting cargado.

| caso | candidatos del rescore | postings cargados | candidatos por posting cargado | vectores exactos que (b) lee | de ellos, los que usa |
|---|---|---|---|---|---|
| 1536d, recall 0.95 | 64 | 10.1 | 6.3 | 2 563 | 2.5 % |
| 1536d, recall 0.99 | 64 | 53 | 1.2 | 13 235 | 0.5 % |
| 768d (wiki-dpr 1M), recall 0.99 | 200 | 44.8 | 4.5 | 10 958 | 1.8 % |

- **Con estas cifras (b) lee entre 40 y 200 vectores exactos por cada uno que usa.** A 1536d y recall 0.99 son 84 MB secuenciales para ahorrarse 64 lecturas aleatorias de 6 KB.
- **(b) solo compensa si los candidatos se concentran:** muchos por posting cargado, en pocos postings.
- **El conteo fino sale de la proyección a disco del v1.4:** por query, media y p95, cuántos candidatos del rescore hay y en cuántos de los postings cargados viven. Las filas de arriba son medias de candidatos entre medias de cargas; ese conteo dirá además si están repartidos o agrupados. Queda por añadir aquí con los datos de 10M.

Lo que no cambia con el layout: en los tres, ningún candidato del rescore necesita cargar un posting propio. Los candidatos solo salen de los scans de postings cargados; los endpoints de las aristas ordenan postings y nunca entran como candidatos.

## Layout (c) en detalle: refinar con el sidecar las estimaciones del scan

**Decisión (2026-11-11): no se construye para el modo memoria; se propone para el modo disco.**

### Por qué interesa en disco

En disco el posting es lo que se lee, y su tamaño lo manda el código de cada miembro. Con códigos de 256 bits (40 B con la cabecera) un posting pesa cinco veces menos que con el código completo de 1536d (200 B). El routing, en cambio, necesita los bits: con 256 bits para todo, las cargas a igual recall son 3.0x las del control.

La disposición que separa las dos cosas ya existe (`UserConfig.ScanCodeBits`): códigos cortos en el posting para el scan, y el código completo en el sidecar residente para ordenar los postings por sus endpoints. Es la "celda desacoplada" del informe.

### Lo medido sin refinar

Cargas de posting por query a igual recall, contra el control de código completo (rescoreLimit 3200 para sacar del medio el corte final, sin margen):

| celda | recall 0.90 | recall 0.95 | recall 0.99 |
|---|---|---|---|
| 256 bits para todo | 1.98x | 3.02x | 3.10x |
| scan a 256 bits, sidecar completo | 1.17x | 1.47x | 1.50x |

Techo de recall por rescoreLimit, subiendo solo ef:

| rescoreLimit | 64 | 200 | 400 | 800 | 1600 | 3200 |
|---|---|---|---|---|---|---|
| scan a 256 bits, sidecar completo | 0.788 | 0.911 | 0.952 | 0.974 | 0.987 | 0.994 |
| código completo (control) | 0.996 | 0.996 | — | — | — | — |

Darle al routing el código completo recupera tres cuartos de las cargas de más. Lo que queda son 1.5x cargas y un techo que obliga a rescorear miles de vectores exactos para llegar a 0.99. En disco cada uno de esos es una lectura al store.

### Las dos causas que quedan

Las dos vienen de usar la estimación del scan como distancia de los candidatos.

1. **La parada, hasta recall 0.97.** El orden en que se cargan los postings ya es el del control: sale de las estimaciones de los endpoints, que son del sidecar. Lo que cambia es dónde para cada query. El umbral de parada es la ef-ésima mejor estimación del scan, con el ruido de 256 bits, y se compara con estimaciones del sidecar. Las cargas se reparten peor entre queries: con la misma mediana de 9 cargas, el control saca recall 0.950 (media 10.1, p95 21) y esta celda 0.932 (media 11.4, p95 29). El corte final no lo explica: doblar el rescoreLimit de 1600 a 3200 mueve el recall 0.0006 como mucho hasta ef=80.
2. **El corte final, desde recall 0.98.** Los candidatos que pasan al rescore exacto se eligen por su estimación de scan. Pasar de rescoreLimit 1600 a 3200 suma 0.0022 de recall a ef=320 y 0.0044 a ef=1024.

Un efecto de la segunda: pasado el techo, subir ef baja el recall (de 0.9516 a 0.9397 con rescoreLimit 400). Más vectores escaneados compiten por los mismos puestos del rescore con estimaciones ruidosas.

### El arreglo

El mismo para las dos: re-estimar con el código completo del sidecar los mejores candidatos del scan antes de que entren en el umbral de parada y en el corte final.

- **Qué se refina.** Tras escanear un posting, los miembros cuya estimación de scan queda dentro de una banda sobre el peor candidato que hoy entraría en el umbral o en el pool del rescore. El resto se descarta con la estimación corta, como ahora.
- **Con qué.** El código completo del miembro en el sidecar, direccionado por slot. Es la misma lectura que ya se hace para cada endpoint de una arista: está residente, no es I/O.
- **Qué deja igual.** El formato del posting en disco (códigos cortos), el routing, las aristas y el rescore exacto final.
- **El knob nuevo.** El ancho de la banda, en unidades de la distancia. Demasiado estrecha deja fuera vecinos verdaderos con mala estimación corta; demasiado ancha refina de más.

### Qué se espera

- **Cargas:** las del control. Con el umbral y el pool sobre estimaciones de código completo, la búsqueda para donde para el control, y el rescoreLimit vuelve al orden de 64.
- **Bytes leídos por query:** hasta cinco veces menos que con el código completo en el posting, a igual número de cargas.
- **Coste nuevo:** una lectura al sidecar por candidato refinado. En RAM un endpoint puntuado cuesta entre 75 y 84 ns.
- **En memoria no compensa,** y por eso no se construye ahí: el scan es entre el 16 y el 19 % de la query y el código corto lo abarata de 54 a 37–50 ns por vector, así que la ganancia tiene un techo del 6 %, con un 14 % más de memoria por guardar dos códigos.

### Qué hay que medir antes de darla por buena

1. Recall a igual número de cargas contra el control de código completo, en ef de 10 a 1024. Sin refinar la diferencia va de -0.038 (8 cargas) a -0.002 (86 cargas); el objetivo es que quede dentro del ruido.
2. Techo de recall con rescoreLimit 64: tiene que alcanzar 0.99 subiendo solo ef, que es el gate del default.
3. Cuántos candidatos se refinan por query en función de la banda, que es el coste.
4. Que subir ef no baje el recall en ningún tramo.
5. El ancho del código de scan. Solo está medido 256 bits; 512 bits (72 B) es el otro candidato, con menos ruido que corregir y menos ahorro por carga.
