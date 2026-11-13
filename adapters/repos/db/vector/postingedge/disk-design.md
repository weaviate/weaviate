# Índice posting+edges: notas para el diseño de la versión en disco

**Estado:** semilla del design doc de disco, y punto de partida de la fase 2 (2026-11-16). Tiene los layouts posibles del posting y de los vectores exactos en disco, el detalle del layout (c), que es lo que dejó decidido el benchmark en memoria (v1.2, Fase B, 2026-11-11), y FastScan. La última sección, "Open for phase 2", lista lo que el diseño de disco tiene que resolver, sin resolverlo. El resto del diseño de disco no está escrito aquí.

Las cifras son de `bench-report.md`: dbpedia-openai 1M (1536d), S=236, P=256, 1 thread, todo en RAM. Son cuentas de trabajo y recall, que no dependen de dónde vivan los postings. Los tiempos de I/O no están medidos en ninguna parte de este documento.

## Los layouts del posting en disco

### El baseline: HFresh hoy

Los postings de HFresh llevan códigos RQ1, no vectores crudos. Una query lee postings y los escanea por código, y el rescore final son lecturas aleatorias, una por candidato (rescoreLimit 350 por defecto).

Así que una query en disco paga dos términos distintos:

1. **Cargas de posting:** lecturas de un posting entero cada una, que salen en lotes paralelos, una ronda por lote.
2. **Rescore:** una lectura aleatoria por candidato, de un vector exacto cada una.

El rescore de hoy no lee de un store propio: lee del bucket de objetos compartido. Cada lectura paga tres cosas que no tienen que ver con el índice:

- **Competencia con el resto del tráfico del nodo,** que usa ese mismo bucket.
- **Deserialización del objeto entero** para sacar de él un vector.
- **La cola de latencia del LSM:** segmentos, compactaciones y su caché.

Eso hace de **eliminar o aislar el rescore del bucket** un criterio de diseño de primer orden. Contar lecturas no basta: dos layouts con las mismas lecturas de rescore no cuestan lo mismo si unas van al bucket y otras a un fichero propio.

El diseño posting+edges ataca el primer término, las cargas. Qué pasa con el segundo depende del layout.

### Las cuatro opciones

| | (a) solo códigos | (b) códigos y exactos inline | (b') exactos en store dedicado | (c) códigos capados, sidecar completo y refinado |
|---|---|---|---|---|
| qué lleva el posting | id, código completo y aristas | lo de (a) más el vector exacto de cada miembro | lo de (a) | id, código corto y aristas |
| dónde viven los vectores exactos | en el bucket de objetos | dentro de los postings | en un fichero propio, en orden de posting | donde digan (a) o (b') |
| qué queda en RAM | sidecar de códigos completos y centroides | lo mismo | lo mismo | lo mismo |
| cargas de posting | las de este índice | las mismas, cada una ~30 veces más pesada | las de (a) | las mismas, cada una entre 2.6 y 4.3 veces más ligera |
| rescore | rescoreLimit lecturas al bucket | ninguna lectura | rescoreLimit lecturas al fichero propio | rescoreLimit lecturas, con el rescoreLimit del código completo |
| estado | layout de HFresh y del índice en memoria | sin construir | sin construir | medido sin el refinado (celda desacoplada); el refinado, sin construir |

**(a) Solo códigos, rescore aleatorio al bucket. Es el layout actual.**

- Hereda de HFresh el término de rescore tal cual: una lectura por candidato al bucket de objetos, con su competencia, su deserialización y su cola.
- Mejora solo las cargas, del orden de 70 veces menos que HFresh. Esa cifra es de contexto, no está medida en este benchmark.
- Medido aquí, a 1536d: 10 cargas a recall 0.95 y 53 a 0.99, en 2.3 y 12.9 rondas, más 64 lecturas de rescore. A 768d el rescore son 200 o 400 lecturas, porque el código completo ya es de 768 bits.

**(b) Códigos y vectores exactos en el posting.**

- Elimina el rescore: todo candidato es miembro de un posting que la query ya cargó, así que su vector exacto ya está leído. El bucket sale del camino de la query.
- A cambio cada carga lleva los vectores exactos de todos los miembros. A 1536d un miembro pasa de 208 B (id y código) a 6 352 B: unas 30 veces más bytes por carga secuencial.
- El tamaño de posting se vuelve en contra. En (a), un S mayor compra menos cargas a cambio de pocos bytes más. En (b) cada vector escaneado de más cuesta 6 KB, y la Fase C dice que los vectores escaneados crecen con S: 13 235 a recall 0.99 con S=236, y 30 814 con S=2048.

**(b') Vectores exactos en un store dedicado, ordenado por posting.**

- Es el layout por orden de posting primario del espectro de la iteración 4 de las simulaciones, aplicado a los vectores exactos: un fichero propio del índice con un vector por slot. Los slots ya van por posting, así que los miembros de un posting quedan contiguos.
- El rescore sigue siendo una lectura por candidato, pero a un offset fijo de un fichero propio: sin bucket, sin deserializar un objeto y sin LSM.
- Los postings no cambian: las cargas son las de (a).
- La localidad da una segunda palanca. Los candidatos de un mismo posting están a menos de un posting de distancia en el fichero, así que varios se pueden servir con una lectura de rango. Cuánto vale depende de cuántos candidatos hay por posting cargado.
- Cuesta una copia de los vectores en disco, y mantener el orden de posting cuando haya inserciones y splits. Lo segundo está abierto: este índice se construye de una vez.

**(c) Códigos capados en el posting, sidecar completo en RAM, refinado antes del rescore.**

- El posting lleva un código corto, que es lo que se lee de disco: con 256 bits un miembro son 48 B y con 512 bits 80 B, frente a los 208 B de (a).
- El routing sigue con el código completo, que vive en el sidecar residente.
- El refinado re-estima con el código completo los mejores candidatos del scan antes del rescore. Es lo que reduce las lecturas aleatorias: sin él, un código de 256 bits en el scan necesita 3 200 candidatos exactos para un techo de recall de 0.994, y el código completo llega a 0.996 con 64.
- No aísla nada por sí solo: sus lecturas de rescore van a donde vivan los vectores. Reduce cuántas son y cuánto pesa cada carga, y se combina con (b').
- Aquí es donde encuentran su papel la celda desacoplada y los códigos capados, que en el modo memoria pierden: en RAM un código corto ahorra poco, y en disco es lo que pesa cada carga.
- El detalle está en la sección siguiente.

### Qué lee cada layout por query

Con S=236 y el código completo de 1536d, del perfil de la Fase C. Los bytes de posting cuentan el id y el código de cada vector escaneado (208 B), sin las aristas, que son 36 B más por miembro en todos los layouts.

| recall | cargas | rondas | vectores escaneados | (a) y (b'): bytes de posting | (a): lecturas al bucket | (b'): lecturas al store propio | (b): bytes de posting | (b): lecturas de rescore |
|---|---|---|---|---|---|---|---|---|
| 0.95 | 10 | 2.3 | 2 563 | 0.5 MB | 64 | 64, de 6 KB | 16 MB | 0 |
| 0.99 | 53 | 12.9 | 13 235 | 2.8 MB | 64 | 64, de 6 KB | 84 MB | 0 |

Para (c) las cargas y las rondas son las mismas si el refinado funciona, y los bytes de posting bajan a 0.12 MB y 0.64 MB con 256 bits, o a 0.21 MB y 1.1 MB con 512. Sin refinado, lo medido son 1.5 veces las cargas del control.

### Por varianza y aislamiento

Los bytes y las lecturas de arriba son cuentas. Esto otro es qué entra en el camino de una query, que es lo que decide su cola de latencia. No hay ninguna latencia de I/O medida: es una evaluación de qué componentes toca cada layout, no de cuánto tardan.

| criterio | (a) | (b) | (b') | (c) |
|---|---|---|---|---|
| lecturas al bucket de objetos por query | rescoreLimit | 0 | 0 | las de (a), o 0 sobre (b') |
| compite con el tráfico del nodo | sí, en el bucket | solo por el dispositivo | solo por el dispositivo | como el layout sobre el que vaya |
| deserialización por candidato | el objeto entero | ninguna | ninguna: offset fijo | como el layout sobre el que vaya |
| de qué depende la cola de latencia | del LSM y de lo que haga el resto del nodo | de lecturas secuenciales grandes, ~30 veces los bytes | de lecturas pequeñas a un fichero propio | de lo mismo que su base, con menos lecturas que un capado sin refinar |
| lo que crece con la query | lecturas al bucket (64 a 400) | bytes leídos (16 a 84 MB) | lecturas pequeñas (64 a 400) | bytes de posting, a la baja |
| disco de más | nada | una copia de los vectores, dentro de los postings | una copia de los vectores, en fichero aparte | nada |

- **(b) aísla del todo y lo paga en ancho de banda.** Saca el bucket del camino, pero convierte cada query en decenas de megabytes secuenciales. Su latencia deja de depender del resto del nodo y pasa a depender de cuántos bytes lee, que son unas 30 veces más.
- **(b') aísla sin tocar las cargas.** Mismas lecturas de rescore que (a) en número, pero a un fichero propio y de coste fijo. Es la única opción que quita las tres cosas que paga hoy el rescore sin encarecer las cargas.
- **(c) no aísla: reduce.** Su efecto es sobre el número de lecturas y los bytes de cada carga. Sobre (a) deja el rescore en el bucket con el rescoreLimit del código completo; sobre (b') suma las dos cosas.
- **La combinación a evaluar es (b') con (c):** cargas ligeras, rescore aislado, y el refinado manteniendo corto el rescore.

### El comparador: candidatos por posting cargado

(b) lee el vector exacto de todos los miembros de cada posting cargado para usar solo los que llegan al rescore. Lo que decide entre (a) y (b) es cuántos son esos por posting cargado. El mismo número dice si en (b') compensa leer por rangos.

| caso | candidatos del rescore | postings cargados | candidatos por posting cargado | vectores exactos que (b) lee | de ellos, los que usa |
|---|---|---|---|---|---|
| 1536d, recall 0.95 | 64 | 10.1 | 6.3 | 2 563 | 2.5 % |
| 1536d, recall 0.99 | 64 | 53 | 1.2 | 13 235 | 0.5 % |
| 768d (wiki-dpr 1M), recall 0.99 | 200 | 43.7 | 4.6 | 10 697 | 1.9 % |
| 768d (wiki-dpr 10M), recall 0.99 | 200 | 51.3 | 3.9 | 13 081 | 1.5 % |

- **Con estas cifras (b) lee entre 40 y 200 vectores exactos por cada uno que usa.** A 1536d y recall 0.99 son 84 MB secuenciales para ahorrarse 64 lecturas de 6 KB.
- **(b) solo compensa si los candidatos se concentran:** muchos por posting cargado, en pocos postings.
- **Para (b'), entre 1 y 6 candidatos por posting cargado es poco para leer postings enteros de vectores** (236 vectores por rango para usar esos pocos). Con estas medias, lo suyo son lecturas sueltas a offset fijo.
- **El conteo por query a 10M (wiki-dpr, 768d, recall 0.99, rescoreLimit 200) lo confirma:** los 200 candidatos viven en postings que la query ya cargó (ninguno fuera, en ninguna de las mil queries), y están repartidos por 51.2 de los 51.3 postings cargados de media (p95 100.7). Son 3.9 candidatos por posting cargado, o 7.8 con rescoreLimit 400. En ese punto (b) leería 40 MB por query (13 081 vectores de 3 KB) para usar el 1.5 %, y (a) y (b') harían 200 lecturas sueltas de 3 KB, al bucket o a un fichero propio.

Lo que no cambia con el layout: en todos, ningún candidato del rescore necesita cargar un posting propio. Los candidatos solo salen de los scans de postings cargados; los endpoints de las aristas ordenan postings y nunca entran como candidatos.

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

## FastScan para el scan de un posting cargado

FastScan puntúa 32 códigos de una vez. Agrupa sus bits de 4 en 4 dimensiones, y una tabla de 16 entradas por grupo, que depende solo de la query, se busca con una TBL para 16 códigos por instrucción. El prototipo está en `postingedge/kernelproto` (`ScanBlock`) y da el mismo entero que el kernel de hoy, bit a bit. Medido en el M1 Max sobre un posting de 236 códigos en caché (bench-report.md, "Coste por distancia"):

| código | kernel de hoy, ns por código | FastScan | factor |
|---|---|---|---|
| 768 bits | 33.3 | 6.6 | 5.0x |
| 1536 bits | 42.9 | 10.8 | 4.0x |

- **En disco el layout no cuesta nada.** El posting de disco lleva sus propios códigos en cualquiera de los layouts de arriba, así que basta con escribirlos en bloques de 32, traspuestos por grupos de 4 dimensiones, en vez de en fila. Son los mismos bytes, con la cabecera de cada código aparte, y leerlos no pide decodificar nada.
- **Los endpoints no se tocan:** se puntúan contra el sidecar residente, que sigue en fila.
- **En memoria no se integra.** Postings y sidecar comparten hoy un solo array de códigos, y FastScan pediría una segunda copia: 992 MB más a 10M.
- **Encaja con (c):** funciona a cualquier ancho múltiplo de 4 dimensiones, y con códigos cortos en el posting el scan por código baja todavía más.

Lo que queda por medir:

- **El scan con el posting recién leído,** no en caché.
- **El último bloque de cada posting va a medias:** 236 son 7 bloques llenos y 12 códigos, y el bloque incompleto cuesta como uno lleno.
- **Construir las tablas de la query,** que en el prototipo sin optimizar cuesta 15.8 µs a 768 bits.

## Open for phase 2

*Añadido al cierre del 2026-11-13 como punto de partida del lunes. Son las cuatro preguntas abiertas, sin resolver. Cada una dice qué hay hoy en el índice en memoria y qué queda por decidir.*

### 1. Mantenimiento: deletes, updates y el mapa vector→posting

**Hoy no hay mantenimiento.** `Delete` devuelve `ErrNotSupported`, y volver a añadir un id también: un update se rechaza.

**Cómo direcciona el índice sus vectores:**

- **Por slot:** su posición en el orden por posting (`slotLayout`). Cada posting tiene un rango contiguo de slots.
- **Una arista guarda el slot de su endpoint.**
- **El sidecar se indexa por slot.**
- **El posting de un endpoint sale de un mapa slot→posting** (`sidecar.postingOf`) en el momento de la búsqueda.

**El argumento del mapa vector→posting.** Una arista apunta a un vector, no a un posting, y la búsqueda solo usa el endpoint para dar rango al posting donde vive. Si las aristas guardan un id estable del vector y el posting se resuelve con un mapa vector→posting, un vector que cambia de posting (un split, una reasignación) cambia una entrada del mapa, y ninguna arista. Con slots en orden de posting, como hoy, cambiar de posting es cambiar de slot, y cada arista que apunta a él quedaría mal.

Abierto:

- **El mapa en sí:** dónde vive, cuánto ocupa (4 B por vector serían 40 MB a 10M) y qué cuesta su indirección por endpoint frente al `postingOf` de hoy.
- **El sidecar:** si se sigue indexando por slot o pasa al id estable, y qué hace con los huecos que dejan los deletes.
- **Las aristas que apuntan a un vector borrado:** si se quedan como lápida, que solo cuesta trabajo y rango, o se reparan, y quién las encuentra (hoy no hay aristas inversas).
- **Un update como delete más insert,** y qué les pasa a las aristas de los vecinos del vector movido.
- **La invisibilidad transitoria de HFresh entre insert y split,** que deja vectores fuera de todo posting, como riesgo que el diseño tiene que cerrar por construcción.

### 2. Build incremental

**Hoy el build es en lote.** `AddBatch` solo admite vectores antes de `Build`; `Build` corre una vez y después `AddBatch` devuelve `ErrBuilt`. A 10M son unas 3 h 45 min, unas 8 veces el de HNSW:

- **La asignación exacta, n·k:** cuadrática en N con S fijo, y anotada como TODO en el código.
- **La etapa de aristas:** 3 668 s con P=320 y R=256.
- **P y R crecen con N** para sostener el recall de arista (bench-report.md, v1.4).

Abierto:

- **Insertar un vector:** su posting de entrada, sus aristas (una búsqueda de vecinos sobre el índice ya construido) y si los vectores que ya existen ganan aristas hacia él, porque sin aristas inversas nadie lo alcanza por arista.
- **Cuándo se parte un posting que pasa de S,** cómo se reasignan sus miembros y cómo se actualiza el grafo de centroides.
- **Qué recall pierde un índice que crece por inserciones** frente a uno construido de una vez, y cada cuánto hace falta rehacerlo.
- **Si la asignación exacta sobrevive** o pasa a una asignación asistida por el grafo de centroides, con el coste de cobertura que eso tenga.

### 3. Modelo de I/O del layout

**Hoy todo son cuentas, no tiempos.** Las cifras de este documento son cargas, rondas, bytes y lecturas por query; ninguna latencia de I/O está medida. A 10M (768d, rescoreLimit 200):

- **A 0.95,** 12.6 cargas en 2.9 rondas.
- **A 0.99,** 51.3 cargas en 12.6 rondas.
- **El rescore** son 200 lecturas.

Abierto:

- **El coste de una ronda:** un lote de lecturas en paralelo, con su profundidad de cola, la latencia del dispositivo y su cola larga. Y el de una query: rondas en serie más el rescore.
- **El tamaño del posting en disco,** con ids, códigos y aristas, su alineación a página y si se lee entero o por rangos.
- **El S del modo disco,** que hay que re-derivar con el presupuesto de I/O. A 0.99, S=2048 son 3.6 veces menos cargas y 3.8 veces menos rondas que S=236, a cambio de 2.3 veces más vectores leídos (Fase C).
- **El rescore:** al bucket de objetos, a un store propio (b') o con el refinado de (c), y cuánto pesa en la cola de latencia frente a las cargas.
- **Qué postings se quedan en caché,** y cómo cambia el modelo con la caché caliente.

### 4. Sidecar

**Hoy el sidecar es el código completo de cada vector, residente en RAM,** indexado por slot. Es lo que puntúa los endpoints sin I/O, y lo que da al routing el código completo en el layout (c).

- **A 10M y 768d son 992 MB,** 104 B por vector. A 1536d serían 200 B por vector, y a 100M de 10 a 20 GB.
- **En memoria, postings y sidecar comparten un solo array de códigos.** En disco el posting lleva sus propios códigos y el sidecar queda como la única copia residente.

Abierto:

- **Si cabe residente a la escala objetivo,** o hace falta un sidecar capado, con lo que eso le quita al routing: con 256 bits para todo, 3.0x cargas a igual recall.
- **Su relación con el layout (c):** el refinado lee el sidecar por cada candidato refinado.
- **Cómo se mantiene con inserciones, deletes y movimientos de posting,** que depende del punto 1.
- **Qué memoria deja el modo disco,** con el sidecar y los centroides residentes, frente a HNSW y a HFresh.
