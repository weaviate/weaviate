# Índice posting+edges: notas para el diseño de la versión en disco

**Estado:** semilla del design doc de disco, y punto de partida de la fase 2 (2026-11-16). Tiene los layouts posibles del posting y de los vectores exactos en disco, el detalle del layout (c), que es lo que dejó decidido el benchmark en memoria (v1.2, Fase B, 2026-11-11), y FastScan. "Open for phase 2" lista lo que el diseño de disco tiene que resolver, sin resolverlo, y "HFresh map" (2026-11-16) dice qué piezas de HFresh sirven. "Mantenimiento: lo que deja v2.0" (2026-11-16) tiene los veredictos del streaming en memoria, como entrada para el diseño de mantenimiento. El resto del diseño de disco no está escrito aquí.

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

- **El mapa en sí:** dónde vive, cuánto ocupa (4 B por vector serían 40 MB a 10M) y qué cuesta su indirección por endpoint frente al `postingOf` de hoy. *Medido el 16: de un 28 a un 31 % más por endpoint con docIDs en orden de inserción, y de un 59 a un 65 % barajados (ver «Aristas por docID: el coste en memoria»).*
- **El sidecar:** si se sigue indexando por slot o pasa al id estable, y qué hace con los huecos que dejan los deletes.
- **Las aristas que apuntan a un vector borrado:** si se quedan como lápida, que solo cuesta trabajo y rango, o se reparan, y quién las encuentra (hoy no hay aristas inversas). *v2.0 no lo resuelve: sus «aristas inversas» son escrituras en las listas de los vecinos de un vector nuevo, no un índice de quién apunta a quién. Y v2.0 no tiene deletes.*
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

*Medido el 16 en memoria (v2.0; el detalle, en «Mantenimiento: lo que deja v2.0»):*

- *los vectores que ya existen tienen que ganar aristas hacia el nuevo: sin aristas inversas el índice no aguanta doblar el corpus;*
- *el split simple (T=360 para S=236, 2-means, sin reasignar vecinos) no necesita reparar aristas si hay aristas inversas;*
- *con aristas inversas, un índice que dobla su corpus por inserciones queda como el lote; más allá de doblarlo no está medido;*
- *para los inserts basta la asignación asistida por el grafo de centroides con ef 8, con cobertura completa y de un 2.3 a un 2.6 % de asignaciones que no van al centroide más cercano.*

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

## HFresh map

*Inventario de solo lectura, 2026-11-16, sobre `adapters/repos/db/vector/hfresh` en la cabeza de esta rama (`1467402870`), que lleva `main`. Las rutas son de ese paquete salvo que se diga otra cosa. El veredicto es para el posting-edge en disco: **reutilizable** (tal cual), **adaptar** (la pieza sirve con cambios) o **eliminar** (posting-edge no la necesita).*

### Componentes

| componente | HFresh hoy | veredicto | para posting-edge |
|---|---|---|---|
| Grafo de centroides | `hnsw.HNSW` en memoria, ef 64, RQ8, persistido por commit log en `<indexID>.hfresh.d`. El centroide float no se guarda: `Get` devuelve la reconstrucción RQ8 (`hnsw.go:62-113`). | adaptar | posting-edge ya busca su entrada en un HNSW de centroides (`centroids.go`), float y L2, sin persistir, con 124 MB de centroides float a 10M. Se aprovecha la persistencia del commit log; queda por ver si la reconstrucción RQ8 basta para la asignación exacta. |
| Store de postings | LSM `SetCollection` con compactación forzada. Clave de 10 B: versión de esquema, postingID y versión del posting. Cada elemento es id (8 B), versión del vector (1 B) y código RQ1 con cabecera de 8 B. Sin cabecera de posting (`store.go:90-158`, `posting.go:24-37`). | adaptar | Un `SetCollection` es un conjunto sin orden, así que las aristas de cada miembro tienen que ir dentro de su elemento: id, código y aristas. Se quedan la clave versionada y la reescritura entera. |
| Reescritura de un posting | `Put` escribe el posting entero bajo la versión siguiente; flush y compactación descartan las claves de versiones viejas, nunca elementos sueltos (`store.go:249-292`, `100-127`). La versión es un `uint8` sin tratamiento de la vuelta a 0 (`store.go:269`). | reutilizable | Es lo que pide un split, un merge o la reparación de las aristas de un posting. La vuelta del `uint8` hay que cerrarla antes de depender de ella. |
| Append | `SetAdd` de un elemento bajo la clave vigente (`store.go:294-313`). | reutilizable | Insert incremental: el elemento nuevo lleva sus aristas. |
| Lectura | `SetRawList`: concatena segmentos y memtables sin deduplicar. Al leer solo se saltan los borrados (`store.go:201-225`, `search.go:127`). | reutilizable | Igual, con la lápida en el mapa vector→posting en lugar de en `VersionMap`. |
| Split | 2-means balanceado sobre los vectores exactos del bucket de objetos (o de FDE). Escribe los dos hijos, inserta sus centroides y borra el viejo, todo con el lock del posting. Después encola la reasignación de vecinos (`split.go:29-196`, `300-449`). | adaptar | Con aristas que guardan un id estable, un split cambia las entradas del mapa de los vectores que mueve, y ninguna arista. Esa actualización es lo que hay que añadir. El 2-means podría trabajar sobre el sidecar en vez de sobre el bucket. |
| Merge | Lo dispara un scan que ve menos vivos que el mínimo. Busca un vecino entre 64 candidatos, funde y borra el pequeño. El centroide del fusionado no se recalcula (`merge.go:21-285`). | adaptar | Lo mismo que el split: actualizar las entradas del mapa de los vectores movidos. |
| Reassign | `RNGSelect` con hasta 4 réplicas por vector, y un bump de versión por CAS que invalida las copias viejas (`reassign.go:28-131`, `rng.go:24-69`). | adaptar | posting-edge asigna cada vector a un solo posting, exacto, y cubre la frontera con aristas, no con réplicas. Sobran la selección RNG y las réplicas. Queda mover un vector a su posting más cercano y cambiar su entrada del mapa. |
| Cola de tareas | Cuatro `DiskQueue` (analyze, split, reassign y merge) con deduplicación en memoria. Después de cada lote escribe los logs del LSM y hace flush (`task_queue.go`). | reutilizable | Tal cual, con sus invariantes de durabilidad (abajo). |
| `PostingMap` | posting → ids de sus vectores, en memoria y en el bucket compartido. El insert solo toca la memoria; lo persisten analyze, split y merge (`posting_map.go`). | adaptar | Es el inverso del que hace falta. posting-edge saca la pertenencia del propio posting. |
| Mapa vector→posting | **No existe.** | (nuevo) | 4 B por vector, persistido. Es la pieza del punto 1 de "Open for phase 2". |
| `PostingSizes` | posting → `uint32`, mantenido como `PostingMap` (`posting_size.go`). | reutilizable | Tal cual. |
| `VersionMap` | vector → 1 B: un contador de 7 bits y un bit de borrado. Detecta las copias viejas que dejan las réplicas y el reassign (`version_map.go`). | eliminar | Con un solo posting por vector, la copia válida es la del posting que dice el mapa vector→posting, y una lápida en el mapa hace de bit de borrado. El CAS del bump pasa a ser un CAS sobre la entrada del mapa. |
| Delete | Pone la lápida en `VersionMap`; postings, mapas y centroides no se tocan. El GC es perezoso, en split y merge (`hfresh.go:230-245`). | adaptar | Además hay aristas que apuntan al borrado: se saltan al puntuar, por la lápida del mapa, y se quitan cuando se reescribe el posting de su source. |
| Update | No llega al índice. El shard da un docID nuevo a un vector que cambia: borra el viejo del índice y añade el nuevo, con un contador monótono (`adapters/repos/db/shard_write_put.go:546-549`, `926-930`). | reutilizable | posting-edge solo ve inserts y deletes. Consecuencia: los docIDs crecen con la historia y dejan huecos, así que un array por docID ocupa 4 B por docID emitido, no por vector vivo. |
| Búsqueda | Centroides a `max(k, searchProbe)`, filtro por ratio de distancia, `MultiGet` y scan RQ1 (`search.go:48-257`). | eliminar | posting-edge tiene su propio bucle, por rondas guiadas por aristas. |
| Rescore | En paralelo, sobre una vista consistente del bucket de objetos (`TempVectorForIDWithViewThunk`), con slices de un pool (`search.go:182-256`, `269-293`). | reutilizable | Es el camino de rescore desde el bucket. Si el layout aísla el rescore, (b') o (c), se adapta. |
| MUVERA | Rescore intermedio contra FDE y MaxSim (`search.go:443-482`, `500-732`). | eliminar | Fuera del alcance de posting-edge v1. |
| Filtros | Allow list de solo pertenencia y orden de centroides determinista (`allow_list.go`, `hnsw.go:155-172`). Por debajo de 5 000 ids, búsqueda plana sobre objetos (`search_flat.go`). | adaptar | posting-edge aún no filtra. El invariante del fix de orden de reclamo vale igual (abajo). |
| Metadata y arranque | Bucket compartido `Replace`: dimensiones, cuantizador, versiones, mapas y la secuencia de ids de posting en bloques de 1 000. Al arrancar: commit log de centroides, colas, metadata y mapas (`index_metadata.go`, `hfresh.go:125-220`). | reutilizable | Con el mapa vector→posting como un prefijo más. |

### La cadena de #12512: dónde vive cada fix y qué invariante protege

- **[#11785](https://github.com/weaviate/weaviate/pull/11785) está en `main`** (squash `022ca976db`, 2026-08-18). Lleva los presupuestos `max(k, …)`, el desempate determinista y el rescore intermedio de MUVERA.
- **[#12512](https://github.com/weaviate/weaviate/pull/12512) sigue abierto y no está en este árbol.** Su re-ruta síncrona (`rerouteLostInsert`, como mucho 3 intentos) vive solo en `origin/hfresh-insert-reroute-lost-split-race`. Aquí, una colocación perdida solo encola un reassign (`insert.go:105-110`), y la ventana de invisibilidad sigue abierta.

| fix | dónde vive | invariante | ¿lo hereda posting-edge? |
|---|---|---|---|
| #12512 (abierto) | `insert.go`, `rerouteLostInsert` | Un insert confirmado está en un posting vivo cuando `Add` vuelve. | Sí, y por construcción. Es el riesgo del punto 1: en HFresh a 10M se midió un 5.9 % de vectores fuera de todo posting. |
| `c5ea8a5e47` | `insert.go:100-111`, `209-216` (`add`, `append`) | Una colocación perdida contra un posting retirado siempre se encola para reassign. | Sí, mientras la re-ruta no sea síncrona. |
| `9a0e663ea9` | `task_queue.go:292-295` (`EnqueueReassign`) | Un push fallido no deja un bit de dedup que bloquee los reassigns siguientes de ese vector. | Sí, con la cola. |
| `70ce8bad08` | `reassign.go:115-127` (`appendReassignReplicas`) | Un reassign que pierde su destino se re-encola, en vez de tragárselo su propio marcador de dedup. | Sí. |
| `63c2351f4f` | `reassign.go:66-70` (`doReassign`) | Ninguna versión se sube, invalidando las copias viejas, sin un destino. | Sí, como regla del mapa: la entrada de un vector movido no cambia antes de que su posting nuevo esté escrito. |
| `de7a8ff03c` | `reassign_all.go:96-116` (`EnqueueReassignAll`) | Antes de reasignar un posting huérfano se crea un destino. | Sí. |
| `f5e79fdfc4` | `split.go:36-49`, `78-128` (`doSplit`) | Un posting de más tamaño conserva su split programado, y un delete concurrente no lo aborta para siempre. | Sí. |
| `0618fa2589` | `allow_list.go:22-132`, `hnsw.go:155-172` | Que un vector permitido sea alcanzable no depende del orden de sondeo. | Sí, cuando posting-edge filtre. |
| `442511fa8c` | `task_queue.go:305-316` (`OnBatchProcessed`) | El commit log de centroides nunca llega a disco antes que los postings a los que apunta. | Sí, si los centroides se persisten por commit log. |
| `fef06768e5` | `adapters/repos/db/queue/queue.go:375-387` | Una tarea procesada no se pierde si falla el flush de después del lote. | Sí, con la cola. |
| `1993f72d9a` | `reassign.go:75-79` | Un cambio de versión concurrente convierte el reassign en un no-op. | Sí, como CAS sobre la entrada del mapa. |

**Sin determinar:**

- qué hace la versión de posting `uint8` al dar la vuelta (255 → 0);
- la capacidad y el desalojo de la caché `otter` de versiones de posting;
- si `Centroids.Get` puede fallar sobre un centroide ya borrado, después de la limpieza de lápidas, en `split.go:301` y `merge.go:236-241`;
- la política de fsync del push de las colas.

## Aristas por docID: el coste en memoria (microbenchmark, 2026-11-16)

*La pregunta del punto 1 de "Open for phase 2": qué cuesta, por endpoint, direccionar las aristas, el sidecar y el mapa vector→posting por docID en lugar de por slot. Solo ns por endpoint; nada está integrado en el índice.*

**Cómo se mide.** Se graban 1 000 búsquedas reales a 10M: wiki-dpr e5, el build por defecto de v1.4, ef=20 (0.95) y ef=160 (0.99), con rescoreLimit 200. Por cada carga se guardan los endpoints que la búsqueda tenía que puntuar. Después se reproduce su puntuación (dedup, scoring por lotes y rango de postings, como `postingSearch.load`) sobre tres ordenaciones del mismo sidecar y del mismo mapa de 4 B por vector:

1. por slot, como hoy;
2. por docID en orden de inserción (el orden de las filas del dataset);
3. por docID barajado, el orden que deja una historia larga de updates, porque cada update da un docID nuevo.

Cada reproducción comprueba que puntúa los endpoints que puntuó la búsqueda, con las mismas estimaciones. Se miden 9 rondas intercaladas, con el orden rotando de ronda en ronda, en un thread, con la máquina sin teclado desde 13 minutos antes.

| ef (recall) | endpoints por query | por slot | por docID, orden de inserción | por docID, barajado |
|---|---|---|---|---|
| 20 (0.9535) | 7 414 | 73.3 ns (71.7–75.0) | 94.0 ns (92.5–100.5): **1.28x** | 116.6 ns (113.7–120.7): 1.59x |
| 160 (0.9909) | 35 691 | 76.2 ns (75.3–76.8) | 100.0 ns (99.5–102.7): **1.31x** | 125.8 ns (124.7–127.5): 1.65x |

**Veredicto: la representación estable cuesta, y se mide.**

- **Por endpoint:** un 28 a 31 % más con los docIDs en orden de inserción, y un 59 a 65 % si ese orden se pierde. Las rondas no se solapan.
- **Por query,** reproducido y no cronometrado de punta a punta:
  - en orden de inserción, unos 153 µs más a 0.95 y unos 850 µs a 0.99. Sobre la query instrumentada de 10M con el ef de centroides 8 (835 y 3 014 µs), son en torno a un 18 % y un 28 %;
  - barajado, unos 320 µs y 1.8 ms.
- **La causa es localidad, no trabajo.** El trabajo por endpoint es idéntico en las tres ordenaciones.
  - Con slots en orden de posting, los endpoints de un source que caen en el mismo posting están juntos en el sidecar y en el mapa.
  - En orden de inserción solo queda lo que comparten docIDs vecinos. En wiki-dpr, los pasajes de un mismo artículo van seguidos.
  - Barajado no queda nada.
- **El orden de inserción depende del corpus.** Lo de wiki-dpr no generaliza: un corpus importado sin orden semántico se acerca al barajado desde el principio.

**Consecuencias para el diseño:**

- El argumento del mapa vector→posting se sostiene en lo que ahorra en mantenimiento: un movimiento cambia una entrada y ninguna arista. Pero en búsqueda no es gratis.
- Opciones abiertas, sin resolver:
  - **(a)** slots como hoy, más un mapa solo para los vectores movidos desde el último relayout;
  - **(b)** ids internos renumerados en orden de posting en cada relayout o compactación, que reescribe las aristas una vez, más un mapa docID→id interno; entre relayouts, un movimiento solo toca el mapa;
  - **(c)** docIDs crudos, aceptando el coste.
  - (a) y (b) recuperan la localidad a cambio de un relayout periódico.
- **Para v2.0:** las métricas sin cronometrar del streaming (recall, cargas, recall de arista) no dependen de la representación. Su frontera cronometrada incluye la referencia convertida a docID, para separar el coste de la representación del del mantenimiento.

Datos en `testdata/bench/phase2/`:

- `wikidpr-10m-endpointkeys.json`: el build;
- `wikidpr-10m-endpointkeys-endpointkeys.json`: los ns por ronda de cada ordenación;
- la cadena, en `scripts/run_endpoint_keys.sh`, que corre `-bench.endpointkeys`.

## Mantenimiento: lo que deja v2.0 (2026-11-16)

*Entrada para el diseño de mantenimiento, sin resolverlo. Las cifras son de `bench-report.md`, «v2.0: streaming en memoria»: dbpedia 1M, lote sobre el 50 % del corpus y el otro 50 % insertado, frente al lote del 100 %. Todo en memoria: son recall, cargas y conteos, y ningún I/O está medido. Las cuentas en bytes son derivadas, con los tamaños de este documento (208 B de id y código y 36 B de aristas por miembro, a 1536d). Lo que es lectura mía va marcado.*

### Los veredictos

| pregunta | veredicto | lo medido, al 100 % frente al lote |
|---|---|---|
| ¿Hacen falta aristas inversas? (H1) | **Sí.** | Sin ellas: −0.0341 de recall a ef 10, +77 % de cargas a 0.95 y +132 % a 0.99. Con ellas: +0.0007 en el peor ef, +2.3 % y −5.7 %. |
| ¿Hay que reparar las aristas de un posting que se parte? (H2) | **No, si hay aristas inversas.** | La celda de aristas inversas hace 2 070 splits sin reparar ninguno y cumple el criterio. |
| ¿Sustituye la reparación por búsqueda a las aristas inversas? | **No.** | Recupera el 60 % del recall perdido a ef 10 y queda en +29.9 % de cargas a 0.95, con 365.5 búsquedas por split. |
| ¿Y la reparación por hermanos, sin búsqueda? | **Sola, no:** no iguala a la de búsqueda. **Sobre las aristas inversas, aporta.** | Sola: −0.0285 y +69.5 %. Con aristas inversas: +0.0067 de recall a ef 10 sobre H1, un 6 % menos de cargas a 0.95 y de un 10 a un 12 % menos a 0.99, con listas un 53 % más largas. |
| ¿Aristas inversas más reparación por búsqueda? | sin medir | La celda no corrió. |

**El mínimo que cumple es: aristas inversas en cada insert, y un split que no toca aristas.** La reparación por hermanos es una mejora opcional encima, que se paga en grado.

### Lo que hace un insert

| paso | medido en memoria | en disco (*lectura mía*) |
|---|---|---|
| asignación | grafo de centroides con ef 8; de un 2.3 a un 2.6 % no va al centroide más cercano, y el criterio se cumple igual | residente, sin I/O |
| búsqueda de aristas, k=16 y ef=320 | de 53 a 60 cargas de posting en 13 a 15 rondas, y 64 candidatos de rescore | las mismas cargas y lecturas que una query a ese ef. Es el coste dominante de un insert |
| escritura del miembro | una | un append al posting propio, el código en el sidecar y la entrada del mapa |
| aristas inversas | lee las listas de 8.97 vecinos y escribe en 4.62 (p95 11), que viven en 3.28 postings distintos (p95 8) | depende de dónde vivan las aristas: la sección siguiente |
| split, uno cada 240 inserts | 2-means, dos centroides nuevos y una lápida; con hermanos, 34 800 distancias exactas y 344 listas cambiadas | la reescritura de los dos hijos, que ya hace el `Put` de HFresh |

- **Un insert carga más postings que una query a 0.99** (de 44 a 53 cargas). En HFresh un insert no lee ningún posting.
- **El ef de inserción no se barrió.** Con 320, el recall de arista al insertar queda entre 0.964 y 0.987. Cuánto se puede bajar, y con él las cargas de un insert, es la primera medida que le falta al diseño.

### Dónde viven las aristas

**Los números:** un insert escribe en las listas de 4.62 vectores de media y de 11 en el p95. Esas listas están en 3.28 postings distintos de media y en 8 en el p95, todos distintos del suyo. El 10.5 % de los inserts no escribe ninguna, y el 20.5 % escribe 8 o más.

**Lo que fija la búsqueda** (*lectura mía de cifras medidas*): a 0.95 una query puntúa unos 490 endpoints por posting cargado, así que sigue las aristas de decenas de sus miembros. Las aristas de un posting tienen que llegar con él, o estar ya en RAM. Un store en disco por docID, con una lectura por lista, queda descartado para la búsqueda.

Quedan tres sitios. Las cuentas son por insert, a 1536d y con postings de 237 miembros (58 KB):

| | (1) en el elemento del posting, reescribiendo el posting | (2) en el posting, con escrituras por elemento | (3) residentes en RAM, con un log |
|---|---|---|---|
| qué es | lo que propone hoy «HFresh map»: id, código y aristas en cada elemento, y `Put` del posting entero | un append por lista cambiada, y la lectura se queda con la última versión de cada miembro | las listas en memoria, como el sidecar; el posting lleva solo id y código |
| escrituras de las aristas inversas | 3.28 postings enteros: 190 KB de media y 460 KB en el p95, frente a los 0.24 KB del append del insert | 4.62 elementos, 1.1 KB si el elemento lleva el código, o 0.3 KB si las aristas van aparte | 4.62 listas en RAM y sus entradas en el log, 0.3 KB |
| lecturas de la comprobación | ninguna nueva: los vecinos salen de postings que la búsqueda del insert ya cargó | ninguna nueva | ninguna: residentes |
| lo que añade a una carga | nada | las versiones viejas, hasta la siguiente reescritura: 4.62 por insert en todo el índice, frente a un miembro nuevo | nada; cada carga pesa 36 B menos por miembro |
| lo que cuesta en RAM | nada | nada | de 36 a 55 B por vector, según el grado, sobre los 200 B del sidecar |
| reparación por hermanos en el split | sin escrituras de más: el split ya reescribe los dos hijos | igual | 344 listas en RAM por split |

- **(1) no aguanta estos números como camino síncrono:** reescribe unas 800 veces lo que escribe el insert.
- **(2) y (3) son las candidatas.** (2) no gasta RAM y paga en lectura hasta que el posting se reescribe. (3) saca las aristas del camino de lectura y de la reescritura de postings, y cuesta RAM a la escala objetivo.
- **La comprobación de la arista inversa necesita la distancia de la peor arista de cada vecino.** En v2.0 se recalcula entera, 10 distancias exactas por lista. *Mío, sin medir:* con la distancia guardada junto a cada arista es una comparación, y no hay que leer ningún vector exacto.

### El split

- **Sin reparar aristas ya cumple,** con aristas inversas. Tiene los pasos del split de HFresh menos su reasignación de vecinos: 2-means sobre los exactos, dos centroides nuevos y una lápida, y el mapa. T=360 deja el tamaño medio en 237 con S=236.
- **La reparación por búsqueda queda descartada:**
  - no llega al criterio, y son 1.51 búsquedas más por insert, cada una con las cargas de un insert;
  - tiene un límite por construcción: con clusters compactos, la búsqueda no llega a la otra mitad (`TestStreamRepairReachesTheOtherHalf`).
- **La reparación por hermanos, si se quiere su mejora, cuesta:**
  - **grado:** de 8.9 a 13.7 aristas de media por vector, 19 B más por miembro. Es un 8 % más de bytes por carga en el layout (a), y un 23 % en el (c) con código de 256 bits;
  - **distancias exactas:** 34 800 por split. Las de entre mitades usan los vectores de los miembros, que el 2-means ya lee. Las de las aristas actuales, de 3 000 a 5 000 por split (*estimado: el contador no las separa*), piden el vector exacto de endpoints de fuera del posting, o la distancia guardada con cada arista;
  - **la búsqueda de cada insert:** puntúa un 11 % más de endpoints a ef 320, y sus aristas inversas escriben un 24 % más (5.73 por insert).
- **Lo que compra:** a igual recall, un 6 % menos de cargas a 0.95 y de un 10 a un 12 % menos de cargas y de rondas a 0.99, con los mismos endpoints a 0.99 y un 4.8 % más a 0.95.
- **Sin medir: los hermanos con la regla del lote.** Quedarse solo con los de la otra mitad que entran entre los 16 más cercanos del vector no subiría el grado. Diría cuánto de la mejora es reparación y cuánto es grado.

### Señales y supuestos

- **El recall de arista contra el corpus actual no sirve como señal de cuándo rehacer un índice.** Ni a nivel de vector ni de posting ordena las celdas por recall de búsqueda: la de reparación por búsqueda lo deja por encima de la de aristas inversas y busca peor.
- **El recall de arista al insertar sí sirve como guardia del constructor:** dice si la búsqueda que crea las aristas funciona.
- **La limpieza de lápidas del grafo de centroides funciona** tras cada tramo de splits (unas 400 lápidas cada 99 000 inserts).
- **La representación por docID cuesta un 15 % de QPS con 8 threads,** de punta a punta y con los docIDs barajados. Es el caso malo del microbenchmark de arriba, medido ahora en la búsqueda entera.
- **El orden natural de dbpedia no prueba la deriva:** queda como el barajado. Un corpus que llegue en orden semántico sigue sin medir.

### Lo que v2.0 no mide

- **Deletes y updates.** No hay ninguno.
- **Más allá de doblar el corpus.** Un solo lote y una sola duplicación, en un dataset y a 1M.
- **Concurrencia.** El harness es secuencial: ninguna búsqueda corre durante un insert ni durante un split.
- **El ef de inserción,** fijo en 320.
- **Aristas inversas con reparación por búsqueda,** y los hermanos con la regla del lote.
- **El QPS de las celdas que cumplen.** Solo se cronometró la principal, que queda en 0.54x–0.64x de la referencia por docID a 0.95.
- **Cualquier I/O.**
