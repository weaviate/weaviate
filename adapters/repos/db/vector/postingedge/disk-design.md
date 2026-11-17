# Índice posting+edges: notas para el diseño de la versión en disco

**Estado:** semilla del design doc de disco, y punto de partida de la fase 2 (2026-11-16). Tiene los layouts posibles del posting y de los vectores exactos en disco, el detalle del layout (c), que es lo que dejó decidido el benchmark en memoria (v1.2, Fase B, 2026-11-11), y FastScan. "Open for phase 2" lista lo que el diseño de disco tiene que resolver, sin resolverlo, y "HFresh map" (2026-11-16) dice qué piezas de HFresh sirven. "Mantenimiento: lo que deja v2.0" (2026-11-16) tiene los veredictos del streaming en memoria, como entrada para el diseño de mantenimiento, y el ef de inserción que deja v2.1. "El halo" (2026-11-17) tiene el modo sin sidecar residente y lo que cuesta, de v3.0. El resto del diseño de disco no está escrito aquí.

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

## El halo: los códigos de los endpoints dentro del posting (v3.0, 2026-11-17)

*De `bench-report.md`, «v3.0: punto de operación del halo»: dbpedia 1M en lote, con S = 236 y con S = 1024. Sobre cada build, la referencia ordena los postings con el código completo de los endpoints, que es lo que hace el sidecar residente, y tres celdas los ordenan con un sketch de 256, 384 y 512 bits. El scan lleva el código completo en todas. Son conteos y un modelo de bytes; ningún I/O está medido. Lo que es lectura mía va marcado.*

**Qué es.** En los cuatro layouts de arriba queda residente el sidecar: el código completo de cada vector, 200 B a 1536d. El halo lo quita. Cada posting lleva un tercer bloque, con el id y un sketch de cada endpoint distinto de las aristas de sus miembros, y la búsqueda ordena los postings sin cargar con lo que traen los que ya cargó. Los centroides y el mapa vector→posting, de 4 B por vector, siguen residentes.

**El modelo de bytes** (*mío*): 208 B por miembro (id y código completo), 4 B por arista, y 4 B de id más el sketch por entrada de halo: 44, 60 y 76 B con 256, 384 y 512 bits. Sin alineación a página ni cabeceras.

| S = 236, frente al sidecar residente | sidecar residente | halo de 256 bits | de 384 bits | de 512 bits |
|---|---|---|---|---|
| cargas a 0.99 | 52.77 | 2.85x | 1.71x | 1.48x |
| cargas a 0.95 | 10.11 | 3.72x | 2.34x | 1.81x |
| rondas a 0.99 | 12.94 | 37.37 | 22.36 | 19.33 |
| un posting en disco, de media | 57.6 KB | 115.6 KB | 136.6 KB | 157.7 KB |
| bytes de posting por query a 0.99 | 3.26 MB | 19.61 MB (6.01x) | 14.07 MB (4.31x) | 14.20 MB (4.35x) |
| bytes de posting por query a 0.95 | 0.63 MB | 5.00 MB (7.89x) | 3.72 MB (5.86x) | 3.33 MB (5.25x) |
| disco, a 1M | 241 MB | 485 MB | 573 MB | 662 MB |
| RAM del sidecar, a 1M | 198 MB | — | — | — |

**El halo es viable, por el criterio del usuario:** alguna celda alcanza 0.99 con 3× o menos las cargas de la referencia. Lo cumplen las seis, de 1.43 a 2.85 veces, todas con rescoreLimit 64 y subiendo solo ef.

- **Lo que cuesta es ancho de banda y disco.** Una query lee al menos 4.3 veces más bytes de posting a 0.99 y 5.3 veces más a 0.95, y el índice ocupa de 2.0 a 2.7 veces en disco.
- **Un posting tiene 1 318 endpoints distintos de media y 2 557 en el p95,** 0.622 por arista. Su halo pesa lo que sus miembros y sus aristas juntos con 256 bits, y 1.7 veces eso con 512.
- **El sketch de cada vector está escrito 5.58 veces,** una por posting con una arista hacia él. Por vector son 246, 335 y 424 B de halo, frente a los 200 B del sidecar: el halo no ahorra bytes, los cambia de RAM a disco.
- **El ancho se paga por los dos lados.** Un sketch más corto pide más cargas, y uno más largo, más bytes por carga. A 0.99 se compensan entre 384 y 512 bits; a 0.95 gana 512.
- **S = 1024 no ayuda:** de 1.43 a 2.64 veces las cargas de su referencia, pero de 18.8 a 25.8 MB por query a 0.99, y su modo con sidecar residente ya lee 5.30 MB.
- **La búsqueda usa una parte de lo que lee:** a 0.99 con 384 bits, 141 551 entradas de halo leídas para 28 174 endpoints puntuados.
- **A igual ef, con sketch se cargan de 1.6 a 3.8 veces más postings,** con algo más de recall desde ef 40. El umbral de parada es el del scan, y las estimaciones de los endpoints son más ruidosas.

*Lectura mía, para el diseño:*

- **Hay dos modos, y el sidecar residente es el barato en I/O.** El halo es para cuando el sidecar no cabe: a 100M y 1536d son de 10 a 20 GB de RAM.
- **El halo iría con S = 236 y 384 bits** si el objetivo es 0.99, y con 512 si es 0.95.
- **El término que crece es el de las rondas,** que en disco van en serie: de 12.9 a entre 19 y 37 a 0.99.
- **La palanca es no leer todo el halo.** Cuatro de cada cinco entradas leídas no se puntúan. Un halo podado, o en un bloque aparte que se lee solo para los sources dentro del margen, está sin medir.
- **La parada pide su propia calibración con sketch.** El margen de 0.06 se calibró con el código completo.
- **El mantenimiento se encarece.** Una arista nueva hacia un vector mete su sketch en el posting del source: las 4.5 aristas inversas de un insert y la lista propia de un refresco tocan halos de otros postings. Sin medir.
- **Abierto:** el sidecar paginado, que guarda cada código una vez y lo lee por páginas, es el otro punto sin RAM. Aquí no está medido; en las simulaciones no era operable por lecturas aleatorias.

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
- *para los inserts basta la asignación asistida por el grafo de centroides con ef 8, con cobertura completa y de un 2.3 a un 2.6 % de asignaciones que no van al centroide más cercano;*
- *la búsqueda de aristas de un insert puede bajar de ef 320 a 80 (v2.1): 21.9 cargas de posting por insert en lugar de unas 57, con el índice dentro del criterio.*

*Medido el 17 en memoria (v2.2; el detalle, en «Crecer diez veces»):*

- *al multiplicar el corpus por diez, el mismo índice cumple en recall a igual ef y en cargas a 0.95, y no a 0.99: carga de un 18 a un 32 % más que el lote en tres corridas, con ef de inserción 160 o con 80;*
- *lo que falla son las aristas, no la partición (diagnóstico de v2.2; el detalle, en «Aristas o partición»): reconstruidas en lote sobre la partición final, el índice carga un 0.8 % menos que el lote a 0.99. El mantenimiento que falta es un refresco de aristas;*
- *ese refresco, hecho con la búsqueda del insert, sirve (v2.3; el detalle, en «El refresco de aristas por búsqueda»): con todos los vectores refrescados el índice carga un 6.3 % más que el lote a 0.99, donde el umbral es un 10 %, a 23.2 cargas de posting por vector refrescado;*
- *cada cuánto hace falta rehacerlo, con aristas inversas solas y el objetivo en 0.99: en algún punto entre ×2 y ×10, sin medir dónde;*
- *las asignaciones que no van al centroide más cercano suben al 3.1 % en el último tramo, con cobertura completa;*
- *el build incremental hace de 0.39 a 0.55 veces las distancias por código del lote y 0.10 veces las exactas. Sin la asignación n·k, 1.6 veces las exactas.*

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
  - *Medido el 17 (v3.0; el detalle, en «El halo»):* con el scan a código completo y solo el ruteo capado, son 2.85, 1.71 y 1.48 veces las cargas a 0.99 con 256, 384 y 512 bits. Sin sidecar, con esos sketches dentro de los postings, una query lee de 4.3 a 6.0 veces más bytes.
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
| ¿Aguantan las aristas inversas, sin reparar splits, multiplicar el corpus por diez? (v2.2) | **A 0.95 sí; a 0.99 no.** | Lote sobre el 10 %: −0.0027 y −0.0037 de recall en el peor ef, +6.3 y +9.2 % de cargas a 0.95, y +19.6 y +17.9 % a 0.99, con ef de inserción 160 y 80. La celda de ef 80, repetida: +32.0 % a 0.99. |
| ¿Lo que falla al crecer diez veces son las aristas o la partición? (diagnóstico de v2.2) | **Las aristas.** | Con las aristas reconstruidas en lote sobre la partición que dejan los inserts: −0.8 % de cargas a 0.99 y +3.0 % a 0.95 frente al lote, y de +0.0008 a +0.0034 de recall a igual ef. El 17.3 % de los vectores no está en el posting de su centroide más cercano. |
| ¿Lo arregla refrescar las aristas con la búsqueda del insert? (v2.3) | **Sí, por el criterio.** | Con todos los vectores refrescados: +6.3 % de cargas a 0.99 y +1.7 % a 0.95 frente al lote, y de +0.0001 a +0.0037 de recall a igual ef. Antes del refresco, +22.7 % a 0.99. Un refresco son 23.2 cargas de posting por vector. |

**El mínimo que cumple es: aristas inversas en cada insert, y un split que no toca aristas.** La reparación por hermanos es una mejora opcional encima, que se paga en grado.

*v2.2, del 17:* ese mínimo vale al doblar el corpus. Al multiplicarlo por diez deja de cumplir a 0.99 («Crecer diez veces», abajo). Lo que le falta es mantener las aristas, no la partición («Aristas o partición», abajo).

*v2.3, del 17:* con un refresco de aristas por búsqueda encima, ese mínimo vuelve a cumplir a 0.99 tras multiplicar el corpus por diez («El refresco de aristas por búsqueda», abajo).

### Lo que hace un insert

| paso | medido en memoria | en disco (*lectura mía*) |
|---|---|---|
| asignación | grafo de centroides con ef 8; de un 2.3 a un 2.6 % no va al centroide más cercano, y el criterio se cumple igual | residente, sin I/O |
| búsqueda de aristas, k=16 | con ef 320, unas 57 cargas de posting en 14 rondas; con ef 80 (v2.1), 21.9 en 5.2 rondas. Siempre 64 candidatos de rescore | las mismas cargas y lecturas que una query a ese ef. Es el coste dominante de un insert |
| escritura del miembro | una | un append al posting propio, el código en el sidecar y la entrada del mapa |
| aristas inversas | lee las listas de 8.97 vecinos y escribe en 4.62 (p95 11), que viven en 3.28 postings distintos (p95 8) | depende de dónde vivan las aristas: la sección siguiente |
| split, uno cada 240 inserts | 2-means, dos centroides nuevos y una lápida; con hermanos, 34 800 distancias exactas y 344 listas cambiadas | la reescritura de los dos hijos, que ya hace el `Put` de HFresh |

- **Con ef 320, un insert carga más postings que una query a 0.99** (de 44 a 53 cargas). En HFresh un insert no lee ningún posting.
- **El ef de inserción se barrió en v2.1,** en la sección siguiente. Con 80, un insert carga 21.9 postings, el 38 % de lo que carga una query a 0.99 en ese índice.

### El ef de inserción (v2.1, 2026-11-16)

*De `bench-report.md`, «v2.1: el ef de inserción, sobre H1»: la celda de aristas inversas con ef de inserción 160, 80, 40 y 20, frente al lote del 100 % y a las dos corridas con ef 320. Las cargas por insert son el conteo directo de su búsqueda; las de ef 320 no se contaron y salen de las queries a ese ef, que en las otras celdas dan el conteo con un 0.3 % de error.*

| ef de inserción | cargas de posting por insert: media (p95) | rondas | recall de arista al insertar | recall frente al lote, peor ef | cargas por query a 0.95, frente al lote | a 0.99 | cumple el criterio |
|---|---|---|---|---|---|---|---|
| 320 | unas 56.6 | unas 13.9 | 0.985 | +0.0007 | +2.3 % | −5.7 % | sí |
| 160 | 34.6 (69) | 8.4 | 0.977 | −0.0001 | +3.0 % | −3.3 % | sí |
| 80 | 21.9 (45) | 5.2 | 0.963 | −0.0024 | +6.2 % | +9.8 % | sí; a 0.99, por 0.2 puntos |
| 40 | 14.4 (29) | 3.3 | 0.942 | −0.0057 | +14.4 % | +23.0 % | no |
| 20, hasta el 80 % | 9.9 (21) | 2.2 | de 0.911 a 0.899 | sin medir | sin medir | sin medir | no: lo corta la guardia del constructor |

**El elegido por la regla pre-registrada es 80:** el menor ef que deja el recall a 0.005 o menos del lote, las cargas a 0.95 un 15 % o menos por encima y las de 0.99 un 10 % o menos.

- **La condición de 0.99 la cumple por 0.2 puntos, con un suelo de ruido del 3.0 %.** No se distingue del ruido. El menor ef que cumple las tres con margen es 160.
- **Lo que cuesta, frente a ef 320:** un 4 % más de cargas por query a 0.95 y de un 16 a un 20 % más a 0.99, con dos rondas más a 0.99. Cumple frente al lote porque con ef 320 el índice mantenido quedaba de un 6 a un 9 % por debajo del lote a 0.99.
- **No cambia lo que escribe un insert:** 4.54 aristas inversas de media y 11 en el p95, en 3.19 postings distintos y 8 en el p95. Las cuentas de «Dónde viven las aristas» valen igual.
- **Las cargas por insert crecen un 7 % al doblar el corpus.**

**Qué lee un insert,** con la cuenta de «Qué lee cada layout por query»: 208 B de id y código por vector escaneado, más 36 B de aristas por miembro.

| ef de inserción | cargas | rondas | vectores escaneados | (a) y (b'): bytes de posting | lecturas de rescore | listas de vecinos leídas para las aristas inversas |
|---|---|---|---|---|---|---|
| 320 | unas 56.6 | unas 13.9 | unos 14 100 | 2.9 MB | 64 | 8.97 |
| 160 | 34.6 | 8.4 | 8 642 | 1.8 MB | 64 | 8.94 |
| 80 | 21.9 | 5.2 | 5 474 | 1.1 MB | 64 | 8.91 |
| 40 | 14.4 | 3.3 | 3 606 | 0.75 MB | 64 | 8.83 |

- **El ef solo baja las cargas.** Los 64 candidatos de rescore y la comprobación de las aristas inversas, con unas 89 distancias exactas, son los mismos con cualquier ef.
- *Mío:* en (a) y (b') eso deja 64 lecturas de vector exacto por insert, que con ef 80 son tres veces sus cargas de posting. La comprobación de las aristas inversas no lee nada si la distancia va guardada con cada arista.

*Lectura mía, para el diseño:*

- **El ef de inserción cambia lecturas del insert por lecturas de la query.** De 160 a 80, cada insert se ahorra 12.8 cargas, y cada query carga 0.32 más a 0.95 y 6.9 más a 0.99. Un índice que sirve más de dos queries a 0.99 por insert lee más en total con 80 que con 160. A 0.95 hacen falta cuarenta.
- **De 320 a 160 casi no hay nada que pagar:** 22 cargas menos por insert, por 0.1 más por query a 0.95 y de 1.3 a 2.8 más a 0.99.
- **El default que sale de la regla es 80, y 160 si el objetivo de recall es 0.99.** Es un parámetro del índice, no una constante.
  - *Decisión del usuario del 17:* queda así, como parámetro del índice, y la celda de ef 80 no se repite.
- **La guardia del constructor en 0.90 no avisa de dónde se rompe el criterio:** ef 40 la pasa con 0.94 y no cumple. Con aristas inversas, la frontera está entre 0.94 y 0.96 de recall de arista al insertar.

**Sin medir:** un ef intermedio entre 80 y 160. La celda de ef 80 no se repite, por decisión del usuario del 17. Más allá de una duplicación está medido en v2.2, en la sección siguiente: al multiplicar el corpus por diez, ni ef 80 ni ef 160 cumplen a 0.99.

### Crecer diez veces (v2.2, 2026-11-17)

*De `bench-report.md`, «v2.2: crecer desde el 10 %, sobre H1»: el lote sobre el 10 % del corpus y el 90 % insertado con aristas inversas y sin reparar ningún split, con ef de inserción 160 y 80, frente al lote del 100 %. Las celdas de v2.1 con el mismo ef parten del 50 %.*

| al 100 %, frente al lote | ef 160, corpus ×10 | ef 80, corpus ×10 | ef 160, corpus ×2 (v2.1) | ef 80, corpus ×2 (v2.1) |
|---|---|---|---|---|
| recall a igual ef, peor ef | −0.0027 | −0.0037 | −0.0001 | −0.0024 |
| cargas por query a 0.95 | +6.3 % | +9.2 % | +3.0 % | +6.2 % |
| cargas por query a 0.99 | +19.6 % | +17.9 % | −3.3 % | +9.8 % |
| cumple el criterio | no | no | sí | sí; a 0.99, por 0.2 puntos |

**El mínimo de v2.0 (aristas inversas en cada insert y un split que no toca aristas) aguanta doblar el corpus y no aguanta multiplicarlo por diez, si el índice sirve queries a 0.99.** A 0.95 sigue dentro del criterio.

- **El ef de inserción no es la palanca.** A 0.99 las dos celdas cargan lo mismo: 63.1 y 62.2 postings, a un 1.4 % una de otra, con un suelo de ruido del 3.0 %. Lo que ef 160 ganaba sobre ef 80 al doblar el corpus, aquí se pierde.
  - *Del diagnóstico:* la celda de ef 80, repetida, carga 69.7. A 0.99 dos corridas iguales se separan un 12.0 %, y el suelo del 3.0 % no vale para esta medida. La conclusión se queda igual.
- **Lo que tiene el índice crecido, con cualquiera de los dos ef:**
  - **a igual ef de búsqueda,** de 0.0005 a 0.0015 menos de recall de ef 160 en adelante, y de un 6 a un 7 % más de cargas. Al doblar el corpus eran de un 3 a un 4 % más;
  - **el grado fijado al entrar:** una arista inversa ocupa el sitio de la peor arista de la lista, y el split no toca aristas. Los vectores del lote del 10 % se quedan con 7.63 aristas y los del último tramo entran con 9.47. El lote del 100 % tiene 8.98 de media;
  - **más asignaciones no-nearest:** suben del 1.6 al 3.1 % a lo largo de la corrida, de 0.5 a 0.6 puntos por encima de v2.1 al mismo tamaño de corpus. La cobertura sigue completa.
- **Lo que hace un insert es función del tamaño del corpus.** Las cuentas de «Lo que hace un insert» y de «Dónde viven las aristas» valen igual.
  - En el tramo del 80 al 100 %, un insert carga 36.8 postings con ef 160 y 23.1 con ef 80. Escribe 4.79 y 4.73 aristas inversas (p95 11), en 3.46 y 3.40 postings distintos (p95 8). Son un 3 % más de cargas y un 1 % más de escrituras que en v2.1.
  - Las cargas por insert crecen un 24 % mientras el corpus se multiplica por seis, entre la mitad del primer tramo y la del último.

**El trabajo del build,** en millones de operaciones:

| | lote del 100 % | incremental, ef 160 | incremental, ef 80 |
|---|---|---|---|
| distancias por código | 51 008 | 28 266 (0.55x) | 19 894 (0.39x) |
| distancias exactas | 4 411.6 | 457.2 (0.10x) | 451.5 (0.10x) |
| exactas, sin la asignación n·k de ningún lote | 258.6 | 415.6 (1.61x) | 409.9 (1.59x) |
| cargas de posting | 253.4, visitas | 55.8 | 44.5 |

- **El 94 % de las exactas del lote es la asignación n·k.** Un insert asigna por el grafo de centroides con unas 125 distancias, frente a las 4 195 de un vector del lote.
- **Un insert calcula 445 distancias exactas con ef 160:** 249 en dos búsquedas iguales en el grafo de centroides, 84 en comprobar las aristas inversas, 64 de rescore, 32 de su parte de los splits y 15 de la limpieza de lápidas.
- **Las cargas del lote son visitas y las de los inserts, lecturas.** Su ratio no se sostiene; el de las distancias, sí.

*Lectura mía, para el diseño:*

- **Con aristas inversas solas, el índice necesita algo más para crecer con el objetivo de 0.99:** un rebuild cada cierto crecimiento, o un mantenimiento que rehaga aristas. El punto en que se rompe está entre ×2 y ×10, sin medir.
- **Las candidatas que ya están en este documento son las que reparan el split:** los hermanos sobre aristas inversas y su variante con la regla del lote. Siguen sin medir en este régimen, por decisión del usuario.
- **Falta saber qué arreglar.** v2.2 no separa cuánto del déficit es de las aristas (viejas, y con el grado de cuando entraron) y cuánto de la partición (todos los postings salen de un 2-means). Lo separa reconstruir en lote las aristas sobre la partición final de una celda, sin cronometrar.
  - *Medido el mismo día:* es de las aristas. Está en la sección siguiente.
- **La búsqueda en el grafo de centroides se puede hacer una vez por insert.** Hoy el harness la hace dos, con la misma query: es el 28 % de las distancias exactas de un insert.
  - *Hecho el 17, después del diagnóstico:* el insert pregunta una vez, y su búsqueda de aristas entra por el posting que le da el grafo. Quedan unas 320 distancias exactas por insert, de 445 con ef 160. Sin volver a medir a 1M.
- **La comparación de trabajo sirve para dimensionar un insert, no para sustituir el lote:** el incremental cuesta menos y deja un índice que no es el del lote.

### Aristas o partición (diagnóstico de v2.2, 2026-11-17)

*De `bench-report.md`, «Diagnóstico: aristas o partición»: la celda de ef 80 de v2.2, repetida. Al 100 %, las aristas de todos los vectores se reconstruyen con el constructor del lote sobre la partición que dejan los inserts, sin tocarla. Después, además, se rehace en lote el grafo de centroides.*

| frente al lote del 100 % | antes de reconstruir | aristas reconstruidas | y el grafo de centroides |
|---|---|---|---|
| cargas por query a 0.99 | +32.0 % | −0.8 % | −0.8 % |
| cargas por query a 0.95 | +10.4 % | +3.0 % | +2.4 % |
| recall a igual ef | de −0.0031 a −0.0007 | de +0.0008 a +0.0034 | de +0.0008 a +0.0037 |
| cargas por query a igual ef | de +5.7 a +6.9 % | de +5.4 a +7.8 % | de +5.6 a +7.7 % |
| aristas por vector, con 8.98 en el lote | 8.85 | 9.79 | 9.79 |

**El déficit a 0.99 es de las aristas.** La regla, del usuario y escrita antes de correr: si las cargas a 0.99 vuelven a +10 % o menos frente al lote, es de las aristas, y lo siguiente es un refresco de aristas en segundo plano.

- **La partición que dejan los splits, sin ningún reassign, busca como la del lote si las aristas están al día.** Son 3 674 splits, y al final el 17.3 % de los vectores (171 128) no está en el posting de su centroide más cercano.
- **Lo que cuesta esa partición, con las aristas al día:**
  - un 3.0 % más de cargas a 0.95, y de un 5 a un 8 % más a igual ef, que a 0.99 compensa el recall;
  - un 9 % más de aristas con la misma regla: 9.79 por vector frente a 8.98, unos 3 B más por miembro;
  - un 3.7 % más de endpoints a 0.99.
- **El grafo de centroides construido por inserts no pesa:** rehacerlo en lote no cambia las cargas a 0.99.
- **Las aristas que faltan son las de los vectores viejos:**

| aristas por vector | lote del 10 % | 10 → 20 % | 20 → 40 % | 40 → 60 % | 60 → 80 % | 80 → 100 % |
|---|---|---|---|---|---|---|
| antes de reconstruir | 7.63 | 8.07 | 8.66 | 9.04 | 9.27 | 9.44 |
| reconstruidas | 10.26 | 10.13 | 9.85 | 9.71 | 9.61 | 9.57 |

- **Contra el corpus actual,** un vector del lote del 10 % tiene el 49 % de las aristas que le da la regla del lote, y uno insertado, el 71 %. Reconstruidas, el 97 % los dos.
- **Las cargas a 0.99 no sirven de señal fina en este régimen:** dos corridas iguales de la celda se separan un 12.0 %, con el recall a igual ef a 0.0010 como mucho. El índice crecido cruza 0.99 entre ef 320 y ef 640, donde 0.001 de recall son de 9 a 12 cargas.

*Lectura mía, para el diseño:*

- **El mantenimiento que falta es de aristas.** El reassign tras el split de HFresh no entra en el mínimo por este criterio. Lo que podría comprar está acotado por lo que cuesta la partición, arriba, y sigue sin medir.
  - Mover un vector de posting sigue haciendo falta donde lo pida otra cosa: un posting retirado, o un merge.
- **Reconstruir en lote no es el refresco.** Cuesta 1.23 veces las distancias por código del lote entero y su misma asignación n·k: 63 625 distancias por código y 4 094 exactas por vector.
- **Un refresco por búsqueda costaría lo que la búsqueda de un insert:** 21.5 cargas de posting, 15 709 distancias por código y 64 de rescore por vector, con ef 80 a 1M. Es la cuarta parte de la regla del lote por código, y no tiene asignación n·k.
  - Sin medir si recupera lo mismo: la búsqueda de un insert encuentra el 96.2 % de sus aristas, y la regla del lote, el 97.0 %.
  - *Medido en v2.3:* cuesta 23.2 cargas y 18 503 distancias por código, y cierra el 72 % de la distancia al lote a 0.99, donde la regla del lote, en la misma corrida, cierra el 126 %.
- **Lo que escribe es la lista del vector refrescado,** en su posting. Con los sitios (2) o (3) de «Dónde viven las aristas» es un elemento o una lista en RAM. Con (1), el posting entero.
- **El orden lo da la edad de las aristas.** Hace falta saber, por vector o por posting, con qué tamaño de corpus se calcularon.
- **Abierto, y es lo siguiente por medir:**
  - si refrescar por búsqueda al final recupera lo que la reconstrucción en lote (*medido en v2.3, abajo: no todo, y lo bastante para el criterio*);
  - cada cuánto: el déficit aparece entre ×2 y ×10 de crecimiento, sin medir dónde;
  - si el refresco escribe también aristas inversas, y si basta con refrescar a los miembros de un posting cuando se parte.

### El refresco de aristas por búsqueda (v2.3, 2026-11-17)

*De `bench-report.md`, «v2.3: refresco de aristas con la búsqueda del insert»: la celda de ef 80 de v2.2 otra vez. Al 100 %, cada vector repite el paso de aristas de su insert: la búsqueda con ef 80, entrando por su posting, su lista nueva y las escrituras inversas. Va en tres etapas por edad, sobre un índice y en una corrida. Al final, informativa, la reconstrucción en lote del diagnóstico.*

| frente al lote del 100 % | antes | refrescados los del lote del 10 % | hasta el 40 % | todos | aristas reconstruidas en lote |
|---|---|---|---|---|---|
| cargas por query a 0.99 | +22.7 % | +19.1 % | +11.9 % | +6.3 % | −5.9 % |
| cargas por query a 0.95 | +9.0 % | +6.4 % | +4.0 % | +1.7 % | +1.0 % |
| recall a igual ef | de −0.0032 a −0.0003 | de −0.0022 a −0.0004 | de −0.0009 a +0.0010 | de +0.0001 a +0.0037 | de +0.0010 a +0.0047 |
| cargas por query a igual ef | de +4.8 a +6.8 % | de +4.2 a +6.9 % | de +4.5 a +7.1 % | de +4.5 a +7.2 % | de +5.2 a +7.5 % |
| aristas por vector, con 8.98 en el lote | 8.85 | 9.11 | 9.55 | 9.77 | 9.77 |

**El refresco por búsqueda sirve.** El criterio, del usuario y escrito antes de correr: con todo refrescado, las cargas a 0.99 a +10 % o menos del lote.

- **Cumple con 3.7 puntos de margen, en una corrida.** El índice refrescado cruza 0.99 entre ef 160 y ef 320, como el lote; ahí el suelo de ruido de v2.0 es del 3.0 %.
- **Lo que deja sin cerrar no es de las aristas.** El recall a igual ef queda en el del lote (+0.0001 en ef 160 y en ef 320), y las cargas a igual ef siguen un 7 % por encima: es lo que cuesta la partición.
- **Rinde más cuanto más viejo es el vector, y hace falta en todos:**

| lo que bajan las cargas a 0.99 | los del lote del 10 % | los que entraron del 10 al 40 % | los que entraron del 40 al 100 % |
|---|---|---|---|
| vectores | 99 000 | 297 000 | 594 000 |
| parte de la recuperación | 22 % | 44 % | 34 % |
| por 100 000 vectores refrescados | 1.89 cargas | 1.29 | 0.50 |
| listas que cambian | 96.9 % | 95.7 % | 78.0 % |

- **Con el 40 % más viejo refrescado, el índice queda en +11.9 %,** a 1.9 puntos del umbral. No se separa de cumplir, y no cumple.
- **Qué lee y qué escribe un refresco,** junto a un insert de la misma corrida:

| por vector | un refresco | un insert | la regla del lote |
|---|---|---|---|
| cargas de posting | 23.2 | 21.5 (23.1 en el último tramo) | 256 postings sondeados |
| distancias por código | 18 503 | 15 717 (17 841) | 63 900 |
| distancias exactas | 112: 64 de rescore y 48 de las inversas | 324.5 | 64 de rescore y 4 099 de la asignación n·k |
| listas propias escritas | 0.85 | 1 | 1 |
| aristas inversas escritas | 0.21 | 4.53 | ninguna |
| postings distintos con una lista de vecino escrita: media, p95 | 0.18, 1 | 3.19, 8 | ninguno |

- **Refrescar el corpus entero son 23.0 millones de cargas de posting y 18 318 millones de distancias por código:** 1.31 veces las de las búsquedas de todos sus inserts, y 0.29 veces las de reconstruir en lote.
- **La regla del lote da más:** en la misma corrida y sobre la misma partición, deja el índice un 5.9 % por debajo del lote a 0.99, con 3.45 veces las distancias por código.
  - Sus listas tienen menos vecinos exactos que las del refresco (0.970 frente a 0.977 de recall de arista) y el mismo grado, y dan de 0.0007 a 0.0017 más de recall a cada ef. Sin investigar por qué.

*Lectura mía, para el diseño:*

- **El mínimo para crecer diez veces queda en tres piezas:** aristas inversas en cada insert, un split que no toca aristas, y un refresco de aristas por búsqueda. Sin reassign y sin la regla del lote.
- **El refresco no pide un camino nuevo.** Es el paso de aristas del insert, entrando por el posting del vector en lugar de preguntar al grafo de centroides.
- **En disco es casi todo lectura.** Lee 23 postings por vector y escribe su lista, en su posting. El 85 % de los refrescos no escribe en ninguna lista ajena, el 13 % escribe en las de un posting más, y el 2 %, en las de dos o más.
  - Con el sitio (1) de «Dónde viven las aristas», la lista propia es el posting entero. Con (2) o (3), un elemento o una lista en RAM.
- **El coste se puede dimensionar como una pasada de inserts.** Un refresco completo por cada ×10 de crecimiento deja el mantenimiento en 2.3 veces las distancias por código de los inserts solos. Es una cuenta sobre esta celda, no una medida de un refresco repartido.
- **El orden lo da la edad,** y el refresco tiene que acabar la vuelta: los más nuevos dan menos por vector, y son un tercio de la recuperación.
- **Las escrituras inversas del refresco están por justificar.** Son 0.21 por vector y cuestan 48 distancias exactas y la lectura de 4.83 listas. Sin medir qué pasa sin ellas.
- **Abierto, y sin medir:**
  - un refresco repartido entre los inserts, y cada cuánto: aquí va todo al final;
  - el orden contrario, para separar lo que da una cohorte de lo que da su turno;
  - por qué las listas de la regla del lote buscan mejor, y si un refresco con más ef llega a ellas;
  - la repetición de la corrida, y lo que recuperaría un reassign sobre el índice refrescado.

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
  - En v2.2 son cinco limpiezas por celda, de 411 a 831 lápidas cada una, con cobertura completa. Cuestan 15 distancias exactas por insert.
- **La representación por docID cuesta un 15 % de QPS con 8 threads,** de punta a punta y con los docIDs barajados. Es el caso malo del microbenchmark de arriba, medido ahora en la búsqueda entera.
- **El orden natural de dbpedia no prueba la deriva:** queda como el barajado. Un corpus que llegue en orden semántico sigue sin medir.

### Lo que v2.0 no mide

- **Deletes y updates.** No hay ninguno.
- **Más allá de doblar el corpus.** Un solo lote y una sola duplicación, en un dataset y a 1M. *Medido en v2.2: «Crecer diez veces», arriba.*
- **Concurrencia.** El harness es secuencial: ninguna búsqueda corre durante un insert ni durante un split.
- **El ef de inserción,** fijo en 320. *Medido en v2.1: «El ef de inserción», arriba.*
- **Aristas inversas con reparación por búsqueda,** y los hermanos con la regla del lote.
  - *Decisión del usuario del 16:* la celda de aristas inversas con hermanos no entra en el barrido del ef, porque su aporte va mezclado con un grado un 53 % mayor. Queda abierta junto con la variante de la regla del lote, sin medir ahora.
- **El QPS de las celdas que cumplen.** Solo se cronometró la principal, que queda en 0.54x–0.64x de la referencia por docID a 0.95.
- **Cualquier I/O.**
