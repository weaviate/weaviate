# Índice posting-edge, hito 1: qué se puede probar

**Para:** Rodrigo · **Fecha:** 2026-11-20 · **Rama:** `feat/posting-edge-integration`

Es un tipo de índice vectorial nuevo, experimental, con el nombre provisional `postingedge`. Los vectores viven en postings de unos 236, con aristas a vecinos de otros postings; en RAM queda un código de 512 bits por vector, y los postings y los vectores exactos están en disco. Este hito lo deja funcionando dentro de un shard. No está optimizado ni terminado.

## Cómo usarlo

Una clase con este tipo, con todos los parámetros en su valor por defecto:

```json
{
  "class": "Prueba",
  "vectorIndexType": "postingedge",
  "vectorIndexConfig": {
    "distance": "cosine",
    "ef": 20,
    "efConstruction": 160,
    "maxPostingSizeKB": 0,
    "rq": {"rescoreLimit": -1, "cacheMode": "routing"}
  }
}
```

- **`ef`:** el ajuste de calidad de una búsqueda. Se puede cambiar en vivo, igual que `rq.rescoreLimit`.
- **`maxPostingSizeKB: 0` y `rq.rescoreLimit: -1`** son «que elija el índice». Lo demás no se puede cambiar después de crear la clase.
- **No hay flag.** Si el nodo tiene `ALLOWED_VECTOR_INDEX_TYPES`, hay que añadirle `postingedge`.

## Qué funciona

- Importar, con indexación síncrona. **Con `ASYNC_INDEXING` no está probado.**
- Buscar por vector, sin filtro.
- Borrar y actualizar objetos.
- Reiniciar el nodo: el índice vuelve como estaba.
- Varios vectores con nombre en una clase, cada uno con su índice.

Todo ello está probado por un shard con unos miles de vectores sintéticos, y a 1M solo el índice, sin el shard alrededor.

## Qué falla por diseño

Cada una de estas cosas da un error que lo dice. Ninguna devuelve un resultado a medias.

| qué | qué pasa |
|---|---|
| Una búsqueda con filtro (`where`), también híbrida o `Aggregate` con filtro | La query falla: «filtered search … not supported» |
| Una búsqueda por distancia o certeza, sin límite | La query falla: «search by distance … not supported» |
| Un backup de una clase con este índice | El backup falla |
| Otra distancia que no sea `cosine` | La clase no se crea |
| Multi-vector | La clase no se crea |
| Cambiar `rq.cacheMode` en una clase que ya existe | El cambio se rechaza: «rq.cacheMode is immutable» |
| Compresión `pq`, `sq`, `bq`, o `rq` con otros bits | La clase no se crea |

## Con qué te vas a encontrar

- **Hasta 10 000 vectores la búsqueda es exacta,** leyendo todos los vectores en cada query. Al llegar a 10 000 el índice construye sus postings, **dentro del insert que los completa:** esa petición tarda lo que tarde el build, y las búsquedas esperan mientras tanto. Con 10 000 vectores de 1536 dimensiones fueron unos 3 s; con 99 000, 1 min 19 s.
- **Un borrado no libera nada.** El vector se marca, deja de devolverse y sigue en su sitio. Con muchos borrados las búsquedas trabajan más para el mismo recall.
- **Un update gasta sitio:** es un borrado más un insert.
- **Los inserts van de uno en uno** y son lentos: unos 180 por segundo a 1M y 1536 dimensiones en un M1 Max, medido en los experimentos con la máquina sola. Cada insert bloquea las búsquedas un momento, mientras escribe.
- **No hay recuperación tras una caída a mitad de un insert.** Si el proceso muere entre dos peticiones, el índice abre bien. Si muere mientras escribe un insert, el índice puede negarse a abrir, y entonces el shard no carga. No hay reparación automática: hay que borrar la clase y volver a importar.
- **Si una escritura falla a medias** (disco lleno, por ejemplo), el índice se para y devuelve error en cada llamada hasta que el shard se reinicia.
- **El disco:** además de los objetos, el índice guarda su propia copia de los vectores, 6.1 GB por millón a 1536 dimensiones, y unos 0.7 GB por millón en sus buckets.
- **Sin métricas de Prometheus** propias, y el endpoint de debug que reinicia un índice no lo conoce.

## Lo medido a 1M

*dbpedia-openai, 990 000 vectores de 1536 dimensiones, 5 000 queries, k = 10. El índice integrado, crecido desde vacío por inserts, sin el shard alrededor. En conteos, sin cronometrar.*

**Con la base por defecto de 10 000, que es lo que vas a usar tú** (medido el 21 de noviembre): el índice crece 99 veces desde su base, sin refresco de aristas.

| ef | recall | postings leídos por query | con la base de 99 000: recall | postings |
|---|---|---|---|---|
| 20 | 0.9489 | 19.2 | 0.9504 | 19.1 |
| 21 | 0.9509 | 19.7 | 0.9523 | 19.6 |
| 40 | 0.9713 | 29.3 | 0.9713 | 29.0 |
| 80 | 0.9840 | 45.8 | 0.9841 | 45.3 |
| 158 | 0.9904 | 72.2 | 0.9902 | 71.6 |
| 160 | 0.9904 | 72.8 | 0.9903 | 72.3 |

- **Crecer cien veces desde 10 000 casi no cuesta:** el recall a igual ef queda a 0.0015 o menos del de la base de 99 000, que es lo que se separan dos corridas iguales, y cada búsqueda lee entre un 0.5 % y un 1.2 % más de postings.
- **Cobertura completa,** con 4 062 splits por el camino.
- **Cerrado y abierto otra vez a 1M, devuelve lo mismo:** ninguna de las 5 000 queries cambia su respuesta, en ninguno de los seis ef. Abrir tarda 0.3 s, con la máquina ocupada.

**Con la base forzada a 99 000 por un gancho de test,** que es el camino que midieron los experimentos:

| ef | recall | postings leídos por query | en los experimentos, dos corridas: recall | postings |
|---|---|---|---|---|
| 21 | 0.9523 | 19.6 | 0.9504 y 0.9509 | 19.6 y 19.5 |
| 158 | 0.9902 | 71.6 | 0.9900 y 0.9901 | 72.1 y 71.9 |

- **El índice integrado es el de los experimentos:** los mismos postings leídos, los mismos splits (3 673 frente a 3 663 y 3 684) y cobertura completa.
- **El recall a ef 21 no es una diferencia:** cinco corridas del mismo algoritmo en los experimentos dan entre 0.9502 y 0.9516, 0.0014 de una a otra, y el 0.9523 queda 0.0007 por encima de la mayor.

**Tras borrar el 30 % al azar,** con las dos bases:

- **Ningún borrado vuelve en los resultados,** en 30 000 búsquedas con cada base. Con la de 10 000, tampoco tras cerrar y abrir: los 990 000 doc ids están como se dejaron.
- **El recall a igual ef no baja:** sube entre 0.002 y 0.006.
- **Cada búsqueda lee entre un 23 % y un 28 % más de postings,** porque los borrados siguen en su sitio.

**Sin medir a 1M:** nada a través del shard, la latencia, y los inserts por segundo con la máquina sola.

*Estos números son del código del hito 1. La rama lleva ya piezas del hito 2; lo que cambian para quien prueba está en la sección siguiente.*

## Lo que cambia con el hito 2

*Estado de la rama el 22 de noviembre. Cada punto dice si está verificado a 1M o solo por tests.*

- **El formato en disco es otro. Un índice creado con el código del hito 1 se abre y funciona como era,** con búsquedas e inserts, pero no recibe ni la limpieza de borrados ni la reordenación de vectores. Para tener el formato nuevo hay que volver a importar.
  - *Verificado a 1M:* con el formato nuevo el índice es el mismo (0.9515 de recall y 19.7 postings leídos a ef 21; 0.9905 y 72.1 a ef 158), y devuelve lo mismo reabierto.
- **El índice trabaja ahora por su cuenta,** sin que nadie se lo pida: parte en línea con los inserts y los deletes, que tardan algo más mientras hay una pasada en marcha, y parte en una goroutine suya cuando las escrituras paran.
- **Un borrado ya libera sitio.** Cuando los vectores marcados llegan al 3 % del índice, una pasada los saca, repara las aristas que llevaban a ellos y fusiona los postings que quedan pequeños. Hasta entonces siguen marcados, como antes.
  - *Verificado a 1M:* tras diez rondas de borrar el 3 % e insertar otros tantos, el índice queda como uno construido de una vez con los vectores que quedan: el recall a igual ef a 0.0011 o menos, y entre un 1 y un 2 % más de postings leídos.
  - *Verificado a 1M, con el índice arrancando las pasadas* (22 de noviembre): tras borrar el 30 % de 990 000, las búsquedas leen entre un 4 y un 5 % menos de postings que antes de borrar, con algo más de recall. En el hito 1, sin limpieza, leían un 24 a un 28 % más.
  - **Justo después de un borrado grande las búsquedas leen mucho más del disco:** 6 lecturas y 716 KB por posting, frente a 2 y 65 KB, hasta que el store compacta lo que las pasadas reescribieron. Compactado vuelve a 2 lecturas y 63 KB, con las mismas respuestas. *Sin medir cuánto tarda un nodo en compactarlo por su cuenta.*
  - El fichero de vectores no encoge al sacar los borrados: su sitio se recupera cuando el índice dobla.
- **Una caída del proceso a mitad de una escritura ya no deja el índice sin abrir.** Cada escritura pasa antes por un diario, y al abrir el índice vuelve a ser el de después de la última escritura entera. No cubre un corte de corriente.
  - *Verificado por tests,* con copias de los ficheros en cada punto donde el proceso puede morir. *A 1M:* con el diario el índice es el mismo, y el diario escribe unos 10.8 KB por insert además de lo que ya se escribía. *Con el dataset de 1M* (22 de noviembre): el proceso, muerto con `kill -9` a los 15 minutos de un import con ocho goroutines, deja 180 485 vectores; otro proceso lo abre, aplica otra vez 191 registros del diario y el índice está entero y responde. *Sin verificar a escala:* que no falte ningún insert ya respondido, que es lo que comprueban los tests.
- **Las aristas de los vectores más viejos se rehacen cada vez que el índice dobla su tamaño.** Mientras dura una de esas pasadas, cada insert rehace además las aristas de un vector viejo: cuesta más o menos como otro insert.
  - *Verificado a 1M* (22 de noviembre): creciendo de 10 000 a 990 000, el índice lee a 0.99 de recall un 9 % menos de postings que uno construido de una vez (71.2 frente a 78.3), y a 0.95 un 5 % más (19.2 frente a 18.2). Reabierto devuelve lo mismo.
  - **Pero sin rehacer aristas sale igual:** la corrida del hito 1, que no las rehacía, da los mismos números (0.9509 y 19.7 a ef 21, 0.9904 y 72.2 a ef 158, frente a 0.9511 y 19.6, 0.9902 y 72.2). Rehacerlas costó un 42 % más de búsqueda al insertar. Si se queda puesto está por decidir.
- **Varios inserts a la vez:** si llegan por varias goroutines (un import con varios workers), sus búsquedas corren en paralelo y solo sus escrituras van de una en una.
  - *Verificado a 1M* (22 de noviembre), en conteos: con ocho goroutines insertando a la vez el índice es el de una sola (0.9523 y 19.7 a ef 21, 0.9906 y 72.6 a ef 158). *Sin ninguna medida de inserts por segundo.*
  - **Con `ASYNC_INDEXING` hay un test por el shard:** importar, buscar, borrar, actualizar y reiniciar con unos miles de vectores. A escala por el shard no está probado.
- **El grafo de centroides ya se limpia solo,** con los ciclos del nodo que limpian cualquier HNSW. *Verificado a 1M:* tras 4 056 splits quedaban 40 centroides borrados en el grafo, y ninguno tras el borrado del 30 %.
- **El fichero de vectores se reescribe entero cada vez que el índice dobla,** para dejar juntos los vectores de cada posting. Mientras dura pide el doble de disco de los vectores: 12 GB por millón a 1536 dimensiones.
- **Abrir tarda más:** de 2.3 a 2.9 s a 1M con la máquina ocupada, frente a 0.3 s.

- **La base ya no se construye dentro de un insert.** Al llegar a 10 000 vectores el índice la construye de fondo: los inserts siguen entrando y las búsquedas siguen siendo exactas mientras tanto. Los vectores que llegan durante la construcción se buscan de forma exacta hasta que el índice los toma, de uno en uno.
  - *Verificado por tests.* En la corrida a 1M la base se construyó de fondo en 2 s; qué pasa con las búsquedas mientras tanto no se mira allí.

- **`rq.cacheMode: "none"` ya se puede poner al crear la clase.** El índice no tiene entonces ningún código de sus vectores en memoria: quedan unos 12 B por vector en lugar de 84. A cambio cada posting lleva en disco los códigos de los vectores a los que llevan sus aristas, y una búsqueda lee tres filas por posting en lugar de dos, con unas 2.6 veces los bytes (160 KB frente a 62 por posting en una prueba de 30 000 vectores).
  - **El modo no se cambia en una clase que ya existe.**
  - *Verificado por tests,* también por el shard, y con una pasada de 30 000 vectores: el mismo índice abierto con los códigos en memoria responde exactamente lo mismo. Y con el dataset de 1M, el proceso muerto con `kill -9` a los 15 minutos de un import deja un índice que abre entero, con el halo de cada posting completo. *Sin verificar todavía: el índice crecido entero a 1M. Sin ninguna medida de latencia.*

**Lo que sigue igual que en el hito 1:** todo lo de «Qué falla por diseño».
