# Índice posting-edge: qué se puede probar (hitos 1 y 2, y la búsqueda con filtro)

**Para:** Rodrigo · **Fecha:** 2026-11-20, al día el 2026-11-26 · **Rama:** `feat/posting-edge-integration`

**Qué es.** Un tipo de índice vectorial nuevo, experimental, con el nombre provisional `postingedge`.
- Los vectores viven en postings de unos 236, con aristas a vecinos de otros postings.
- En RAM queda un código de 512 bits por vector, o ninguno con `rq.cacheMode: "none"`. Los postings y los vectores exactos están en disco.
- Funciona dentro de un shard: los hitos 1 y 2, más la búsqueda con filtro. No está optimizado ni terminado.

**Las PRs.**
- [#14041](https://github.com/weaviate/weaviate/pull/14041) es el núcleo, en `feat/posting-edge-integration-m2`, contra `main`.
- [#14023](https://github.com/weaviate/weaviate/pull/14023) es la búsqueda con filtro, en esta rama.
- El backup ([#14013](https://github.com/weaviate/weaviate/pull/14013)) **no está en esta rama.**

## Cómo arrancar

Desde la rama `feat/posting-edge-integration`. Son los comandos del repositorio; *sin probar por mí en esta rama*.

- **En local:**
  1. `./tools/dev/restart_dev_environment.sh`, que levanta las dependencias con docker compose.
  2. `make local`, que deja un nodo en `http://localhost:8080`, con los datos en `./data-weaviate-0`.
- **Con Docker:**
  1. `make weaviate-image WEAVIATE_IMAGE=weaviate-postingedge:local`.
  2. `docker run -p 8080:8080 -p 50051:50051 -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true -e PERSISTENCE_DATA_PATH=/var/lib/weaviate -e DEFAULT_VECTORIZER_MODULE=none -e CLUSTER_HOSTNAME=node1 weaviate-postingedge:local`
- **No hay flag.** Si el nodo tiene `ALLOWED_VECTOR_INDEX_TYPES`, hay que añadirle `postingedge`.
- **Trae tus vectores:** el índice no vectoriza; `vectorizer: "none"` en la clase.

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

| parámetro | qué es | por defecto | ¿se cambia en una clase que ya existe? |
|---|---|---|---|
| `distance` | solo `cosine` en esta versión | `cosine` | no |
| `ef` | el ajuste de calidad de una búsqueda | 20 | sí, en vivo |
| `efConstruction` | cuánto busca un insert | 160 | no |
| `maxPostingSizeKB` | el tamaño de un posting; 0 es «que elija el índice» (unos 236 vectores) | 0 | no |
| `rq.rescoreLimit` | cuántos vectores exactos rescora una búsqueda; −1 es «que elija el índice» | −1 | sí, en vivo |
| `rq.cacheMode` | `routing`, los códigos en RAM, o `none`, ninguno | `routing` | no |

`rq` va siempre encendido, con 1 bit y centrado; no hace falta ponerlo.

## Qué funciona

- **Importar,** con indexación síncrona y con `ASYNC_INDEXING`. Esta última tiene un test por el shard, sin probar a escala.
- **Buscar por vector,** sin filtro y con filtro (`where`, también en las híbridas y en `Aggregate`), en los dos modos de `rq.cacheMode`.
- **Borrar y actualizar objetos,** con limpieza de los borrados (hito 2, abajo).
- **Reiniciar el nodo:** el índice vuelve como estaba. También tras una caída del proceso a mitad de una escritura.
- **Varios vectores con nombre** en una clase, cada uno con su índice.

Todo ello está probado por un shard con unos miles de vectores sintéticos, y a 1M solo el índice, sin el shard alrededor.

## Qué falla por diseño

Cada una de estas cosas da un error que lo dice. Ninguna devuelve un resultado a medias. Los mensajes son los del código.

| qué | qué pasa | el error |
|---|---|---|
| Una búsqueda por distancia o certeza, sin límite, con filtro o sin él | La query falla | «search by distance: not supported by this version of the posting-edge index» |
| Un backup de una clase con este índice | El backup falla | «backup: not supported by this version of the posting-edge index» |
| Otra distancia que no sea `cosine` | La clase no se crea | «unsupported distance "l2-squared": the posting-edge index only supports "cosine" in this version» |
| Multi-vector | La clase no se crea | «multi-vectors are not supported by the posting-edge index» |
| Compresión `pq`, `sq` o `bq` | La clase no se crea | «pq is not supported for the posting-edge index (only rq with 1 bit is)» |
| `rq` con otros bits | La clase no se crea | «the posting-edge index only supports rq with 1 bit, got 8» |
| `rq.enabled: false` | La clase no se crea | «the posting-edge index only works with rq: rq.enabled cannot be false» |
| `rq.centering: false` | La clase no se crea | «the codes of the posting-edge index are always centered: rq.centering cannot be false» |
| Cambiar `distance`, `efConstruction`, `maxPostingSizeKB` o `rq.cacheMode` en una clase que ya existe | El cambio se rechaza | «rq.cacheMode is immutable: attempted change from "routing" to "none"», y lo mismo con el nombre de los otros |

Un error de la configuración de la clase llega envuelto en «invalid posting-edge config: …».

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
- **Las aristas de los vectores más viejos ya no se rehacen al doblar el índice: viene apagado** (decisión del 22 de noviembre). El código sigue, y no hay forma de encenderlo desde una clase. Encendido, cada insert rehace además las aristas de un vector viejo mientras dura una pasada.
  - *Verificado a 1M* (22 de noviembre): creciendo de 10 000 a 990 000, el índice lee a 0.99 de recall un 9 % menos de postings que uno construido de una vez (71.2 frente a 78.3), y a 0.95 un 5 % más (19.2 frente a 18.2). Reabierto devuelve lo mismo.
  - **Pero sin rehacer aristas sale igual:** la corrida del hito 1, que no las rehacía, da los mismos números (0.9509 y 19.7 a ef 21, 0.9904 y 72.2 a ef 158, frente a 0.9511 y 19.6, 0.9902 y 72.2). Rehacerlas costó un 42 % más de búsqueda al insertar: por eso viene apagado.
  - **Las corridas a 1M del hito 2 se hicieron con él encendido,** que era lo que hacía el código entonces.
- **`rq.rescoreLimit: -1` cambia de regla** (decisión del 22 de noviembre): el índice rescora el mayor entre 64, 150 × (768 / bits)³ y 4 veces el `limit` de la query, con un tope de 1 200 en lo que depende de los bits. A 1536 dimensiones, como dbpedia, siguen siendo 64 para un `limit` de 10, y 400 para uno de 100.
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
  - *Verificado por tests,* también por el shard, y con una pasada de 30 000 vectores: el mismo índice abierto con los códigos en memoria responde exactamente lo mismo. Y con el dataset de 1M, el proceso muerto con `kill -9` a los 15 minutos de un import deja un índice que abre entero, con el halo de cada posting completo. *Verificado a 1M* (22 de noviembre): creciendo de 10 000 a 990 000 el índice es el mismo que con los códigos en memoria (0.9536 y 19.7 postings leídos a ef 21, 0.9910 y 72.4 a ef 158), lee tres filas por posting con 208 KB, y abierto con los códigos en memoria no cambia ninguna respuesta. *Sin ninguna medida de latencia ni de heap.*

- **Las búsquedas con filtro (`where`) ya funcionan,** también las híbridas y `Aggregate` con filtro (desde el 23 de noviembre). Ningún resultado queda fuera del filtro.
  - **Con menos de 40 000 objetos permitidos la búsqueda es plana:** ordena los permitidos por un código y rescora los mejores.
  - **Con más, recorre el índice** y solo devuelve permitidos.
  - **Con `rq.cacheMode: "none"` la búsqueda plana también funciona** (desde el 24 de noviembre). Ordena por el código completo que guardan los postings, así que carga una vez cada posting que tenga un permitido. A 1M, al azar al 1 %, son unos 3 655 postings por query, 14 veces lo del recorrido del índice. Lo de cargar postings en esta búsqueda plana cambia en otra rama, que no está en esta.
  - *Verificado por tests,* también por el shard, en los dos modos.
  - **Medido a 1M en `routing`, cronometrado el 25 de noviembre** (detalle en [#14023](https://github.com/weaviate/weaviate/pull/14023)):
    - con un filtro al azar, la plana empieza a ser más barata que el índice entre el 7 % y el 5 % de permitidos;
    - un filtro correlado con la query va mejor siempre por el índice, y uno anticorrelado por la plana desde el 10 %;
    - en caliente gana a HNSW por defecto con `acorn` en los cuatro filtros medidos; frente a HNSW centrado pierde al azar al 10 %, empata al 1 % y gana en los correlados y anticorrelados al 1 %;
    - en frío empata o pierde, sobre todo por las 507 lecturas de vector de la plana.
  - **La búsqueda por distancia sigue sin funcionar,** con filtro o sin él.

- **Un índice al que le falta su `meta.json` ya no abre vacío borrándolo todo** (desde el 23 de noviembre). Da error al abrir, dice qué ficheros hay y no borra nada. Lo único que el índice borra por su cuenta es lo que escribió un build de su base que no acabó. Y solo si ese build dejó la lista de lo que creaba y los vectores de los que partía siguen apartados. Cada vez deja una línea en el log con lo que borró y por qué.
  - *Verificado por tests:* el proceso que muere a mitad del build, el build que falla sin morir, una copia hecha a mitad de un build y restaurada en otro sitio, y los casos en que no borra nada.

- **Los centroides ocupan una cuarta parte, y los de los postings retirados ya no ocupan nada** (desde el 26 de noviembre).
  - Se guardan como códigos RQ de 8 bits, sin rescore, como en HFresh: a 10M el índice en `none` pasa de 236.6 a 152.8 MB de heap tras abrir, con el mismo recall y las mismas cargas (lo más que se separa es un 0.4 %).
  - **Un índice creado antes se convierte la primera vez que se abre:** el arranque tarda algo más esa vez, sin error.
  - *Verificado por tests y medido a 1M y 10M sobre copias de los índices de los experimentos.*

**Lo que sigue igual que en el hito 1:** lo de «Qué falla por diseño», arriba con sus errores.

## Lo que falta

- la búsqueda por distancia;
- `l2-squared`;
- las métricas de Prometheus propias del índice;
- el backup, en #14013;
- ponerse al día con `main`: la rama sale de un `main` de hace semanas.
