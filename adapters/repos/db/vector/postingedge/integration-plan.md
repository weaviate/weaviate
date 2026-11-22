# Plan de integración en Weaviate

**Fecha:** 2026-11-19 · **Rama:** `feat/posting-edge-index`, leída en `c74cbd28cf` · **Paquete:** `adapters/repos/db/vector/postingedge` (nombre provisional)

**Alcance.** Solo lectura y este documento: el plan no cambia código del índice. Al leer apareció un bug en flat y en HFresh, que sí se arregló, en un commit aparte (`665f3a1806`). El plan dice qué pide Weaviate a un tipo de índice, qué de eso existe ya, qué vive en ficheros de test y qué falta. Las cifras salen de `bench-report.md` y de `disk-design.md`; las rutas, del código de esta rama. Lo que es propuesta mía va marcado como tal.

**La tabla de parámetros.** El encargo pedía en el punto 4 «la tabla de arriba», que no venía en el mensaje: la primera versión de la sección 4 la monté con los parámetros que el mensaje nombraba y los de la configuración del paquete. La lista acordada llegó después, el 19 por la tarde. La sección 4 empieza ahora por ella, con las diferencias marcadas.

## Resumen

- **El índice ya cumple la interfaz del shard a nivel de tipos,** y de sus 23 métodos tiene hechos los de búsqueda sin filtro, validación y configuración. Faltan los que hacen de él un índice vivo: insertar después del build, borrar, persistir, reiniciar y entrar en un backup.
- **Casi todo lo que se ha medido en disco es código de test.** El paquete tiene 3 216 líneas de código y 27 165 de test. La lectura de disco, los inserts y los deletes viven en `disk_test.go`, `grow_test.go`, `stream_test.go` y `delete_test.go`.
- **Registrar el tipo en Weaviate es mecánico y disperso:** unos quince sitios, sin una lista única. Es lo único que puede empezar ya.
- **Lo que bloquea de verdad es `Delete`.** El shard lo llama en cada borrado y en cada update que cambia un vector. Sin él, ninguna clase con este índice admite cambios.
- **Los deletes tienen una celda que cumple con el 30 % borrado, en una pasada de limpieza (v6.1), y otra que cumple en un churn sin búsquedas (v6.2 y v6.2b, del 20):** la sustitución local de las aristas muertas. Aguanta treinta rondas del 3 %, con el 90 % de la base renovado, y el 30 % borrado por regiones.
- **Un filtro hoy da error, y eso falla la query entera.** La opción mínima es una búsqueda exacta sobre la lista por debajo de un corte, y filtrar en el scan por encima. Sin medir.
- **Dos decisiones abiertas condicionan el formato:** dónde viven los vectores exactos, y si hay un formato en disco o dos.
- **En parámetros,** lo acordado son seis: `distance` (solo `cosine` en la primera versión), `ef`, `efConstruction`, `rq.rescoreLimit`, `maxPostingSizeKB` y `rq.cacheMode`, que acepta `routing` (por defecto) y `none`; el modo `scan` salió de la lista el 20, descartado por v4.1. El `rescoreLimit` automático no se acopla a `ef` en el modo `routing`: sale de los bits del código de scan, 64 a 1536d y 200 a 768d.
- **Medido el 21:** k = 1 y k = 100, y el barrido a 768d con rescore 64, 100, 150 y 200. El `ef` no necesita un factor por k, y el `rescoreLimit` automático pasa a ser el mayor entre 64, 150 × (768 / bits)³ y 4 × k (sección 4). **La regla del rescore está validada solo a 768 y a 1 536 bits; por debajo es extrapolación, y falta un barrido a 1M con un dataset de 384 o 512d antes de publicar el índice.**
- **Al leer apareció un bug en flat y en HFresh,** en la búsqueda por distancia. Está arreglado en esta rama con sus tests, y sigue en `main`.

## 1. Qué exige un tipo de índice

Tres estados por pieza: **lo tenemos**, **hay que adaptarlo** o **falta**.

### La interfaz

**Es `db.VectorIndex`** (`adapters/repos/db/vector_index.go:26-57`), con 23 métodos. El índice ya la cumple a nivel de tipos: `interface_test.go` lo comprueba al compilar. Lo que decide es qué hace cada método.

| método | qué hace HFresh | qué hace este índice hoy | estado |
|---|---|---|---|
| `Type` | Devuelve `hfresh` | Devuelve `postingedge` | Lo tenemos |
| `Add`, `AddBatch` | Insertan. `AddBatch` es un bucle de `Add` | Acumulan hasta `Build`. Después devuelven `ErrBuilt` | Hay que adaptarlo: el camino de inserts está en test. El shard llama a `AddBatch` en síncrono y a `Add` desde la cola |
| `Delete` | Marca el id en su mapa de versiones. Un id desconocido no es error | `ErrNotSupported` | Falta. El shard lo llama en cada borrado y en cada update que cambia un vector |
| `SearchByVector` | Con filtro de menos de 5 000 ids, búsqueda plana. Con más, filtra en el scan | Sin filtro, busca. Con filtro, `ErrNotSupported` | Lo tenemos sin filtro. El filtro falta |
| `SearchByVectorDistance` | Páginas cada vez mayores sobre `SearchByVector` | `ErrNotSupported` | Falta. Es poco código, con la forma del bucle de HNSW |
| `UpdateUserConfig` | Aplica `searchProbe` y `rq.rescoreLimit` | Aplica `ef`, `rescoreLimit`, `entryPostings`, `loadBatch` y `sourceMargin`, y rechaza lo que dio forma al build | Lo tenemos |
| `Flush` | Vuelca el commit log de centroides y sus cuatro colas | No hace nada | Falta, con la persistencia |
| `Shutdown`, `Drop` | Cierran colas y centroides. `Drop` no borra buckets: lo hace el shard | Apagan el grafo de centroides y borran su directorio | Hay que adaptarlo |
| `PrepareForBackup`, `ResumeAfterBackup`, `ListFiles`, `SnapshotMutableFiles` | Pausan colas y commit log, y listan sus ficheros. `SnapshotMutableFiles` no hace nada | Los dos primeros de backup devuelven `ErrNotSupported` a propósito, para que un backup no acabe sin el índice | Falta |
| `PostStartup` | Calienta el mapa de versiones en segundo plano | No hace nada | Falta: restaurar lo residente |
| `Compressed`, `CompressionStats` | `true`, y las estadísticas del cuantizador | Lo mismo | Lo tenemos |
| `Multivector` | `true` solo con MUVERA | `false` | Lo tenemos. Con `false`, el shard rechaza el multi-vector por su cuenta |
| `ValidateBeforeInsert` | Comprueba dimensiones | Dimensiones y valores finitos | Lo tenemos |
| `ContainsDoc`, `Iterate` | El primero, por su mapa de versiones. El segundo solo escribe un log | Un bitmap de ids presentes | Lo tenemos. Con deletes, `ContainsDoc` tiene que dar `false` para un id borrado |
| `Preload` | No hace nada | No hace nada | Lo tenemos |
| `QueryVectorDistancer` | Distancia exacta, leyendo el vector | Lo mismo | Lo tenemos |

**Interfaces opcionales, que el código comprueba con una aserción de tipo:**

- **`VectorIndexMulti`** (`vector_index.go:90-99`): siete métodos de multi-vector. No hace falta para una primera versión.
- **`upgradableIndexer`** (`vector_index_queue.go:257-263`): para pasar de sin comprimir a comprimido. No aplica, y HFresh tampoco la implementa.
- **Los endpoints de debug** miran tipos concretos: las estadísticas piden un `Stats()` que solo tiene HNSW (`adapters/handlers/rest/handlers_debug.go:891`), y el reset de un índice solo acepta HNSW y `*hfresh.HFresh` (`adapters/repos/db/index.go:5086`, `adapters/repos/db/shard_debug.go:64`).

### El registro, de punta a punta, con HFresh de modelo

| pieza | dónde está para HFresh | en este índice | estado |
|---|---|---|---|
| El nombre del tipo | `vectorindex.VectorIndexTypeHFresh` (`entities/vectorindex/config.go:30`) y `common.IndexTypeHFresh` (`adapters/repos/db/vector/common/index_stats.go:21`) | Solo existe `common.IndexTypePostingEdge` (`index_stats.go:24`). El nombre es provisional | Hay que adaptarlo, con el nombre decidido |
| La configuración de usuario | `entities/vectorindex/hfresh/config.go`: `UserConfig`, `SetDefaults`, `validate` y `ParseAndValidateConfig` | `UserConfig` y `Validate` viven en el paquete del índice (`config.go:82`). No hay parseo desde JSON | Hay que adaptarlo: pasa a `entities/vectorindex/`, con el parseo y los nombres de la sección 4 |
| El despacho por tipo | `ParseAndValidateConfig` y `UnmarshalConfig` (`entities/vectorindex/config.go:40-57` y `:62-79`), cableados en `adapters/handlers/rest/configure_api.go:659` y `:820` | No está | Falta: un `case` en cada uno |
| Las listas cerradas de tipos | `usecases/schema/parser.go:259`, `usecases/config/config_handler.go:542`, `entities/schema/collection.go:66-84`, `usecases/modules/vectorizer.go:514-519` | No está en ninguna | Falta |
| El esquema en el cluster | `normalizeVectorConfig` (`cluster/schema/movement_guard.go:103-121`). Con un tipo que no conoce, bloquea el movimiento de shards | No está | Falta |
| La validación de un cambio de configuración | `Migrator.ValidateVectorIndexConfigUpdate` (`adapters/repos/db/migrator.go:908-925`), que llama a `hfresh.ValidateUserConfigUpdate` | La regla existe, dentro de `Index.UpdateUserConfig` | Hay que adaptarlo: sacarla a una función que se pueda llamar sin el índice |
| Las reglas del esquema | `validateVectorIndexTypeBasic` (`usecases/schema/class.go:2033`). HFresh se salta `ALLOWED_COMPRESSION_TYPES` y la compresión por defecto (`:1795`, `:1826`). El multi-vector solo se admite en HNSW (`usecases/schema/parser.go:264-268`) | No está | Falta: los mismos saltos que HFresh, porque la compresión es fija |
| Un interruptor | HFresh no tiene flag propio. Se limita con `ALLOWED_VECTOR_INDEX_TYPES` y `DEFAULT_VECTOR_INDEX` | No está | Por decidir: si una primera versión va detrás de un flag propio |
| La creación en el shard | La rama `hfresh` de `initVectorIndex` (`adapters/repos/db/shard_init_vector.go:286-355`). Le pasa el store del shard, los thunks que leen vectores del bucket de objetos, el scheduler, los callbacks, las métricas y el logger | No hay rama. `Config` ya pide los thunks, el proveedor de distancia, `AllocChecker` y `RootPath`. No pide el store, el scheduler ni las métricas | Hay que adaptarlo |
| Los directorios y los nombres de lo que escribe | `vectorIndexStorageDirs` (`adapters/repos/db/shard_vector_storage.go:31-55`), que da error con un tipo desconocido. Los nombres para borrar por vector y en frío (`adapters/repos/db/helpers/helpers.go:178-236`). La cuenta de uso (`adapters/repos/db/shard_usage/usage.go`) y la categoría de dimensiones (`adapters/repos/db/shard_dimension_tracking.go:189-204`) | El índice ya usa `<id>.postingedge.d`, el mismo esquema que `<id>.hfresh.d`. Los buckets de test tienen nombres fijos | Falta. Los buckets tienen que llevar el id del índice, como `hfresh_postings_<id>`: un shard tiene un índice por vector con nombre |
| El scheduler y las colas de fondo | `DB.scheduler` (`adapters/repos/db/repo.go:425-432`) baja hasta `hfresh.Config.Scheduler`. HFresh registra cuatro `queue.DiskQueue` en disco (`hfresh/task_queue.go`): analizar, partir, reasignar y fusionar | El mantenimiento corre dentro del insert, sin cola | Falta. Es el requisito de `disk-design.md`, «Los recursos compartidos del nodo» |
| La indexación asíncrona | `VectorIndexQueue` (`adapters/repos/db/vector_index_queue.go`) llama a `Add`, `AddBatch` y `Delete` de cualquier índice. Apagada, llama a `AddBatch` y a `Delete` en el momento | Nada propio que escribir | Lo tenemos, en cuanto `Add` y `Delete` existan |
| El presupuesto de una búsqueda | `Admit` en `adapters/repos/db/shard_read.go:736`, que viaja en el contexto. HFresh lo lee en su rescore (`hfresh/search.go:184`) | El rescore ya lo lee (`search.go:556`). Las cargas de una ronda no | Hay que adaptarlo |
| La compresión | RQ de 1 bit, fijo. El cuantizador se guarda en su bucket compartido y se restaura al abrir | RQ centrado de 1 bit, entrenado con el lote en `Build`. En disco, la media en `mean.f32` | Hay que adaptarlo: guardarlo con el índice, y decidir cuándo se entrena si el índice empieza vacío |
| Las métricas | `hfresh.NewMetrics` (`hfresh/metrics.go:48-136`), sobre los vectores compartidos de `usecases/monitoring/prometheus.go`: tamaño, operaciones, duraciones, postings y tareas de fondo. Más cuatro anotaciones en el log de queries lentas | `SearchStats`, `BuildStats` y `MemoryStats`, internos, sin Prometheus | Falta |
| El backup | El shard pausa la cola, llama a `PrepareForBackup` y vuelca los memtables (`adapters/repos/db/shard_backup.go:46-181`). Lista `store.ListFiles` más `index.ListFiles` | Rechaza el backup | Falta. Lo que viva en buckets del store del shard entra solo. Los ficheros propios hay que listarlos |
| El reinicio | `hfresh.New` lo restaura todo: sus buckets, el commit log de centroides, las dimensiones, el cuantizador, los mapas y las colas pendientes. `PostStartup` calienta después | `openDiskIndex` y `openGrowIndex`, en test | Falta |
| La carga perezosa y el borrado en frío | Borran ficheros por nombre, sin abrir el índice (`adapters/repos/db/shard_lazyloader.go`, `adapters/repos/db/helpers/helpers.go:178-236`) | No está | Falta: los nombres del índice, en las dos listas de `helpers.go` |
| Los tests que nombran el tipo | `test/acceptance/drop_vector_index/hfresh_test.go`, `test/acceptance/vector_index_restrictions/`, `usecases/schema/class_restrictions_test.go`, `cluster/schema/schema_test.go` | No hay | Falta |

- **No existe una lista única de tipos.** El nombre de un tipo nuevo hay que añadirlo en unos quince sitios, y varios fallan en silencio o tarde: el guardián de movimiento de shards deja de moverlos, y el borrado en frío deja ficheros.

## 2. Qué código hay que mover

**El paquete tiene 3 216 líneas de código y 27 165 de test.** El código es el índice en memoria: se construye una vez en lote y después solo sirve búsquedas. Todo lo que lee de disco, inserta o borra está en ficheros `_test.go`, y por eso no entra en ningún binario.

**Lo que ya es código del paquete:**

- **El build en lote** (`build.go`, `partition.go`, `assign.go`, `edges.go`): partición, asignación y aristas, desde `AddBatch` y `Build`.
- **La búsqueda** (`search.go`): las rondas, el scan de un posting, los endpoints, la parada y el rescore. El camino de disco ya la reutiliza: llama a `newPostingSearch`, a `postingSearch.visit` y a `Index.rescore`.
- **Los postings y el sidecar en RAM** (`postings.go`), el grafo de centroides (`centroids.go`, sobre el paquete `hnsw`) y la configuración (`config.go`).
- **La marca de borrado de v6.0** (`state.deleted` en `index.go`, leída en `search.go`): hoy solo la pone el código de test.

**Lo que hay que mover, por fichero:**

| fichero de test | líneas | qué contiene que tendría que ser código | qué se queda como test |
|---|---|---|---|
| `disk_test.go` | 1 241 | El camino de lectura de v4.0. El formato (`diskMeta`, las claves de bloque, los ficheros residentes `meta.json`, `mean.f32`, `layout.u32`, `routing.u64`, `centroids.f32`, `vectors.f32`). La escritura de un índice en lote (`writeDiskIndex`, `writeCentroidGraph`, `writeDiskVectors`). La apertura (`openDiskIndex`, `openDiskStore`, `openCentroidGraph`). La lectura por rondas (`diskIndex.fetch`, `fetchMembers`, `fetchEdges`, `diskPosting`, `diskRound`). La búsqueda sobre postings traídos (`fetchedSearch.run`), que es lo común a v4.0 y v5.0. El scan con los códigos en RAM de v4.1 (`residentMembers`, `sketchScanState`) | Las cuentas de I/O del arnés (`diskSearchStats`) y el modo `pread` |
| `halo_test.go` | 771 | El halo: el sketch de cada endpoint dentro del posting que lleva a él (`haloPosting`, `haloRead`, `fetchHalo`, `scoreHalo`), y su poda | Sus tests |
| `grow_test.go` | 2 513, unas 2 100 de camino | Los inserts en disco de v5.0 a v5.2. El formato que crece: un bucket Set con dos filas por posting (`postingedge_grow_postings`) y tres buckets Replace por vector (`postingedge_grow_map`, `_routing`, `_edgedists`), más `postingedge_grow_edges` y `slots.u32`. La escritura de la base (`writeGrowIndex`) y la apertura (`openGrowIndex`). La lectura (`row`, `fetchMembers`, `fetchLists`, `joinEdges`). El insert partido en buscar y aplicar (`plan`, `apply`, `insertBatch`), las aristas inversas (`enterNeighbors`), el split (`split`), las aristas pendientes y su consolidación (`setPending`, `writeRow`, `flushPending`), y `flushAll` y `compactAll` | `sameAs`, que compara con el gemelo en memoria, y `checkCoverage` |
| `stream_test.go` | 1 474, unas 830 de camino | El mismo camino de escritura, en memoria (`streamIndex`): `insert`, `nearestCentroid`, `enterNeighbors`, `split`, `linkSiblings`, `repairEdges`, `refreshEdges`. Es el gemelo contra el que se valida el de disco, insert a insert | Los diagnósticos: `edgesDigest`, `snapshotEdges`, `rebuildEdges`, las cohortes de grado y el recall de arista |
| `delete_test.go` | 457 | Los deletes de v6.0, solo en memoria: la marca (`remove`), la reescritura de un posting sin sus borrados (`rewrite`), la limpieza y la fusión (`cleanup`, `merge`) y el refresco de las listas medio muertas (`refresh`) | `checkCoverage` y las cuentas |
| `*_equiv_test.go`, `stream_insert_test.go`, `grow_batch_test.go`, `delete_cells_test.go` | 4 844 | Nada: son los tests de lo anterior | Todo |
| `bench_test.go` y `bench_*_test.go`, casi todos con el tag `postingedgebench` | 10 590 | Nada: es el arnés | Todo |

- **`stream_test.go` no hace falta moverlo como índice.** Su papel es de oráculo: el índice que crece en disco se compara con él insert a insert. Puede quedarse en test si el camino de disco pasa a ser el único que escribe. La decisión depende de si un nodo quiere alguna vez este índice entero en memoria.
- **Mover no es copiar.** El código de test asume un escritor, cierra el store para compactar y da por buenos sus argumentos. Al pasar al paquete necesita errores de entrada, concurrencia y cierre ordenado.

**Lo que no existe ni en test, y hay que escribir:**

- **Durabilidad.** Qué queda escrito cuando `Add` devuelve, y qué se recupera tras una caída. Con las aristas consolidadas de v5.1, los cambios pendientes viven en memoria hasta que se escribe la fila. En v5.0 cada cambio se entrega al store al momento.
- **La reorganización en segundo plano.** El índice crecido solo cumple el criterio de latencia en frío con el bucket de postings en un segmento y los vectores en orden de posting (1.07 veces el lote, frente a 1.33 tal cual). `lsmkv` no llega solo a un segmento. No está diseñada ni medida.
- **Inserts y búsquedas a la vez.** Todo lo medido tiene un escritor y ninguna búsqueda mientras escribe. v5.2 pone en paralelo las búsquedas de los propios inserts: 3.6 veces con 8 buscadores, en la medida limpia de la noche del 20, con el aplicador en un 19 % del tiempo. Las queries mientras se inserta siguen sin medir.
- **Deletes en disco, y el halo en el camino de inserts.** Los deletes de v6.0 son en memoria y en conteos.
- **El estado residente del índice en lote como algo que se actualiza.** v4.0 lo lee de ficheros planos escritos una vez. El formato que crece ya lo tiene en buckets.

**Una decisión que bloquea el formato: dónde viven los vectores exactos.**

- **En el prototipo, en un fichero propio** (`vectors.f32`): en orden de posting en v4.0 y por id de vector en v5.0. Es el layout (b') de `disk-design.md`. Cada lectura del rescore son 6 144 B a 1536d.
- **En Weaviate, en el bucket de objetos.** HNSW y HFresh leen el vector de ahí, con `TempVectorForIDWithViewThunk`, y el índice en memoria de este paquete también. Es el layout (a). Una lectura trae el objeto entero, con sus propiedades.
- **Hay un precedente de copia propia: flat.** Guarda sus vectores en un bucket suyo y hace el rescore desde ahí (`vectorById`, `adapters/repos/db/vector/flat/index.go:657`).
- **No está medido con objetos reales.** Todo lo cronometrado en disco usa el fichero propio, o un bucket de objetos donde el objeto es solo el vector. Con (b') el índice guarda una segunda copia de los vectores y tiene que mantenerla con los inserts, los deletes, el backup y la replicación.

## 3. Huecos conocidos

### Deletes

**Hoy no hay.** `Delete` devuelve `ErrNotSupported`, y volver a añadir un id también: un update se rechaza.

**Lo que pide el shard:**

- **`Delete` en cada borrado, en todas las colas de vectores del shard** (`adapters/repos/db/shard_write_delete.go:120`). Llega también para ids que el índice nunca tuvo: tiene que aceptarlos sin error, como HFresh.
- **Un update que cambia el vector es un borrado más un insert.** El objeto recibe un doc id nuevo, y el shard borra el viejo y añade el nuevo (`updateVectorInVectorIndex`, `adapters/repos/db/shard_write_put.go:912`). Los doc ids salen de un contador y no se reutilizan.
- **Si `Delete` falla:** en síncrono falla la petición, cuando el bucket de objetos ya cambió. En asíncrono la cola descarta la tarea con un log, y el id se queda en el índice.

**La red que hay por encima.** Un id que el índice devuelve y cuyo objeto ya no existe se descarta al cargar los objetos (`ObjectsByDocIDWithDists`, `entities/storobj/storage_object.go`). Un id viejo cuesta resultados de menos, no resultados falsos.

- **Esa red no sirve hoy a este índice.** Su rescore falla la búsqueda entera si no puede leer un vector (`exactDistance`, `search.go:591`), donde HNSW y HFresh saltan el candidato. Hay que cambiarlo antes que nada: sin eso, cualquier borrado rompe las búsquedas que lo encuentren.

**Lo que hay medido:** v6.0 y v6.1, cerrados el 19, y v6.2, cerrado el 20, en memoria y en conteos. En disco, nada.

- **Por su criterio, los deletes no se sostienen** con ninguna de las cinco celdas, con el 30 % borrado al azar.
- **La celda 1b es la única que cumple el recall a igual ef:** un borrado se sigue puntuando y sus aristas se siguen, y nunca es candidato. Falla solo en cargas, +19.1 % a 0.95 y +13.7 % a 0.99, y ese fallo es de llenado: se atribuye a la política de fusión, no a las aristas.
- **Con el 10 % borrado la celda 1b cumple.**
- **v6.1, la limpieza en dos fases, cumple con el 30 % borrado al azar en una pasada.** Primero repara los vectores con el 25 % o más de sus aristas hacia borrados, navegando por ellos. Después los quita y fusiona por debajo de 177 vivos si la suma no pasa de T. Queda en −0.0023 de recall, +2.7 % de cargas a 0.95 y −10.7 % a 0.99.
- **Cuesta 66 cargas por vector borrado,** casi dos inserts.
- **En el churn esa pasada no cumple:** −0.0071 de recall y +16.9 % de cargas a 0.99, con un 3 % borrado por ronda. El umbral se mide en cada pasada y no acumula lo perdido en las anteriores: las listas pasan de 8.6 a 7.2 aristas por vector.
- **v6.2 arregla el churn de tres maneras, y la más barata no busca nada.** Al quitar un borrado, cada vector que apuntaba a él recibe como sustituto el vecino más cercano de la lista del borrado, por una distancia entre dos códigos. Cumple en diez rondas del 3 %: −0.0004 de recall, +2.7 % de cargas a 0.95 y +1.8 % a 0.99, con 62.8 distancias entre códigos por borrado y ninguna carga. Las listas se quedan en 8.84 aristas por vector.
  - **Con memoria en el umbral y reparación por búsqueda también cumple,** a 68 cargas por borrado.
  - **v6.2b, del 20: aguanta treinta rondas y el borrado por regiones.** Sobre una base de 520 000, con el 90 % renovado al final: −0.0011 de recall, +1.6 % de cargas a 0.95 y −1.3 % a 0.99 en la ronda 30, y no empeora de la ronda 10 a la 30. Los sustitutos se frenan en el 23.5 % de las aristas. Con el 30 % borrado por regiones entra, y carga menos que el lote.
  - **La red de búsqueda no hace falta, por la regla pre-registrada:** la sustitución con red tampoco mejora nada medible, y su coste sube ronda a ronda.
  - **Sin medir:** lo que cuesta la pasada en disco y cada cuánto pasa: cada pasada lee y escribe todos los postings.

*Lo mínimo para una primera versión, por lo leído:*

- **La marca de v6.0, guardada con el índice, con la semántica de la celda 1b:** un bit por vector, y el borrado sigue ruteando mientras esté en su posting. `Delete` la pone y no falla con ids desconocidos. `ContainsDoc` da `false` para un id marcado.
- **El rescore salta un vector que falta.**
- **La fusión que devuelve el llenado, medida en v6.1 dentro de la pasada de dos fases.** Por debajo de 177 vivos, con el vecino más cercano, si la suma no pasa de T: deja la media en 235.1, con la referencia en 236.0.
- **No está medida sobre la celda 1b sola.** Reescribir un posting sin sus borrados le quita las fuentes, y por eso la pasada repara antes. Sin pasada, una primera versión con la marca sola carga un 19 % más a 0.95 con el 30 % borrado, y cumple con el 10 %.
- **La pasada para un churn continuo, medida en v6.2 y en v6.2b (del 20):** la sustitución local en la fase que quita los borrados, sin fase de reparación y sin red de búsqueda. Es del hito 2.

### Filtros

**Hoy una búsqueda con filtro devuelve un error:** `ErrNotSupported`, «filtered search» (`search.go:173`).

**Qué pasaría en un shard:**

- **El shard resuelve el filtro a una lista de ids permitidos y se la pasa siempre al índice** (`adapters/repos/db/shard_read.go:743` y `:820`). Nunca responde él: no tiene un camino plano propio para listas pequeñas. Esa decisión es de cada índice.
- **El error del índice falla la query entera,** como «vector search: …», y manda un evento a Sentry cada vez (`shard_read.go:822-829`).
- **Lo mismo en la búsqueda híbrida con filtro y en `Aggregate` con filtro,** que llaman al índice con la misma lista.
- **Nadie comprueba el filtro después.** Un índice que ignorase la lista devolvería objetos de fuera del filtro, en silencio. Devolver error es lo correcto mientras no haya filtros.
- **No hay forma de declarar que un tipo no filtra.** La configuración de un índice solo dice su tipo, su distancia y si es multi-vector.

**Qué hacen los otros:**

- **HNSW:** búsqueda plana por debajo de `flatSearchCutoff` (40 000 ids). Por encima, el grafo con una estrategia de filtro (`acorn` por defecto).
- **HFresh:** búsqueda plana por debajo de 5 000 ids, leyendo el vector exacto de cada id permitido del bucket de objetos, de uno en uno (`adapters/repos/db/vector/hfresh/search_flat.go`). Por encima, comprueba cada entrada en el scan del posting y pasa la lista al grafo de centroides.
- **Flat:** recorre sus vectores y salta los que no están en la lista.

*La opción mínima que veo, en dos partes:*

1. **Por debajo de un corte, búsqueda exacta sobre la lista.** Se leen los vectores de los ids permitidos con el thunk que el índice ya recibe y se ordenan por distancia exacta. Son unas 90 líneas, con `hfresh/search_flat.go` de plantilla. No hay un helper compartido que reutilizar.
2. **Por encima, filtrar en el scan.** Un miembro fuera de la lista se puntúa y sus aristas se siguen, y no entra en los candidatos ni en el rescore. Es el mismo mecanismo que la celda 1b de v6.0 aplica a un borrado, y ahí cumple el recall a igual ef: la comprobación de la lista iría donde va la de la marca.

- **Nada de esto está medido.** Con un filtro selectivo por encima del corte, la búsqueda cargaría muchos postings para reunir k candidatos permitidos. Lo más parecido que hay es la variante agrupada de v6.0, que borra regiones enteras: la celda 1b cumple ahí su criterio, con el 30 % del corpus fuera.
- **La alternativa: rechazar la query antes de ejecutarla,** con un error claro. Pide fontanería nueva por encima del índice y no tiene precedente.

### Métricas de distancia

**Acordado: `distance` se expone, y la primera versión valida solo `cosine`.** Cualquier otro valor da error de configuración.

- **El error tiene que salir al parsear,** para que la clase no llegue a crearse. Hoy la distancia se rechaza al crear el índice (`Config.validate`, `config.go:259`), es decir, al arrancar el shard. HFresh valida las suyas al parsear (`entities/vectorindex/hfresh/config.go:113`).
- **El código de hoy admite también `l2-squared` por dentro,** y sus tests pasan. Con lo acordado queda sin camino desde el esquema.
- **El shard no normaliza:** el índice recibe vectores crudos y los normaliza él para el coseno. Ya lo hace, en el insert, en la query y en el rescore.

**Qué haría falta para `l2-squared`:**

- **Medirlo: no hay ninguna medida con esa métrica.** Ni una curva de recall, ni de cargas, ni el recall de arista. Todos los defaults (el `ef` de 20, el `rescoreLimit` automático, T, P y R) salen de dos datasets en coseno.
- **El margen de expansión está en unidades de la distancia** (`SourceMargin`, `config.go`). El 0.06 calibrado vale para el coseno, donde la distancia va de 0 a 2. En l2 la escala depende de la norma de los vectores de cada dataset, y un valor absoluto no puede ser un default: habría que recalibrarlo por dataset o hacerlo relativo, por ejemplo al umbral de parada de la propia búsqueda.
- **Sin margen el índice pierde su mejor palanca.** Hoy, en l2, sigue las aristas de todos los vectores escaneados. En coseno el margen daba de un 41 a un 64 % más de QPS a igual recall.
- **Lo que ya vale tal cual:** la partición y el grafo de centroides trabajan en l2, y el código RQ soporta l2.

**Qué haría falta para `dot`:**

- **Lo mismo que para l2:** ninguna medida, y un margen que no vale.
- **Y un diseño que no existe.** La partición y el grafo de centroides trabajan en l2, que coincide con el coseno sobre vectores normalizados y no con un producto escalar crudo: el centroide más cercano en l2 no es el posting de mayor producto. Hoy `dot` se rechaza por eso. *Dos caminos que veo, ninguno probado:*
  - **Transformar los vectores** para reducir el producto escalar máximo a una búsqueda en l2, con una dimensión más que lleva la norma. Pide una cota de la norma conocida de antemano, y mantenerla con los inserts.
  - **Rutear por producto escalar,** que cambia la partición y la asignación.
- **Las aristas.** *Lectura mía:* los vecinos más cercanos por producto escalar tienden a ser los vectores de norma grande, que acabarían de vecinos de casi todos. Sin medir.
- **Un margen relativo no vale sin más:** la distancia `dot` es el producto cambiado de signo, y un umbral que puede ser negativo no se puede escalar por un factor.

### Multi-vector

- **Hoy no hay:** `Multivector()` devuelve `false` y el índice no implementa `VectorIndexMulti`.
- **En Weaviate eso basta para un rechazo limpio, sin escribir nada:**
  - **El esquema** rechaza un vectorizador multi-vector con cualquier tipo que no sea HNSW (`usecases/schema/parser.go:264`).
  - **El shard** rechaza la escritura y la búsqueda: «index does not support multi-vectors» (`checkVectorKind`, `adapters/repos/db/vector_index.go:64`).
  - **La cola** devuelve `common.ErrMultiVectorUnsupported`.
- **HFresh solo lo soporta con MUVERA,** que convierte un documento multi-vector en un solo vector dentro del índice. El mismo camino valdría aquí más adelante. Sin medir.

### Otros huecos, que salen de la lectura

- **Empezar vacío.** Todo lo medido parte de un índice construido en lote, con el 10 % o el 50 % del corpus. En un shard el índice nace vacío y recibe inserts: hace falta el primer posting, y entrenar el cuantizador y su media con los primeros vectores. HFresh crea su primer posting en el primer insert (`ensureInitialPosting`).
- **Una búsqueda antes de tener nada** devuelve `ErrNotBuilt`. HFresh devuelve una lista vacía sin error, y un error ahí falla la query.
- **`SearchByVectorDistance`** falta. Al leer cómo lo hacen los demás apareció un bug en flat y en HFresh: ver el final de esta sección.
- **Búsquedas mientras se inserta, y varios escritores.** Sin medir.
- **4 294 967 294 vectores por índice,** porque un slot y una arista son 4 B. Es por shard.
- **Los mapas por doc id crecen con los ids emitidos, no con los vivos.** Un update gasta un doc id nuevo. A HNSW le pasa lo mismo.
- **Índices muy pequeños.** Con menos de T vectores hay un solo posting y la búsqueda es su scan. Es el caso de un tenant pequeño, y no está medido.
- **k distinto de 10, y Linux.** Todo lo cronometrado es con k = 10, en un M1 Max con APFS.

**El bug encontrado al leer, ya arreglado en esta rama (`665f3a1806`).**

- **Qué era:** en flat y en HFresh, el bucle de `SearchByVectorDistance` ampliaba la página y no volvía a buscar. HNSW sí vuelve a buscar.
- **Qué causaba:** una búsqueda por distancia devolvía como mucho sus primeros 100 resultados. Sin máximo, que es lo que pasa un `Aggregate` con `certainty` y sin `objectLimit`, no terminaba nunca.
- **Dónde:** `adapters/repos/db/vector/flat/index.go` y dos veces en `adapters/repos/db/vector/hfresh/search.go`. Está igual en `main` a día 19.
- **Tests:** `search_by_distance_test.go` en los dos paquetes. Fallan sin el arreglo, con dos casos truncados a 100 y dos que no vuelven, y pasan con él.

## 4. Parámetros públicos

**Convenciones que sigo,** leídas en `entities/vectorindex/{hnsw,hfresh,flat}/config.go`:

- **Claves en camelCase,** con la unidad en el nombre cuando hay duda (`maxPostingSizeKB`, `cleanupIntervalSeconds`).
- **La compresión, en un bloque con su nombre** (`rq`, `bq`), con `enabled` dentro. El `rescoreLimit` vive en ese bloque en los tres índices, nunca arriba.
- **`-1` es «que elija Weaviate»:** `ef` en HNSW y `rescoreLimit` en flat.
- **Los enumerados, en minúsculas** (`acorn`, `sweeping`, `l2-squared`).
- **`vectorIndexConfig` es libre en el OpenAPI** (`openapi-specs/schema.json`): los nombres solo existen en los structs y en las funciones de parseo. Las claves desconocidas se ignoran sin error, y un valor de otro tipo también.

### Lo acordado (del usuario, 19 por la tarde)

| público | default | a qué corresponde dentro | qué validar |
|---|---|---|---|
| `distance` | `cosine` | `UserConfig.Distance` | Solo `cosine` en la primera versión. Cualquier otro valor, error de configuración |
| `ef` | 20 | `UserConfig.EF`. El efectivo es el mayor entre `ef` y k | 1 o más |
| `efConstruction` | 160 | El ef de la búsqueda de un insert | 16 o más: el número de aristas |
| `rq.rescoreLimit` | Automático, por los bits del código de scan: 64 a 1536d y 200 a 768d. Sin acoplar a `ef` en el modo `routing` | `UserConfig.RescoreLimit`. El efectivo nunca baja de k | −1 para el automático, o 0 o más |
| `maxPostingSizeKB` | `0`: automático, 236 vectores de media | El umbral de split T: 360 vectores en automático. Un valor explícito, con la fórmula de HFresh | 0, o de 8 a 1024 |
| `rq.cacheMode` | `routing` | Qué códigos quedan en RAM: `routing`, el de ruteo; `none`, ninguno (el halo) | Uno de los dos valores. Cualquier otro es un valor desconocido, `scan` incluido |

**Todo lo demás, interno.**

### Diferencias con mi tabla

**Coincide:** `ef`, `efConstruction` con 160, `maxPostingSizeKB` con `0` como automático, y el modo de memoria con `routing` por defecto. *Del 20:* los valores son dos, `routing` y `none`; `scan` se quitó.

**Cambia, y queda como lo acordado:**

- **El nombre del modo de memoria es `rq.cacheMode`.** Yo ponía `rq.cache` primero y este de alternativa.
- **`dynamicEfMin`, `dynamicEfMax` y `dynamicEfFactor` no son públicos.** Yo reservaba los nombres. Pasa a pendiente: ver la comprobación 3.
- **`rq.enabled`, `rq.bits`, `rq.centering` y `rq.trainingLimit` son internos.** Yo los listaba como públicos de valor fijo. Una consecuencia para el parseo: una configuración que traiga `rq.enabled: false`, otros `bits`, o un bloque `pq`, `sq` o `bq` tiene que dar error, como en HFresh. Los parseadores comunes ignoran en silencio lo que no conocen.

**Los tres puntos que quedaban, resueltos por el usuario el 19 por la tarde:**

1. **`rq.rescoreLimit`,** dentro del bloque de compresión, como en HNSW, HFresh y flat.
2. **Sin acoplar a `ef` en el modo `routing`.** El automático no es un 64 fijo: a 768d su techo es 0.9816. Sale de una regla por los bits del código de scan: ver «El valor automático de `rq.rescoreLimit`».
3. **`distance` se expone, y la primera versión valida solo `cosine`.** Lo que harían falta `l2-squared` y `dot` está en la sección 3, «Métricas de distancia».

### Mi tabla original, con los equivalentes en HNSW, HFresh y flat

*Es la tabla de antes del cotejo. Vale por la columna de equivalentes y sus rutas. Donde choca con lo acordado, manda lo acordado.*

| público, propuesto | interno hoy | en HNSW | en HFresh | en flat | default propuesto | qué validar | ¿cambia después? |
|---|---|---|---|---|---|---|---|
| `distance` | `UserConfig.Distance` (`config.go`) | `distance`, `entities/vectorindex/hnsw/config.go:60` | `distance`, `entities/vectorindex/hfresh/config.go:69` | `distance`, `entities/vectorindex/flat/config.go:45` | `cosine` | Acordado: solo `cosine` en la primera versión, y al parsear, como hace HFresh con las suyas. HNSW y flat lo rechazan más tarde, al crear el shard | No, como en los tres |
| `maxPostingSizeKB` | El umbral de split T: `streamConfig.threshold` y `growConfig.threshold`, en test. En el build en lote, su media: `UserConfig.TargetPostingSize` | No tiene | `maxPostingSizeKB`, `entities/vectorindex/hfresh/config.go:66`. Default 48, de 8 a 1024 | No tiene | `0`, automático: T = 360 vectores. Ver la comprobación 1 | 0, o de 8 a 1024 como HFresh. El T que resulte, de 192 vectores o más | No. HFresh acepta el cambio y no lo aplica hasta reconstruir el índice: mejor rechazarlo |
| `ef` | `UserConfig.EF`, 20 | `ef`, `entities/vectorindex/hnsw/config.go:54`. Default −1 | `searchProbe`, `entities/vectorindex/hfresh/config.go:68`. Default 256. Es el concepto equivalente, no el mismo nombre | No tiene | 20. O −1, si se adopta el ef dinámico: ver la comprobación 3 | −1, o 1 o más. Se sube a k, como hoy | Sí, en vivo, como en HNSW y como hoy |
| `dynamicEfMin`, `dynamicEfMax`, `dynamicEfFactor` | No existen | `entities/vectorindex/hnsw/config.go:55-57`. Defaults 100, 500 y 8 | No tiene | No tiene | Sin proponer valores: falta medir otros k | Mínimo de 1 o más, máximo no menor que el mínimo, factor de 1 o más | Sí, en vivo |
| `efConstruction` | El ef de la búsqueda de un insert: 160, flag del arnés. En el build en lote lo sustituyen `BuildProbe` (256) y `RescueLimit` (64) | `efConstruction`, `entities/vectorindex/hnsw/config.go:53`. Default 128, mínimo 4 | No tiene. Lo más cercano es interno: `InternalPostingCandidates`, 64 | No tiene | 160 | El número de aristas (16) o más. Medido: 80 cumple por 0.2 puntos y 40 no cumple | No, como en HNSW |
| `rq.enabled`, `rq.bits`, `rq.centering` | Fijos: código RQ centrado de 1 bit (`trainQuantizer` en `postings.go`) | `rq`, `entities/vectorindex/hnsw/rq_config.go:30-36`. Bits 8, 4 o 1; `centering` con 4 o 1 | `rq`, `entities/vectorindex/hfresh/config.go:70`. Solo `enabled: true` y `bits: 1`. Rechaza `centering` | `rq`, `entities/vectorindex/flat/config.go:37-42`. Bits 1 u 8 | `true`, 1 y `true` | Rechazar cualquier otro valor, y los bloques `pq`, `sq` y `bq`, con error, como HFresh | No |
| `rq.trainingLimit` | `UserConfig.TrainingLimit`, 10 000 | `rq.trainingLimit`, `entities/vectorindex/hnsw/rq_config.go:35`. Default 10 000 | No se parsea | No tiene | 10 000 | 1 o más | No |
| `rq.rescoreLimit` | `UserConfig.RescoreLimit`, 64 | `rq.rescoreLimit`, `entities/vectorindex/hnsw/rq_config.go:33`. Default 20, y 512 con 1 bit | `rq.rescoreLimit`. Default 350, `entities/vectorindex/hfresh/config.go:34` | `rq.rescoreLimit` y `bq.rescoreLimit`, `entities/vectorindex/flat/config.go:33` y `:39`. Default −1, que es k | −1, automático. Ver «El valor automático de `rq.rescoreLimit`» | −1, o 0 o más. Efectivo: nunca menos que k | Sí, en vivo |
| `rq.cache`, enumerado | Tres celdas del arnés: `diskMeta.Halo`, `RoutingCodeWords` y `ScanInMemory` | No tiene: siempre cachea, hasta `vectorCacheMaxObjects` (`:58`) | No tiene | `bq.cache` y `rq.cache`, booleanos, `entities/vectorindex/flat/config.go:34` y `:40`, con `vectorCacheMaxObjects` (`:46`) | `routing`. Ver la comprobación 2 | Uno de los valores del enumerado. Un booleano, con error | No: cada modo es un formato en disco |

**Lo que propongo no exponer en una primera versión:**

- **`Edges` (16), `BuildProbe` (256), `RescueLimit` (64).** Dan forma al build y solo están medidos en sus defaults, con una excepción a 10M (P = 320 y R = 256). El análogo de `Edges` en HNSW sería `maxConnections`, pero aquí cuenta otra cosa: los vecinos fuera del posting propio.
- **`EntryPostings` (1), `LoadBatch` (4), `SourceMargin` (automático).** Cerrados por lo medido.
- **`CodeBits` y `ScanCodeBits`.** Los cubre el modo de memoria.
- **Los bits del código de ruteo** (512; 384 ahorra 16 MB por millón y pierde un 7 % a 0.95 en frío), **el mínimo de fusión** (T / 3), **el umbral de reescritura** (30 % de borrados) y **el K de consolidación.**
- **`replicas` de HFresh no tiene equivalente.** Las aristas hacen su papel.

### Comprobación 1: `maxPostingSizeKB` como umbral de split

**En HFresh es el umbral de split, y su unidad es KiB de posting.**

- **La conversión a vectores** está en `computeMaxPostingSize` (`adapters/repos/db/vector/hfresh/config.go:163-171`): `ceil(KB × 1024 / (dims / 8 + 25))`. Los 25 B son el id (8), la versión (1) y los metadatos de RQ (16).
- **Tiene un suelo de 192 vectores** (`maxPostingSizeFloor`, `:32`), y el mínimo de fusión es el máximo entre tres (`setMaxPostingSize`, `:173`).
- **Es el umbral de split:** un posting con más entradas que `maxPostingSize` encola su split (`insert.go:246`), y con más de cinco veces se parte en el propio insert (`insert.go:236`).
- **Con su default de 48:** 227 vectores a 1536d y 407 a 768d.

**En este índice el umbral es T = 360 vectores, y deja la media en 236.**

- **La calibración de v2.0:** T = 360 da una media final de 237.4. T = 320 da 212.9 y T = 400, 264.4.
- **Creciendo desde el 10 % (v2.2):** 241.8 y 242.8.
- **La media queda entre 0.66 y 0.67 veces T.**

**El valor que deja la media en 236 a 1536d, con la fórmula de HFresh tal cual, es 76.** Da T = 359. Con 77 da 364.

**Cómo cambia con la dimensión:**

| dimensión | B por entrada, con la fórmula de HFresh | con 76 KB fijos: T | media esperada | KB que dejan T en 360 | lo que da el 48 de HFresh |
|---|---|---|---|---|---|
| 384 | 73 | 1 067 | 704 | 25.7 | 674 |
| 768 | 121 | 644 | 425 | 42.5 | 407 |
| 1024 | 153 | 509 | 336 | 53.8 | 322 |
| 1536 | 217 | 359 | 237 | 76.3 | 227 |
| 3072 | 409 | 191, que el suelo deja en 192 | 127 | 143.8 | 121, que el suelo deja en 192 |

- **Con un default fijo en KB, la media en vectores se mueve con la dimensión:** de 127 a 3072d a 704 a 384d.
- **Todo lo medido tiene la media fijada en 236 vectores, también a 768d.** En wiki-dpr S se fijó a 236 a propósito: el presupuesto de 48 KiB habría dado 438. Un default de 76 a 768d daría una media de 425, que no está medida a esa dimensión. A 1536d y en memoria, S = 512 quedaba a 0.95 veces el QPS a 0.95 (Fase C).

*Propuesta mía, que coincide con lo acordado:*

- **El default, `0`: automático, T = 360 vectores a cualquier dimensión.** Es lo único que reproduce lo medido. Un valor explícito se convierte con la fórmula de HFresh, para que el mismo número signifique el mismo umbral en los dos índices.
- **La alternativa, un default fijo de 76,** es más simple de explicar y deja la media en 236 solo a 1536d. Si se prefiere, hay que medir antes 768d con una media de 425.
- **El mínimo de fusión, T / 3:** 120 con T = 360. v6.0 lo mide con 118. v6.1 mide además una fusión por debajo de 177, tres cuartos de la media, si la suma no pasa de T: es la que devuelve el llenado.

**Dos avisos sobre la unidad:**

- **Es nominal.** Una entrada de miembro en disco son 208 B a 1536d, no 217, y un posting lleva además sus aristas: 36 B por miembro con el grado medido de 8.98, y 64 B como mucho. Un posting en el umbral pesa unos 86 KiB, no 76. A HFresh le pasa lo mismo en pequeño: su entrada real son 209 B.
- **El suelo de 192 vectores de HFresh** haría de umbral a 3072d con cualquier valor por debajo de 77 KB. Hay que decidir si se hereda.

### Comprobación 2: el modo de memoria

**Flat sí expone un parámetro de caché de vectores comprimidos.**

- **Se llama `cache`, es booleano y vive dentro del bloque de compresión:** `bq.cache` y `rq.cache` (`entities/vectorindex/flat/config.go:34` y `:40`). Default `false`.
- **Pide compresión:** `cache: true` sin `enabled: true` da el error «not possible to use the cache without compression» (`:199`).
- **Con la caché, los códigos están en RAM** y la búsqueda los recorre ahí. Sin ella, cada query recorre el bucket de códigos con un cursor. Los vectores exactos del rescore se leen de disco en los dos casos.
- **El tamaño lo acota `vectorCacheMaxObjects`** (`:46`), arriba del todo, el mismo nombre que en HNSW.
- **HNSW no tiene el flag:** sus vectores comprimidos van siempre a la caché.

**Los modos que hay que nombrar,** por qué códigos quedan residentes. A 1M y 1536d:

| modo | qué hay en RAM por vector | heap del índice | lo que cuesta | estado |
|---|---|---|---|---|
| ninguno: el halo | Nada. El sketch de cada endpoint viaja en el posting que lleva a él | 35.0 MB | De 1.3 a 1.6 veces la latencia en frío del modo de ruteo | Medido en v4.0 |
| el código de ruteo | 72 B: un código de 512 bits, con el que se ordenan los postings. El scan lee los códigos completos del posting | 106.0 MB | La referencia | Medido en v4.0. Es la celda sobre la que se miden los inserts |
| el código de scan y de ruteo: **descartado** | Un código de 512 o 768 bits que sirve para las dos cosas. En disco quedan las aristas y los vectores | Sin desglosar. En heap en uso, con unos 40 MB del arnés, 153.8 y 185.2 MB, donde el de ruteo son 145.8 | El store baja de 243.0 a 36.8 MB, un 3 % del disco. Pide más rescore: 200 o más | v4.1, cerrado el 20: no es una opción, y no tiene valor en el parámetro. Ver abajo |

**Tres nombres para el parámetro,** como enumerado:

1. **`rq.cache`,** con valores `none`, `routing` y `scan`. Es el nombre y el sitio de flat. El inconveniente: la misma clave es booleana en flat y sería una cadena aquí. Habría que rechazar un booleano con un error que diga los valores, porque los parseadores comunes ignoran en silencio un valor de otro tipo.
2. **`rq.cacheMode`,** con los mismos valores. Evita el choque de tipos y se lee como enumerado.
3. **`codeCache`,** arriba, al lado de donde HNSW y flat tienen `vectorCacheMaxObjects`. Dice qué se cachea: códigos, no vectores.

*Propuesta mía:* `rq.cache` si se acepta el choque de tipos con flat, y `rq.cacheMode` si no. El default, `routing`. El valor `scan` no debería existir hasta que v4.1 tenga veredicto.

**Acordado: `rq.cacheMode`,** con `routing` por defecto, `none`, y `scan` reservado.

**Cambiado el 20 (del usuario, a las 11:51): `scan` sale de la lista.** `rq.cacheMode` acepta `routing`, que es el valor por defecto, y `none`. Cualquier otro valor es desconocido.

- **Descartado, el modo `scan` (v4.1):** con el código de ruteo usado también para el scan, la mejor curva queda en frío a 0.95 a 1.49 veces del modo `routing`, con el límite en 1.20; usa más RAM y ahorra un 3 % de disco.
  - **Sin medir:** 768 bits con un rescore por debajo de 200, y 768d.
- **Por qué quitarlo y no reservarlo** (*mío, y el usuario de acuerdo*): un valor reservado es una promesa en la API. Añadir un valor más adelante es compatible; retirarlo no.
- **Para el parseo:** `scan` se rechaza como cualquier valor que no existe, con un error que diga los dos que hay. No es un «todavía no disponible».

### Comprobación 3: el ef dinámico

**HNSW lo tiene, y es su default.**

- **`ef` vale −1 por defecto** (`DefaultEF`, `entities/vectorindex/hnsw/config.go:29`).
- **Con cualquier `ef` menor que 1,** el ef de una query es `k × dynamicEfFactor`, acotado entre `dynamicEfMin` y `dynamicEfMax`, y nunca menor que k (`autoEfFromK`, `adapters/repos/db/vector/hnsw/search.go:63`). Con los defaults: 8 × k, entre 100 y 500.
- **HFresh no tiene equivalente.** Su único ajuste es `max(k, searchProbe)`.

**Aquí tiene sentido, y no está medido.**

- **A favor: el ef de este índice se compara con k.** La búsqueda para cuando el mejor posting sin cargar no mejora al ef-ésimo candidato, y un ef menor que k se sube a k. Con el default fijo de 20, una query con k = 50 corre con ef = k, que es la parada más temprana posible. A k = 10 esa parada da un recall de 0.92, no de 0.95. Un ef fijo cambia el objetivo de recall según k sin decirlo.
- **A favor: es la convención de HNSW,** y un usuario que venga de ahí la espera.
- **En contra: todo lo medido es con k = 10.** El default de 20 es 2 × k, y no sé si ese factor vale con k = 1 o con k = 100.

**Acordado: el `ef` dinámico no entra todavía. Pendiente antes de integrar: medir k = 1 y k = 100,** para decidir si hace falta y con qué factor. Hoy solo hay curvas con k = 10.

*Mi propuesta, para cuando esté esa medida:*

- **Reservar los nombres de HNSW** (`ef: -1`, `dynamicEfMin`, `dynamicEfMax`, `dynamicEfFactor`) y la semántica de `autoEfFromK`.
- **No fijar los valores sin medir.** El punto de partida sería un mínimo de 20 y un factor de 2, que con k = 10 es el default de hoy. Antes de darlo por bueno: un barrido de k (1, 10 y 100) que diga qué ef da 0.95 en cada uno.
- **Mientras tanto, el default sigue siendo `ef: 20`.**

**Medido el 21 (`bench-report.md`, «Parámetros»): no hace falta un factor por k.** En dbpedia 1M, con la búsqueda de las celdas de disco:

| k | rescore | 0.95: ef | cargas | 0.99: ef | cargas |
|---|---|---|---|---|---|
| 1 | 64 | 18 | 16.8 | 283 | 102.8 |
| 10 | 64 | 20 | 18.0 | 188 | 76.9 |
| 100 | 100, que es k | no llega: techo 0.815 | | no llega | |
| 100 | 200 | 144 | 63.9 | no llega: techo 0.973 | |
| 100 | 400 | 100, que es k | 49.9 | 444 | 141.0 |

- **Propuesta mía, con el dato:** `ef` 20 por defecto, y `ef` efectivo = el mayor entre `ef` y k, que es lo que la búsqueda ya hace. En los nombres de HNSW, `dynamicEfMin` 20 y `dynamicEfFactor` 1.
- **Lo que k = 100 necesita es rescore, no `ef`:** con el rescore en k el recall no pasa de 0.815. Va en la regla de `rq.rescoreLimit`, abajo.
- **Sin medir:** k entre 10 y 100, y por encima de 100.

### El valor automático de `rq.rescoreLimit`

**Acordado:** vive en `rq.rescoreLimit`. En el modo `routing` no se acopla a `ef`. Y el automático no puede ser un 64 fijo.

**Lo medido.** El recall tiene un techo que pone el `rescoreLimit`. La tabla da ese techo, el recall a ef 1 024 subiendo solo ef, por los bits del código. Todo con k = 10 y en coseno. Sale de «v1.2, Fase B» y de «v1.4» en `bench-report.md`.

| bits del código | de dónde | con 64 | 200 | 400 | 800 | 1 600 | 3 200 |
|---|---|---|---|---|---|---|---|
| 1 536 | dbpedia 1536d, el código completo | 0.9960 | 0.9963 | — | — | — | — |
| 768 | wiki-dpr 768d, el código completo | 0.9816 | 0.9961 | 0.9978 | — | — | — |
| 768 | dbpedia 1536d, capado | 0.9749 | 0.9937 | 0.9958 | 0.9963 | — | — |
| 512 | wiki-dpr 768d, capado | 0.9176 | 0.9772 | 0.9913 | 0.9959 | — | — |
| 512 | dbpedia 1536d, capado | 0.9255 | 0.9836 | 0.9910 | 0.9947 | 0.9960 | 0.9963 |
| 384 | wiki-dpr 768d, capado | 0.8436 | 0.9411 | 0.9706 | 0.9863 | — | — |
| 256 | dbpedia 1536d, capado | 0.7521 | 0.8867 | 0.9369 | 0.9692 | 0.984 | 0.991 |

- **Cuentan los bits, no la dimensión.** Las dos filas de 768 bits se parecen, y las dos de 512 también.
- **El `rescoreLimit` que deja el techo en 0.99 crece con el cubo de lo que se acorta el código.** Interpolando entre columnas: de 100 a 140 con 768 bits, unos 355 con 512, unos 1 070 con 384 y unos 2 800 con 256. Es 120 × (768 / bits)³, con un error de un 15 % como mucho.
- **Con 1 536 bits esa ley daría 15.** Lo único medido ahí es 64 y 200, y los dos dejan el techo donde lo deja el índice, en 0.996.

*Propuesta mía,* para `rq.rescoreLimit: -1`:

**El automático es el mayor entre 64 y 200 × (768 / bits)³,** con los bits del código con el que se hace el scan. El efectivo nunca baja de k.

| bits del scan | 384 | 512 | 768 | 1 024 | 1 536 | 3 072 |
|---|---|---|---|---|---|---|
| automático | 1 600 | 675 | 200 | 85 | 64 | 64 |
| lo medido más cerca | 800 no llega: 0.9863 | 400 llega justo, a ef 640 y 1 024. 800, a ef 320 y 640 | 200 llega: a ef 160 a 768d, y a ef 640 capado | nada | 64 llega: a ef 188 | nada |

- **Cumple «0.99 alcanzable subiendo solo `ef`» en los dos datasets:** con 200 a 768d (0.9904 en ef 160) y con 64 a 1536d.
- **No gasta lecturas de más a 1536d:** se queda en el 64 de hoy.
- **El ancla de 200 lleva margen sobre el mínimo interpolado,** de 1.5 a 2 veces. Un techo de 0.99 justo obliga a un ef muy alto: con 512 bits y 400 hace falta ef 1 024.
- **En `routing` y en `none`,** los bits del scan son los de la dimensión, redondeados a 64.
- **Un valor explícito se respeta tal cual,** con k como único suelo.

**Medido el 21 (`bench-report.md`, «Parámetros»): el ancla a 768 bits baja a 150, y hace falta un término por k.** En wiki-dpr 1M a 768d, con la búsqueda de las celdas de disco:

| rescore | 0.95: ef | cargas | 0.99: ef | cargas | techo |
|---|---|---|---|---|---|
| 64 | 35 | 22.5 | no llega | | 0.9815 |
| 100 | 29 | 19.6 | 704 | 195.0 | 0.9913 |
| 150 | 28 | 19.3 | 198 | 74.9 | 0.9958 |
| 200 | 27 | 18.7 | 159 | 64.4 | 0.9973 |

- **Propuesta mía, con el dato: el automático es el mayor entre 64, 150 × (768 / bits)³ y 4 × k.** Con k = 10: 1 200 con 384 bits, 506 con 512, 150 con 768 y 64 de 1 024 en adelante.
- **150 es el menor rescore que cruza 0.99 antes de ef 320.** Frente a 200 lee un 20 % menos por query a 0.95 y un 9 % menos a 0.99.
- **El término de 4 × k sale de un punto,** k = 100 a 1 536 bits: con 2 × k el 0.99 queda fuera y con 4 × k entra.
- **Validada solo a 768 y a 1 536 bits** (*decisión del usuario del 21*). A 768 bits con wiki-dpr 1M y a 1 536 con dbpedia 1M. **Por debajo de 768 bits es una extrapolación** de la forma cúbica: con 512 bits da 506, donde lo medido con códigos capados es que 400 llega a 0.99 solo con ef 640 o más.
- **Pendiente explícito antes de publicar el índice: un barrido a 1M con un dataset nativo de 384 o 512 dimensiones,** rescore frente a `ef` hasta llegar a 0.99, que diga cuánto rescore pide de verdad. Hoy no hay en disco ninguno de ese tamaño: los únicos de 384d son `custom-fiqa-12k` y `custom-fiqa-48k`, de 12 000 y 48 000 vectores, y el rescore que hace falta crece con el corpus. De 512d no hay ninguno.
- **A 10M la regla queda del lado seguro** (medido el 22, «10M en disco» en `bench-report.md`). Con 768 bits, 64 llega a 0.99 en ef 315, donde a 1M su techo era 0.9815. Con 150, que es lo que da la regla, la celda de ruteo es un 29 % más lenta en frío a 0.95 que con 64, y un 25 % más rápida a 0.99. La regla no mira el tamaño del corpus. *Propuesta mía:* dejarla como está, porque 150 cumple a 1M y a 10M, y no añadirle el tamaño con solo dos medidos.
- **El 10M de esta noche corre nuestras celdas con 150,** con 200 y 400 de reserva si no llegara a 0.99: dirá si el ancla depende del tamaño del corpus.

**Qué puntos faltan para fijarla:**

1. ~~Dónde cruza 0.99 con 768 bits, entre 64 y 200.~~ *Medido el 21:* con 100 llega tarde, en ef 704; con 150, en ef 198.
2. **Por debajo de 64 con 1 536 bits.** El suelo de 64 está ahí porque no hay nada medido por debajo, y la ley dice que bastarían unos 25. A 0.95 el rescore son 64 de las 101 lecturas de una query: es donde más habría que ahorrar.
3. **Una anchura entre 768 y 1 536,** por ejemplo 1 024 bits. La regla da 85 sin ningún dato cerca.
4. **Un dataset nativo de menos de 768d.** Los puntos de 512, 384 y 256 bits son códigos capados de vectores de 768d y de 1536d.
5. ~~Otros k.~~ *Medido el 21 con k = 1 y k = 100 a 1 536 bits:* hace falta 4 × k. Entre 2 × k y 4 × k, y a 768 bits, sin medir.
6. **Un segundo dataset a 1536d.** El 64 sale solo de dbpedia.

**Pendiente antes de integrar,** junto a k = 1 y k = 100:

- **Un barrido corto, sin cronometrar, a 768d, con rescore 64, 100, 150 y 200.** Fija el ancla, que es el punto 1. Hace falta.
- *Propuesta mía, para el mismo barrido:* 16, 32 y 48 a 1536d, que es el punto 2. Sin él la regla es correcta a 1536d y quizá cara.

**Dos avisos:**

- **Por debajo de 768 bits el rescore se dispara:** 675 lecturas con 512 bits y 1 600 con 384. *Lectura mía:* ahí un código de 1 bit por dimensión se queda corto, y habría que mirar un código de más bits antes que un rescore tan ancho.
- **El modo `scan`, descartado el 20, habría necesitado además un suelo que suba con `ef`.** Con un código corto, pasado el techo, subir `ef` baja el recall: con 256 bits y 400, de 0.9516 a 0.9397. Mi propuesta anterior ponía ese suelo en la mitad del `ef`. Sale de dos puntos y no está medido como regla: queda para cuando ese modo tenga veredicto.

## 5. Orden de trabajo propuesto

*Todo este orden es propuesta mía.* Va en pasos, cada uno con lo que necesita antes.

**Paso 0. Decisiones, que bloquean nombres y formato.**

| decisión | bloquea | de qué depende |
|---|---|---|
| El nombre del tipo | La configuración en `entities`, los nombres de buckets y directorios, todo el registro | De nada |
| Dónde viven los vectores exactos: bucket de objetos o fichero propio | El formato en disco, el backup y la replicación | De una medida con objetos reales, que no hay |
| Un formato en disco o dos: el que crece (v5.0) y el del lote (v4.0) | El camino de lectura y la reorganización | De si la reorganización reescribe al formato del lote |
| La regla del `rescoreLimit` automático | El default de `rq.rescoreLimit` | **De una medida pendiente: el barrido a 768d con rescore 64, 100, 150 y 200.** El resto de la tabla de parámetros está acordado |
| El `ef` dinámico | El default de `ef` para k distinto de 10 | **De una medida pendiente: k = 1 y k = 100** |
| Qué fusión devuelve el llenado sin quitar las fuentes | El paso 4, si la primera versión tiene que aguantar más de un 10 % de borrados | De una medida que no existe: la celda 1b con fusión |

**Paso 1. La configuración y el registro del tipo.** Solo necesita el nombre y la tabla de parámetros.

- La configuración en `entities/vectorindex/`, con su parseo y su validación de cambios.
- El tipo en el despacho y en las listas cerradas de la sección 1.
- La rama en `initVectorIndex`, los directorios y los nombres de artefactos.
- **Puede ir en paralelo con los pasos 2 a 4.** No sirve para nada visible hasta que el índice inserte y borre: detrás de un flag o fuera de `ALLOWED_VECTOR_INDEX_TYPES`.

**Paso 2. El camino de lectura, al paquete.** Necesita las decisiones de formato.

- El formato, la apertura y la búsqueda por rondas de `disk_test.go`, con los buckets nombrados por el id del índice.
- El estado residente, restaurado al abrir.
- El rescore, tolerante a un vector que falta.
- Una búsqueda sobre un índice vacío, que devuelve vacío.

**Paso 3. El camino de escritura, al paquete.** Necesita el paso 2.

- El insert de `grow_test.go`: buscar, aplicar, aristas inversas y split.
  - **Con la forma de v5.2, desde el hito 1** (*decisión del usuario del 20*): un paso de cálculo, que busca las aristas del vector nuevo y no escribe nada (`plan`), y un aplicador que escribe los resultados en orden (`apply`). En el hito 1 corren en serie, uno detrás de otro por cada insert.
  - **El paso de cálculo es el que se llevará a los workers compartidos del nodo** (paso 5). Es el 92 % del tiempo de un insert y solo lee, así que es lo que se puede repartir. El aplicador se queda como único escritor: aplicar un insert son 0.30 ms.
- Empezar vacío: el primer posting y el entrenamiento del cuantizador.
- La durabilidad: qué está escrito cuando `Add` devuelve.
- `Flush`, `Shutdown` y `Drop` de verdad.

**Paso 4. Los deletes mínimos.** Necesita el paso 3. La marca es la de la celda 1b de v6.0.

- La marca guardada, `Delete` sin error para ids desconocidos y `ContainsDoc`.
- Con esto un shard ya puede hacer updates: sin `Delete`, ninguna clase con este índice admite un cambio de vector.

**Paso 5. Los recursos compartidos.** Necesita los pasos 3 y 4.

- El split, el refresco, la limpieza y la reorganización, como tareas de una cola del scheduler del nodo.
- **El paso de cálculo de los inserts, en esos mismos workers** (`queue.Scheduler`, `GOMAXPROCS − 1` para todos los índices del nodo). El insert ya está partido en cálculo y aplicador desde el hito 1, así que este paso cambia quién ejecuta el cálculo y no su forma.
  - **Lo medido es el mejor caso:** con 4 y con 8 buscadores propios de un solo índice, 2.74 y 3.60 veces los inserts por segundo de uno, en tandas de 32: 505 y 663 frente a 184. Es la medida limpia, de la noche del 20 y sin actividad; de día salieron 3.0 y 3.76, con el denominador medido con actividad. Ocho búsquedas a la vez tardan un 55 % más cada una. Con los workers repartidos entre índices le tocan menos a cada uno.
- La limpieza medida es la de v6.2: quitar los borrados sustituyendo cada arista muerta por un vecino de la lista del borrado, y fusionar por debajo de 177 vivos si la suma no pasa de T. Cumple sin búsquedas en un churn de treinta rondas del 3 % (v6.2b) y con el 30 % borrado por regiones. Cada pasada toca todos los postings: falta medir lo que cuesta en disco y decidir cada cuánto pasa.
- Las cargas de una ronda, dentro del presupuesto de la query.
- **Antes de este paso, ninguna medida vale como medida de un nodo.**

**Paso 6. El reinicio y el backup.** Necesita los pasos 2 y 3, y la decisión de los vectores.

- `PostStartup`, `ListFiles`, `PrepareForBackup` y `ResumeAfterBackup`.
- Los nombres para el borrado en frío y la carga perezosa.

**Paso 7. Lo que falta para que una clase normal funcione.** Necesita el paso 4.

- Los filtros mínimos: búsqueda exacta por debajo del corte y filtro en el scan por encima.
- `SearchByVectorDistance`.
- Las métricas de Prometheus y las anotaciones del log de queries lentas.

**Paso 8. La reorganización en segundo plano.** Necesita los pasos 3 y 5.

- Es lo que deja el índice crecido a 1.07 veces el lote en frío. Sin ella queda en 1.33.
- No está diseñada. Puede ir después de una primera versión si se acepta esa latencia.

**Paso 9. Tests de aceptación, y medir como nodo.** Necesita todo lo anterior.

- Los tests de aceptación que hoy nombran a HFresh, con este tipo.
- Las curvas de siempre, a través de un shard, con objetos reales y en Linux.

**Lo que bloquea a qué, en una línea:** las decisiones bloquean el formato; el formato bloquea leer; leer bloquea escribir; escribir bloquea borrar; y sin borrar no hay shard que funcione. El registro del tipo es lo único que puede empezar ya.

**Pendiente de medir antes de integrar:**

- **k = 1 y k = 100,** para decidir el `ef` dinámico. Con el `ef` fijo en 20, una query con k = 100 corre con ef = k, la parada más temprana.
- **El `rescoreLimit` con k = 100:** 64 pasa a 100 y no deja margen de reordenación. Sale de la misma medida.
- **Un barrido corto, sin cronometrar, a 768d, con rescore 64, 100, 150 y 200,** para fijar el ancla del `rescoreLimit` automático. *Propuesta mía, para el mismo barrido:* 16, 32 y 48 a 1536d.

**Medido el 20, en v6.2:** el umbral de reparación acumulado, en el churn, y dos alternativas. Cumplen las tres; la más barata es la sustitución local, sin búsquedas. **Y en v6.2b:** la sustitución local se sostiene en treinta rondas y con el borrado por regiones; la red de búsqueda no pasa a ser obligatoria.

**Lo que está en vuelo y puede mover este plan:**

- **v4.1,** el scan con los códigos en RAM, cerrado el 20: el modo `scan` no es una opción a 1M y 1536d. Descartado y fuera de la lista de parámetros.
- **v5.2,** inserts con búsquedas en paralelo, cerrado el 20: 634 inserts por segundo con 8 buscadores, 3.76 veces los 169 de un escritor en la misma sesión. No llega a las 4 veces de su criterio. Repetido de noche y sin actividad: 663 con 8 buscadores, 3.60 veces los 184 de un escritor, que es el número limpio. HFresh, con un escritor y sus workers de fondo, hace 2 089.
- **La sesión de la noche del 19,** con HFresh configurado como un nodo, hecha: en frío a 0.95 con 1 thread, 51.96 ms de HFresh frente a 3.54 del modo de ruteo y 4.76 del halo. Son los números de referencia frente a HFresh.
- **El control combinado,** cerrado el 20: el índice de aristas consolidadas con K = 64, reorganizado, cumple en caliente (1.20 veces el lote) y no en frío (1.2045, con el límite en 1.20). No es el formato candidato. El de v5.0 reorganizado cumple al revés: 1.10 en frío y 1.34 en caliente.
- **v5.3,** cerrado el 20: la fila de aristas en un valor, en el bucket de los miembros y junto a ellos, tiene las dos mitades. Reorganizado es el formato candidato: 1.10 veces el lote en frío y 1.21 en caliente, a 0.95 con 1 thread.
  - **La disposición en disco a la que apuntar es esa.** El almacenamiento de aristas sigue detrás de su interfaz.
  - **La forma de escribirla, todavía no:** reescribir la fila con una lápida de Set hace que el store escriba 17.1 GB al crecer, casi el doble que v5.0, y que entre compactaciones una carga lea de 2.2 a 2.6 veces lo que usa.
  - **En marcha como v5.4** (*encargo del usuario del 21*): la misma disposición con un bucket Map, donde la fila se reemplaza por su clave, sin lápida que lleve la fila. La otra opción, un Set de `lsmkv` que admita reemplazar sin una lápida por valor, queda sin escribir.
  - **v5.4, cerrado el 21: no sustituye a v5.3.** Sobre un Map la misma disposición cumple en frío y en caliente (1.11 y 1.23 veces el lote), escribe 11.78 GB donde v5.3 escribe 17.08, y no baja de los 10 GB ni de 1.5 veces lo leído sin compactar (1.57). Lo que sobra es de compartir bucket con los miembros, no de la lápida. La pieza 1 del hito 2 sigue con el formato de v5.3.

**Diferencias del índice integrado frente a v5.0:**

- **El recall a ef 21 del índice integrado a 1M, 0.9523: cerrado** (*decisión del usuario del 21, a las 12:15: cinco corridas del mismo algoritmo ya se separan 0.0014 entre sí, así que no había nada que explicar*). Es la corrida A de la sesión de integración, con la base forzada a 99 000, y queda 0.0014 y 0.0018 por encima de los dos índices de v5.0, 0.9509 y 0.9504. Las cargas coinciden, 19.63, y a 0.99 coincide todo.
  - **Hipótesis, sin comprobar: el orden de la base.** El índice integrado construye su base con los vectores en orden de doc id, y v5.0 en orden de llegada. Es otra base, con los mismos 420 postings. La búsqueda es la misma función con los mismos parámetros.
  - **Lo que ya hay medido del ruido entre corridas, sin correr nada más:** los índices crecidos del 10 al 100 % con el insert de siempre, desde una base en lote de 99 000 en orden de llegada, dan a ef 21 0.9502 (v5.3, su validación), 0.9504 y 0.9509 (v5.0), 0.9513 (N = 1 de v5.2, de día) y 0.9516 (N = 1 de v5.2, de noche). Son 0.0014 entre cinco corridas del mismo algoritmo: el margen de 0.001 que se daba por ruido era corto. El 0.9523 queda 0.0007 por encima de la mayor. No lo explica, y no descarta la hipótesis.
  - **No se corre nada más por esto.** La hipótesis del orden de la base queda sin comprobar, y no hace falta: la diferencia cabe en el ruido entre corridas.
  - **La corrida B,** con la base por defecto de 10 000 y reapertura a 1M, sigue, y avisa de cualquier otra diferencia con v5.0.
