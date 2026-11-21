# Plan de integración en Weaviate

**Fecha:** 2026-11-19 · **Rama:** `feat/posting-edge-index`, leída en `c74cbd28cf` · **Paquete:** `adapters/repos/db/vector/postingedge` (nombre provisional)

**Alcance.** Solo lectura y este documento: el plan no cambia código del índice. Al leer apareció un bug en flat y en HFresh, que sí se arregló, en un commit aparte (`665f3a1806`). El plan dice qué pide Weaviate a un tipo de índice, qué de eso existe ya, qué vive en ficheros de test y qué falta. Las cifras salen de `bench-report.md` y de `disk-design.md`; las rutas, del código de esta rama. Lo que es propuesta mía va marcado como tal.

**La tabla de parámetros.** El encargo pedía en el punto 4 «la tabla de arriba», que no venía en el mensaje: la primera versión de la sección 4 la monté con los parámetros que el mensaje nombraba y los de la configuración del paquete. La lista acordada llegó después, el 19 por la tarde. La sección 4 empieza ahora por ella, con las diferencias marcadas.

## Resumen

- **El índice ya cumple la interfaz del shard a nivel de tipos,** y de sus 23 métodos tiene hechos los de búsqueda sin filtro, validación y configuración. Faltan los que hacen de él un índice vivo: insertar después del build, borrar, persistir, reiniciar y entrar en un backup.
- **Casi todo lo que se ha medido en disco es código de test.** El paquete tiene 3 216 líneas de código y 27 165 de test. La lectura de disco, los inserts y los deletes viven en `disk_test.go`, `grow_test.go`, `stream_test.go` y `delete_test.go`.
- **Registrar el tipo en Weaviate es mecánico y disperso:** unos quince sitios, sin una lista única. Es lo único que puede empezar ya.
- **Lo que bloquea de verdad es `Delete`.** El shard lo llama en cada borrado y en cada update que cambia un vector. Sin él, ninguna clase con este índice admite cambios.
- **Los deletes tienen una celda que cumple con el 30 % borrado, en una pasada de limpieza (v6.1).** Con pasadas pequeñas y frecuentes, en un churn, todavía no.
- **Un filtro hoy da error, y eso falla la query entera.** La opción mínima es una búsqueda exacta sobre la lista por debajo de un corte, y filtrar en el scan por encima. Sin medir.
- **Dos decisiones abiertas condicionan el formato:** dónde viven los vectores exactos, y si hay un formato en disco o dos.
- **En parámetros,** lo acordado son seis: `distance` (solo `cosine` en la primera versión), `ef`, `efConstruction`, `rq.rescoreLimit`, `maxPostingSizeKB` y `rq.cacheMode`, que acepta `routing` (por defecto) y `none`; el modo `scan` salió de la lista el 20, descartado por v4.1. El `rescoreLimit` automático no se acopla a `ef` en el modo `routing`: sale de los bits del código de scan, 64 a 1536d y 200 a 768d.
- **Pendiente antes de integrar:** medir k = 1 y k = 100, para decidir el `ef` dinámico, y un barrido corto a 768d con rescore 64, 100, 150 y 200, para fijar la regla del `rescoreLimit` automático.
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
- **Inserts y búsquedas a la vez.** Todo lo medido tiene un escritor y ninguna búsqueda mientras escribe. v5.2 pone en paralelo las búsquedas de los propios inserts: 3.76 veces con 8 buscadores, con el aplicador en un 19 % del tiempo. Las queries mientras se inserta siguen sin medir.
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

**Lo que hay medido:** v6.0 y v6.1, en memoria y en conteos, cerrados el 19. En disco, nada.

- **Por su criterio, los deletes no se sostienen** con ninguna de las cinco celdas, con el 30 % borrado al azar.
- **La celda 1b es la única que cumple el recall a igual ef:** un borrado se sigue puntuando y sus aristas se siguen, y nunca es candidato. Falla solo en cargas, +19.1 % a 0.95 y +13.7 % a 0.99, y ese fallo es de llenado: se atribuye a la política de fusión, no a las aristas.
- **Con el 10 % borrado la celda 1b cumple.**
- **v6.1, la limpieza en dos fases, cumple con el 30 % borrado al azar en una pasada.** Primero repara los vectores con el 25 % o más de sus aristas hacia borrados, navegando por ellos. Después los quita y fusiona por debajo de 177 vivos si la suma no pasa de T. Queda en −0.0023 de recall, +2.7 % de cargas a 0.95 y −10.7 % a 0.99.
- **Cuesta 66 cargas por vector borrado,** casi dos inserts.
- **En el churn esa pasada no cumple:** −0.0071 de recall y +16.9 % de cargas a 0.99, con un 3 % borrado por ronda. El umbral se mide en cada pasada y no acumula lo perdido en las anteriores: las listas pasan de 8.6 a 7.2 aristas por vector.

*Lo mínimo para una primera versión, por lo leído:*

- **La marca de v6.0, guardada con el índice, con la semántica de la celda 1b:** un bit por vector, y el borrado sigue ruteando mientras esté en su posting. `Delete` la pone y no falla con ids desconocidos. `ContainsDoc` da `false` para un id marcado.
- **El rescore salta un vector que falta.**
- **La fusión que devuelve el llenado, medida en v6.1 dentro de la pasada de dos fases.** Por debajo de 177 vivos, con el vecino más cercano, si la suma no pasa de T: deja la media en 235.1, con la referencia en 236.0.
- **No está medida sobre la celda 1b sola.** Reescribir un posting sin sus borrados le quita las fuentes, y por eso la pasada repara antes. Sin pasada, una primera versión con la marca sola carga un 19 % más a 0.95 con el 30 % borrado, y cumple con el 10 %.
- **La pasada no está lista para un churn continuo.** Le falta que el umbral de reparación cuente lo que una lista ha perdido desde que se construyó. Sin medir.

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

**Qué puntos faltan para fijarla:**

1. **Dónde cruza 0.99 con 768 bits, entre 64 y 200.** Es el ancla de la regla y cae en un hueco sin medir. La interpolación dice que entre 100 y 140. Si 150 deja margen, son 50 lecturas menos por query a 768d.
2. **Por debajo de 64 con 1 536 bits.** El suelo de 64 está ahí porque no hay nada medido por debajo, y la ley dice que bastarían unos 25. A 0.95 el rescore son 64 de las 101 lecturas de una query: es donde más habría que ahorrar.
3. **Una anchura entre 768 y 1 536,** por ejemplo 1 024 bits. La regla da 85 sin ningún dato cerca.
4. **Un dataset nativo de menos de 768d.** Los puntos de 512, 384 y 256 bits son códigos capados de vectores de 768d y de 1536d.
5. **Otros k.** Todo es con k = 10, y no sé si el valor escala con k. Sale de la medida pendiente de k = 1 y k = 100.
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
  - **Lo medido es el mejor caso:** con 4 y con 8 buscadores propios de un solo índice, 3.0 y 3.76 veces los inserts por segundo de uno, en tandas de 32. Ocho búsquedas a la vez tardan un 54 % más cada una. Con los workers repartidos entre índices le tocan menos a cada uno. *La medida limpia de N = 1, 4 y 8, de noche y sin actividad, está en cola para la noche del 20.*
- La limpieza medida es la pasada de v6.1: reparar y después eliminar. Cumple en una pasada sobre el 30 % borrado, y con pasadas pequeñas y frecuentes todavía no.
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

**Sin medir, de v6.1** (*mío*; no está acordado como pendiente): el umbral de reparación acumulado, en el churn. Es lo que separa la pasada medida de una tarea de mantenimiento continua.

**Lo que está en vuelo y puede mover este plan:**

- **v4.1,** el scan con los códigos en RAM, cerrado el 20: el modo `scan` no es una opción a 1M y 1536d. Descartado y fuera de la lista de parámetros.
- **v5.2,** inserts con búsquedas en paralelo, cerrado el 20: 634 inserts por segundo con 8 buscadores, 3.76 veces los 169 de un escritor en la misma sesión. No llega a las 4 veces de su criterio. HFresh, con un escritor y sus workers de fondo, hace 2 089.
- **La sesión de la noche del 19,** con HFresh configurado como un nodo, hecha: en frío a 0.95 con 1 thread, 51.96 ms de HFresh frente a 3.54 del modo de ruteo y 4.76 del halo. Son los números de referencia frente a HFresh.
- **El control combinado,** encargado el 20: el índice de aristas consolidadas con K = 64, reorganizado. Decide el formato de las aristas, que en el hito 1 de la integración va detrás de una interfaz.

## 6. Hito 1 de la integración (2026-11-20): qué se hizo y qué cambió respecto de este plan

*Rama `feat/posting-edge-integration`, salida de `feat/posting-edge-index` en `423b7214d1`. Lo que cuenta esta sección está en esa rama. Las secciones 1 a 5 describen el paquete de antes del hito: donde chocan con esta, manda esta.*

**Qué hay.** Una clase puede crearse con `vectorIndexType: "postingedge"`. El shard abre el índice sobre su propio store, vacío, y el índice inserta, busca, borra con la marca guardada y vuelve como estaba tras un reinicio. Cubre los pasos 1 a 4 del orden de la sección 5 y, del paso 5, solo el presupuesto de la query.

**Lo que cambió respecto del plan.** *Lo marcado como mío lo decidí al integrar. El usuario lo confirmó el 20 a las 14:15: el nombre `postingedge`, provisional; los vectores en fichero propio, con su coste en disco anotado; la base de 10 000; el tope de 1 600; el suelo de 192; el cerrojo; y la parada si una escritura falla a medias.*

- **El índice del shard es un tipo nuevo, `postingedge.Persistent`** (`persistent.go`, `postingedge.Open`). `Index` sigue siendo el índice en memoria construido en lote: lo usan los experimentos, y `Persistent` lo usa para su primer lote.
- **Empezar vacío** (*mío*). Los primeros vectores se guardan aparte, en un bucket propio, y se buscan de forma exacta. Al llegar a 10 000 (el límite de entrenamiento de RQ centrado) se construye con ellos el primer lote, con el build en lote, y desde ahí todo entra por inserts. Es el camino que midió v5.0, con una base de 10 000 en vez del 10 % del corpus.
  - **El insert que completa la base paga ese build entero,** dentro de la petición: no hay tareas de fondo.
  - **Para el hito 2 (del usuario, 20): el build de la base no puede ir dentro de un insert.** Mientras dura, ese insert no vuelve y las búsquedas esperan al cerrojo. Tiene que ser una tarea de fondo, con los inserts y las búsquedas servidos de forma exacta mientras tanto.
  - **Crecer cien veces desde una base de 10 000 no está medido.** v2.2 midió crecer diez veces desde el 10 %.
  - **Una colección de menos de 10 000 vectores se queda en búsqueda exacta,** leyendo sus vectores del bucket en cada query.
- **Los vectores exactos viven en un fichero propio,** por doc id, como en v5.0 (*mío*). Con la celda 1b el split necesita el vector de un miembro borrado, que el bucket de objetos ya no tiene.
  - **Lo que cuesta en disco:** 4 B por dimensión y por doc id, 6 144 B a 1536d, que son 6.1 GB por cada millón de vectores. Es una segunda copia: el bucket de objetos guarda la suya con cada objeto.
  - **Crece con los doc ids emitidos, no con los vivos.** El vector de doc id d está en la posición d, así que un borrado deja su hueco, y un update, que gasta un doc id nuevo, también. Nada lo recupera en este hito.
- **El insert son dos pasos** (del usuario, 20): `plan`, que solo lee, y `apply`, que escribe en orden. En el hito corren en serie, un insert cada vez.
- **Concurrencia** (*mío*): un cerrojo de lectura y escritura. Las búsquedas y el paso de cálculo de un insert corren a la vez. El aplicador, un delete y la construcción de la base excluyen a las búsquedas mientras duran. Sin medir.
- **Las aristas van detrás de una interfaz** (`growEdges`, en `grow_edges.go`). La implementación del índice es la de v5.0, un valor por lista. La consolidada de v5.1 y v5.3 se queda en test (`grow_rows_test.go`) y los arneses la siguen usando.
- **La configuración pública es nueva y vive en `entities/vectorindex/postingedge`.** El `UserConfig` del paquete del índice no se movió: se queda como configuración interna, con los ajustes de los experimentos.
- **`rq.cacheMode`:** `routing` funciona, `none` es un valor válido que responde «todavía no disponible», y cualquier otro, también `scan` y un booleano, es desconocido.
- **`rq.rescoreLimit` automático:** la regla de la sección 4, con un tope en 1 600, lo que da a 384 bits (*mío*). Por debajo no hay medidas y la regla crece sin límite. El insert usa el mismo límite que las queries.
- **`maxPostingSizeKB` explícito** hereda el suelo de 192 vectores de HFresh (*mío*). El plan lo dejaba por decidir.
- **El código de ruteo son 512 bits** donde el código completo es más ancho, y el código completo donde no.
- **El rescore salta un candidato cuyo vector no se encuentra,** como HNSW y HFresh. Cualquier otro fallo de lectura sigue fallando la búsqueda.
- **Sin flag propio** (*mío*): el tipo se limita con `ALLOWED_VECTOR_INDEX_TYPES`, como HFresh. Sin esa variable, el tipo se puede usar.
- **Los buckets están en el store del shard,** con el id del índice en el nombre: `postingedge_{postings,map,routing,edgedists,deleted,staging}_<id>`. El shard los vuelca y los compacta con los ciclos del nodo.
  - **El de `staging` solo existe mientras no hay base.** Al construirla sale del store y del disco: borrados uno a uno, los vectores apartados se quedaban en sus segmentos bajo sus lápidas, 573 MB con una base de 99 000 a 1536d (`a5a487d98f`).
- **Una escritura que falla a medias deja el índice parado** (*mío*): lo dice en cada llamada hasta que el shard se reinicia, en vez de servir algo que no sabe si es un índice.
- **`PostingEdgeBaseSize`** es un ajuste interno de la base de datos, para tests: ninguna variable de entorno lo fija.

**Qué fichero de test se vació y dónde quedó cada cosa.**

| estaba en | qué | ahora está en |
|---|---|---|
| `grow_test.go` | El índice que crece en disco: `growIndex`, `growMeta`, `growStats`, `growPlan`, `writeGrowIndex`, `openGrowIndex`, `fetch`, `plan`, `apply`, `insert`, `insertBatch`, `planBatch`, `refresh`, `enterNeighbors`, `split`, `sync`, `close` | `grow.go`, el mismo fichero renombrado |
| `grow_test.go` | La lectura y la escritura de las listas de v5.0, y `joinEdges` | `grow_edges.go`, en `growLists` |
| `grow_test.go` | Las aristas consolidadas: `growEdgesBlock`, `getRow`, `putRow`, `dropRow`, `setPending`, `dropPending`, `writeRow`, y la rama consolidada de `joinLists` | `grow_rows_test.go`, en `growRows` |
| `grow_test.go` | `cleanCentroidGraph`, `checkCoverage`, `sameAs`, `reorganizeGrowIndex`, `sizeOnDisk` | `grow_check_test.go` |
| `disk_test.go` | Las constantes del formato, `diskKey`, `diskCodecs`, `writeArray`, `readArray`, `openCentroidGraph`, `writeCentroidGraph`, `diskPosting`, `diskRound`, `diskSearchStats`, `fetchedSearch` y su `run`, `scoreHalo` | `disk.go` |
| `halo_test.go` | `haloState` | `disk.go` |
| `stream_test.go` | La constante `unplaced` | `disk.go` |

- **Se quedan en test:** el formato en lote de v4.0 (`writeDiskIndex`, `openDiskIndex`), el halo, el gemelo en memoria (`streamIndex`) y los deletes en memoria (`deleteIndex`).
- **Los `bench_*_test.go` no se tocaron.** Los campos de `growIndex` que solo usa la variante consolidada siguen ahí, porque los arneses los leen.

**Lo que no está, y da error o falta sin más:**

- **Error explícito:** filtros, búsqueda por distancia, backup, multi-vector, distancias que no sean coseno.
- **Limpieza de deletes.** Un borrado se marca y nada más: ocupa su sitio y cuesta trabajo en cada búsqueda. Es la celda 1b sola, que con el 30 % borrado carga un 19 % más a 0.95.
- **Durabilidad ante una caída a mitad de un insert.** Lo comprobado es una caída entre dos peticiones: una copia de los ficheros tomada tras un `Flush` abre como el índice que era. A mitad de un insert no hay garantía, y el índice puede negarse a abrir.
- **Mantenimiento del grafo de centroides.** Cada split deja un centroide borrado en el grafo y nada lo limpia, ni condensa su commit log.
- **Tareas de fondo y workers compartidos.** El paso de cálculo del insert es el que irá a ellos.
- **Reorganización,** refresco de aristas, el modo `none`.
- **Métricas de Prometheus, la cuenta de uso, el endpoint de reset.**

**Verificado, en la máquina de los experimentos:** los tests del paquete, con los de equivalencia entre disco y memoria tal como estaban; los del índice del shard con `-race`; el de punta a punta por un shard (`adapters/repos/db/postingedge_integration_test.go`); `go vet` con y sin `postingedgebench`; `golangci-lint` sobre el repo; el linter de goroutines.

**Los tests usan unos miles de vectores sintéticos de 64 dimensiones,** donde el recall sale 1.0: comprueban el cableado, no la calidad. La calidad la mide la verificación a 1M de abajo. De los arneses con el tag solo se ha corrido ese sobre el código movido; los demás compilan.

### Verificación a 1M, corrida A: la base forzada a 99 000

*20 de noviembre, de 17:01 a 19:09. En conteos, sin cronometrar. Resultado en `testdata/bench/integration-m1/dbpedia-1m-base99000.json`, con su log al lado.*

- **Qué es:** `TestIntegrationScale` (`bench_integration_test.go`), el índice que abre el shard sobre un store propio, sin el shard alrededor. dbpedia-openai, 990 000 vectores de 1536 dimensiones, 5 000 queries, k = 10.
- **Cómo entra:** por `AddBatch`, de 100 en 100 y en el orden de v5.0, con todos los parámetros de la clase por defecto y un `Flush` tras cada lote.
- **Frente a qué:** los dos índices que v5.0 creció por el mismo camino, el validado contra el gemelo y el cronometrado. Entre ellos solo cambian los niveles que sortea el grafo de centroides.
- **Una diferencia de procedimiento:** el índice integrado construye su base con los vectores en orden de doc id, y v5.0 en orden de llegada. La base es otra, con los mismos 420 postings.

| | v5.0, validado | v5.0, cronometrado | integrado, corrida A |
|---|---|---|---|
| postings repartidos | 7 746 | 7 788 | 7 766 |
| splits | 3 663 | 3 684 | 3 673 |
| cobertura | completa | completa | completa |
| ef 21: recall | 0.9504 | 0.9509 | **0.9523** |
| ef 21: cargas por query | 19.63 | 19.54 | 19.63 |
| ef 21: rondas | 4.66 | 4.64 | 4.66 |
| ef 158: recall | 0.9900 | 0.9901 | 0.9902 |
| ef 158: cargas por query | 72.09 | 71.91 | 71.64 |
| ef 158: rondas | 17.77 | 17.73 | 17.66 |
| por insert: cargas | 63.1 | | 62.7 |
| por insert: aristas inversas | 4.60 | | 4.58 |

- **Coincide dentro del ruido en todo menos en el recall a ef 21,** que queda 0.0018 y 0.0014 por encima de los dos de v5.0. Entre sí, esos dos difieren en 0.0004, y el margen que se daba por ruido es 0.001.
- **En lo demás:** a ef 158 el recall queda a 0.0002 o menos, y las cargas a menos de un 0.7 % en los dos puntos.
- **Por qué sube a ef 21 no está comprobado.** La búsqueda es la misma función con los mismos parámetros: ef del grafo de centroides 8, rescore de 64, ef de inserción 160. Queda la base distinta.

**El recall a ef 21 frente a v5.0: cerrado** (*decisión del usuario, 21 de noviembre a las 12:15: «Cinco corridas del mismo algoritmo ya se separan 0.0014 entre sí, así que no había nada que explicar»*).

- **Lo que había:** 0.9523 en el índice integrado frente a 0.9504 y 0.9509 en los dos de v5.0, con las mismas cargas, 19.63. A ef 158 coincide todo.
- **El ruido entre corridas, según la sesión de experimentos:** cinco índices crecidos del 10 al 100 % con el mismo insert, desde una base en lote de 99 000, dan a ef 21 0.9502 (la validación de v5.3), 0.9504 y 0.9509 (v5.0), 0.9513 y 0.9516 (los dos N = 1 de v5.2).
  - Son 0.0014 entre corridas del mismo algoritmo: el margen de 0.001 que se daba por ruido era corto.
  - El 0.9523 queda 0.0007 por encima de la mayor.
- **No se corrió nada por esto.** La hipótesis del orden de la base, doc id frente a orden de llegada, queda sin comprobar y sin hacer falta.
- **La corrida B da 0.9509 a ef 21,** dentro de esas cinco.

**Tras borrar el 30 % al azar,** 297 000 de 990 000, por `Delete` como los borra un shard:

| ef | antes: recall | cargas | después: recall | cargas | diferencia de recall | de cargas |
|---|---|---|---|---|---|---|
| 20 | 0.9504 | 19.08 | 0.9554 | 23.55 | +0.0051 | +23.4 % |
| 21 | 0.9523 | 19.63 | 0.9572 | 24.27 | +0.0049 | +23.6 % |
| 40 | 0.9713 | 28.97 | 0.9752 | 36.38 | +0.0039 | +25.6 % |
| 80 | 0.9841 | 45.33 | 0.9866 | 57.46 | +0.0025 | +26.8 % |
| 158 | 0.9902 | 71.64 | 0.9922 | 91.54 | +0.0021 | +27.8 % |
| 160 | 0.9903 | 72.29 | 0.9923 | 92.36 | +0.0020 | +27.8 % |

- **Ningún vector borrado devuelto** en las 30 000 búsquedas, y cobertura completa.
- **El recall a igual ef no baja:** sube entre 0.0020 y 0.0051 sobre el del índice antes de borrar, cada uno contra su verdad. La de los que quedan sale de la verdad del dataset, sin ninguna query por fuerza bruta.
- **El coste está en las cargas:** entre un 23 % y un 28 % más a igual ef. En la celda 1b de v6.0, contra el lote de los que quedan, eran entre un 22 % y un 32 % más a igual ef (12.15 frente a 9.92 en ef 20, 45.10 frente a 34.10 en ef 160).
- **No es la misma comparación que la de v6.0.** Allí la referencia es un lote construido solo con los que quedan, y el índice es el de lote en memoria, que rutea con el código completo: en ef 20 carga 9.9 postings donde este carga 19.1. Aquí la referencia es el mismo índice antes de borrar. Un lote de los 693 000 que quedan, por el camino integrado, no se ha construido.
- **Frente a la curva de la celda 1b, con esa salvedad:** en ef 20, 40, 80 y 160 este índice da 0.9554, 0.9752, 0.9866 y 0.9923, y la celda 1b dio 0.9580, 0.9731, 0.9830 y 0.9891.
- **Las cargas a 0.95 tras borrar no están medidas:** el índice ya da 0.9554 en ef 20, el menor de la malla.

**Lo demás que dejó la corrida:**

- **La base de 99 000 se construyó en 1 min 19 s,** dentro del insert que la completó.
- **Inserts:** 120 por segundo desde la base, sin cronometrar y con otras corridas en la máquina. No se compara con los 179 de v5.0, medidos con la máquina sola.
- **Disco, con los 990 000 dentro, según `du`:**
  - `vectors.f32`: 6.1 GB. Su tamaño aparente son 7.6 GB, por los doc ids reservados por delante; lo de más son huecos sin escribir.
  - Los buckets del índice: 0.73 GB, como los dejan los ciclos. Postings 297 MB, distancias de aristas 188, códigos de ruteo 130, mapa 98 y marcas de borrado 17. v5.0 dejaba 818 MB al 100 %.
  - Los vectores de centroide: 48 MB.
  - El binario de la corrida era anterior a `a5a487d98f`: su store llevaba además los 573 MB de los vectores apartados.

### Verificación a 1M, corrida B: la base por defecto de 10 000, y reapertura

*21 de noviembre, de 10:43 a 14:53. En conteos, sin cronometrar, con cuatro o cinco corridas de experimentos en la máquina. Código de `8f54557d02`: el del hito 1 más las etapas de reapertura del arnés. Resultado en `testdata/bench/integration-m1/dbpedia-1m-base10000.json`, con su log al lado.*

- **Qué es:** la corrida A con la base que tiene una clase creada sin tocar nada. El índice guarda aparte los 10 000 primeros vectores, construye con ellos 43 postings y crece 99 veces por inserts, sin refresco de aristas.
- **Lo que añade el arnés:** tras medir el índice crecido, y otra vez con el 30 % borrado, cierra el índice y su store, los abre de disco y mide de nuevo. Cada búsqueda se pide dos veces en cada etapa, para no tomar por efecto de la reapertura una respuesta que cambie sola.

**La deriva: base de 10 000 frente a base de 99 000,** a igual ef:

| ef | base 10 000: recall | cargas | base 99 000: recall | cargas | diferencia de recall | de cargas |
|---|---|---|---|---|---|---|
| 20 | 0.9489 | 19.19 | 0.9504 | 19.08 | −0.0015 | +0.6 % |
| 21 | 0.9509 | 19.74 | 0.9523 | 19.63 | −0.0014 | +0.5 % |
| 40 | 0.9713 | 29.32 | 0.9713 | 28.97 | 0.0000 | +1.2 % |
| 80 | 0.9840 | 45.81 | 0.9841 | 45.33 | −0.0001 | +1.0 % |
| 158 | 0.9904 | 72.18 | 0.9902 | 71.64 | +0.0002 | +0.7 % |
| 160 | 0.9904 | 72.80 | 0.9903 | 72.29 | +0.0001 | +0.7 % |

- **Crecer cien veces sin refresco deriva poco:** entre un 0.5 % y un 1.2 % más de cargas a igual ef, y un recall a 0.0015 o menos, que es lo que se separan dos corridas del mismo algoritmo.
- **Frente a los dos índices de v5.0** (0.9504 y 0.9509 a ef 21 con 19.63 y 19.54 cargas; 0.9900 y 0.9901 a ef 158 con 72.09 y 71.91): el recall coincide, y las cargas quedan un 0.6 % y un 1.0 % por encima a ef 21, y un 0.1 % y un 0.4 % a ef 158.
- **Postings repartidos y splits:** 8 167 y 4 062, frente a 7 766 y 3 673 con la base de 99 000. Los postings vivos son casi los mismos, 4 105 frente a 4 093: la base pequeña pasa por más splits para llegar al mismo sitio.
- **Por insert:** 61.6 cargas y 4.60 aristas inversas, frente a 62.7 y 4.58.
- **Cobertura completa.**
- **No hay otra diferencia con v5.0 que anotar.**
- **Esta deriva es la referencia de la pieza 3 del hito 2,** el refresco de aristas al doblar: lo que el refresco puede recuperar aquí es, como mucho, ese 1 % de cargas.

**Reabierto a 1M:**

- **Devuelve lo mismo:** ninguna de las 5 000 queries cambia su respuesta, en ninguno de los seis ef, ni con el índice crecido ni con el 30 % borrado. Tampoco cambia ninguna al pedirla dos veces sobre el mismo índice abierto.
- **El meta y el número de borrados son los de antes de cerrar,** y con el 30 % borrado los 990 000 doc ids están como se dejaron: ninguno de más ni de menos.
- **Abrir el store y el índice:** 0.28 y 0.29 s, con la máquina ocupada. No es una medida de tiempo.

**Tras borrar el 30 % al azar,** sobre el índice ya reabierto:

| ef | antes: recall | cargas | después: recall | cargas | diferencia de recall | de cargas |
|---|---|---|---|---|---|---|
| 20 | 0.9489 | 19.19 | 0.9550 | 23.74 | +0.0061 | +23.7 % |
| 21 | 0.9509 | 19.74 | 0.9566 | 24.49 | +0.0057 | +24.1 % |
| 40 | 0.9713 | 29.32 | 0.9749 | 36.74 | +0.0036 | +25.3 % |
| 80 | 0.9840 | 45.81 | 0.9869 | 58.02 | +0.0029 | +26.6 % |
| 158 | 0.9904 | 72.18 | 0.9923 | 92.17 | +0.0019 | +27.7 % |
| 160 | 0.9904 | 72.80 | 0.9924 | 93.02 | +0.0020 | +27.8 % |

- **Como en la corrida A:** ningún borrado devuelto en 30 000 búsquedas, el recall sube y las cargas suben entre un 24 % y un 28 %.

**Lo demás que dejó la corrida:**

- **La base de 10 000 se construyó en 3 s,** dentro del insert que la completó.
- **Inserts:** 67 por segundo desde la base, con la máquina a más de 50 de carga. No es una medida.
- **Disco:** 6.3 GB según `du`, 8.3 GB aparentes. Con `a5a487d98f` ya no queda el bucket de los vectores apartados.

**Sin verificar a escala, del hito 1:**

- **Nada a 1M a través del shard,** ni latencia, ni inserts por segundo con la máquina sola.

## 7. Hito 2 de la integración (desde el 2026-11-21)

*Encargo del usuario del 21 a las 11:10: siete piezas, por orden, cada una con sus tests y una verificación a 1M sin cronometrar contra el número del prototipo. Las tres primeras van primero y enteras. Donde esta sección choca con la 6, manda esta. Lo marcado como mío lo decidí al escribirlo y es provisional.*

| pieza | qué | código y tests | verificación a 1M |
|---|---|---|---|
| 1 | Formato de v5.3 y reorganización de fondo | hechos | cumple: 2.00 lecturas por carga con el bucket en un segmento |
| 2 | Limpieza de deletes: sustitución local y fusión | hechos | cumple: −0.0011 de recall, +3.1 % de cargas a 0.95 y −3.2 % a 0.99 contra el lote |
| 3 | Refresco de aristas al doblar | hechos | corriendo desde el 21 a las 21:21 |
| 4 | Cálculo de inserts en los workers compartidos | hechos | sin hacer |
| 5 | Durabilidad a mitad de un insert, y base en segundo plano | hechos | sin hacer |
| 6 | Limpieza del grafo de centroides | hechos, por los ciclos del nodo | sin hacer |
| 7 | Modo `none`, con el halo en la escritura | sin empezar | |

### Lo que el índice hace por su cuenta, y cómo (*mío*)

Las tres piezas añaden trabajo que nadie pide con una llamada. Corre de dos maneras:

- **En línea con las escrituras.** Mientras hay una pasada de refresco o de limpieza en marcha, cada insert paga un paso de ella después de sus propias escrituras, y cada delete un paso de la limpieza. La pasada acaba así en un número acotado de escrituras, lleguen como lleguen.
- **De fondo, en una goroutine del índice,** que hace una cosa cada vez: acaba las pasadas cuando las escrituras paran, reordena los vectores, y cada 2 s mira si toca pedir el bucket de postings en un segmento. No es una cola del scheduler del nodo: la pieza 4 llevó a los workers del nodo el cálculo de los inserts, y este trabajo sigue aquí.
- **Por qué en línea.** La primera versión del refresco iba solo de fondo, tomando el cerrojo de escritura entre dos lotes de inserts. Con inserts continuos refrescaba un par de vectores por lote de cien: la pasada no acababa nunca, y la reordenación de vectores esperaba detrás de ella. Lo vi en la prueba pequeña del arnés, no en un test.
- **`Settle`** hace todo lo pendiente y vuelve cuando no queda nada. Lo usan los tests que comparan respuestas antes y después de un reinicio.
- **Nadie hace `Flush` detrás de este trabajo,** como lo hace el shard detrás de una petición. Las pasadas que escriben en el store vuelcan el índice cada 256 pasos y al acabar.

### Pieza 1: formato de v5.3 y reorganización

**Qué hay.** Un índice nuevo escribe su base con las aristas de cada posting en una fila, en el bucket de los postings y junto a sus miembros, reescrita cada 64 cambios (K = 64, como v5.3). Va detrás de `growEdges`: `growRows`, que deja de ser código de test (`grow_rows.go`).

- **Lo que espera en memoria está también en un bucket** (`postingedge_pending_<id>`, *mío*). En los experimentos los cambios que esperan a su fila viven solo en memoria y se escriben al cerrar. Un shard hace `Flush` tras cada petición y puede morir sin cerrar: sin ese bucket, lo que deja un `Flush` no es el índice, y los miembros nuevos se quedan sin aristas. Cada cambio se escribe ahí al hacerse y sale cuando su fila está escrita. Ninguna búsqueda lo lee.
  - **La fila se entrega al sistema antes de que sus cambios salgan del bucket.** Cada bucket tiene su log, que se escribe cuando se llena. Un proceso muerto con los borrados de uno en disco y la fila todavía en la memoria del otro dejaba miembros sin aristas. Lo cazó un test con una pasada de fondo entre el `Flush` y la copia de los ficheros; tiene su test propio (`TestGrowRowIsHandedBeforeItsChangesLeave`), que falla sin el arreglo.
  - **Al cerrar no se escribe ninguna fila:** lo que espera sigue en su bucket y vuelve al abrir.
- **Los vectores exactos van por slot** (*mío*): `vectors-<gen>.f32` con los vectores seguidos y `slots-<gen>.u32` con el slot de cada doc id. Un vector que entra toma el slot siguiente.
  - **El fichero ya no tiene huecos** por los doc ids que nunca se dieron: ocupa lo que ocupan los vectores que hay, más los de los borrados hasta la reordenación siguiente.
  - **Reordenar** es escribir los dos ficheros de la generación siguiente, con los vectores de cada posting seguidos, y nombrar esa generación en el meta. Hasta ese momento el índice son los dos ficheros viejos, enteros. Los de cualquier otra generación se borran al abrir.
  - **Corre junto a búsquedas e inserts:** copia de 256 en 256 vectores con el cerrojo de una búsqueda, entrega el fichero al disco sin ningún cerrojo, y solo su final excluye a los demás, para copiar lo que entró mientras tanto y cambiar los ficheros.
  - **Cuándo:** cuando los vectores que han entrado desde la última vez son tantos como los que se ordenaron entonces, es decir, cada vez que el índice dobla (*mío*). En el peor momento la mitad de los vectores está en orden de llegada. Reescribe el fichero entero cada vez: al crecer, el doble de lo que ocupa al final.
  - **Pide el doble de disco de los vectores mientras dura.** Si falla, el índice sigue con los ficheros que tenía y no lo reintenta hasta crecer un 10 % más.
- **«El bucket en un segmento» no sale solo de la compactación del store, y lo pide el índice** (*mío*, y toca `lsmkv`). Mi primera decisión fue dejarlo a la compactación forzada de HFresh, con la que se abre el bucket. La verificación a 1M la desmintió: tres segmentos y 3.67 lecturas por carga, también tras compactar hasta que el store no encontró nada más (abajo).
  - **Por qué:** el store compacta dos segmentos del mismo nivel, y dos sueltos de niveles distintos solo si tienen tamaños parecidos (`isSimilarSegmentSizes`). El segmento grande se queda junto a los pequeños que llegan después, con las filas viejas y sus lápidas dentro.
  - **Lo que añado a `lsmkv`:** `Bucket.CompactLeftOverSegmentsOfAnySize`, un interruptor por bucket que, mientras está puesto, deja compactar un par suelto sea cual sea su tamaño. Los ciclos del store dejan entonces el bucket en un segmento. Apagado, todo sigue como estaba; ningún otro bucket lo usa. Con su test en `lsmkv`.
  - **Cuándo lo pide el índice:** cuando lo escrito desde la última vez llega al 10 % de sus vectores (*mío*), y lo retira cuando el bucket es un segmento. Cada vez reescribe el bucket entero, unos 290 MB a 1M.
  - **La otra salida, sin tocar `lsmkv`,** era copiar todas las filas a un bucket nuevo volcado una vez y cambiarlo por el viejo, como hace la copia de los experimentos. Pide la memtable entera en RAM, o llegar a un segmento por la misma compactación que no llega, y poner al día lo escrito mientras se copia. No la escribí.
- **Un índice del hito 1 se abre como lo que es:** un valor por lista y los vectores en la posición de su doc id. Ni se reordena ni se limpia. Se refresca, porque el refresco va por la interfaz.

**Tests.** Idéntico al gemelo en memoria en cada insert, con slots y con el bucket de lo que espera, con una reordenación de una vez y otra repartida sobre doscientos inserts con postings partiéndose debajo, y cerrado y abierto con cambios esperando (`TestGrowSlotsAndPendingEqualMemory`). Una copia de los ficheros tras un `sync` abre como el índice que era, y sin el bucket no (`TestGrowOpensWhatASyncLeft`). Una pasada que no acaba, parada o muerta, deja el índice como estaba. Una tabla de slots que haría leer otro vector, o ninguno, no abre y dice por qué. Por el índice del shard, con `-race`: la reordenación sola al doblar, junto a búsquedas, inserts y deletes, y un índice de la primera versión.

**Verificación a 1M** (*21 de noviembre, de 15:34 a 17:21; código de `d30e5a7c64`; sin cronometrar, con otras corridas en la máquina; resultado en `testdata/bench/integration-m2/piece1-dbpedia-1m.json`, con su log al lado*). La corrida B con el formato nuevo: base de 10 000, 980 000 inserts de uno en uno, y las mismas etapas, más lo que lee el store por carga.

| | corrida B, formato de v5.0 | pieza 1, formato de v5.3 |
|---|---|---|
| postings repartidos y splits | 8 167 y 4 062 | 8 117 y 4 037 |
| cobertura | completa | completa |
| ef 21: recall y cargas | 0.9509 y 19.74 | 0.9515 y 19.70 |
| ef 158: recall y cargas | 0.9904 y 72.18 | 0.9905 y 72.11 |
| por insert: cargas y aristas inversas | 61.6 y 4.60 | 61.7 y 4.59 |
| con el 30 % borrado, ef 21 | 0.9566 y 24.49 | 0.9565 y 24.31 |
| con el 30 % borrado, ef 158 | 0.9923 y 92.17 | 0.9921 y 92.00 |

- **Es el mismo índice:** recall, cargas y trabajo por insert quedan dentro de lo que se separan dos corridas iguales. El formato cambia dónde están las cosas, no qué son.
- **Reabierto, devuelve lo mismo,** crecido y con el 30 % borrado: ninguna de las 5 000 queries cambia, en ningún ef.
- **Abrir tarda 2.6 y 2.8 s,** con la máquina ocupada, frente a 0.3 s en la corrida B: lee los 115 000 cambios que esperan y la tabla de slots. No es una medida de tiempo.
- **Seis reordenaciones de fondo al crecer,** una por doblado, de 20 000 a 640 000 vectores: 1.26 millones de vectores reescritos para 990 000. Al acabar, 640 000 están en orden de posting y 350 000 en orden de llegada.

**Lo que lee el store por carga,** que es el número de v5.3: 2.00 lecturas y 67 KB con el bucket en un segmento.

| estado | segmentos | lecturas por carga | KB por carga |
|---|---|---|---|
| v5.3, compactado (referencia) | 1 | 2.00 | 67.3 |
| v5.3, como lo dejan los ciclos (referencia) | 4 | 5.67 | 143.3 |
| pieza 1, como lo dejan los ciclos | 3 | 3.67 | 77.4 |
| pieza 1, compactado hasta que el store no encuentra más | 3 | 3.67 | 77.4 |
| pieza 1, con el bucket pedido en un segmento | 1 | 2.00 | 65.1 a 65.5 |

- **Sin el interruptor no cumplía, y no por el formato:** el store no deja el bucket en un segmento. Compactar hasta el final no cambió nada, porque los tres segmentos son de niveles distintos y tamaños dispares. Es lo que cuenta el punto de arriba, y de ahí sale el interruptor de `lsmkv`.
- **Con el interruptor cumple** (*21 de noviembre, de 18:40 a 18:45; código de `16742dc04e`; resultado en `piece1-one-segment-dbpedia-1m.json`*). El índice que dejó la corrida, con el 30 % marcado como borrado, se abrió tal cual. El arnés pidió el bucket en un segmento como lo pide el índice, y dos rondas de la compactación del store lo dejaron en uno: de 286 a 250 MB.
  - **2.00 lecturas por carga en los seis ef,** y 65 KB, frente a 67.3 de v5.3. Antes de pedirlo, 3.67 lecturas y 77 KB, las de la corrida.
  - **Ninguna de las 5 000 queries cambia de respuesta,** en ningún ef, entre antes y después.
  - **Lo que esta medida no cubre:** que el índice lo pida por su cuenta mientras crece, con la regla del 10 %. Eso lo ve la corrida de la pieza 3, que crece con todo encendido.
- **Lee menos que la referencia sin compactar** (77 KB frente a 143). La validación de v5.3 creció con la compactación por niveles y esta con la forzada de HFresh; que esa sea la causa no está comprobado.

**Lo que escribe y ocupa:**

- **Filas de aristas:** 89 179 escritas, 0.95 GB, y 81 105 lápidas, 0.86 GB. v5.3 midió 84 336 y 76 994.
- **El bucket de lo que espera:** 5.48 millones de cambios escritos, 0.45 GB entregados al store. En disco, 69 MB al acabar.
- **En memoria esperan, en su máximo,** 115 094 listas, 9.1 MB.
- **Disco, según `du`:** 6.4 GB. Los vectores, 6.08 GB justos para 990 000, sin huecos. Los buckets: postings 286 MB, distancias de aristas 136, códigos de ruteo 124, mapa 83, lo que espera 69, marcas de borrado 17.
- **Inserts:** 162 por segundo desde la base, sin cronometrar y con otras corridas en la máquina. La corrida B dio 67 con la máquina mucho más cargada: no se comparan.

**Sin verificar todavía de la pieza 1:**

- **Cuánto escribe de más el store por dejar el bucket en un segmento** cada vez que lo escrito llega al 10 %: cada vez reescribe el bucket entero. La corrida de la pieza 3 lo cuenta.
- **La latencia en frío con los vectores en orden de posting.** Es lo que la reordenación compra, y esta verificación no cronometra.
- **Los inserts por segundo con la máquina sola,** y lo que cuesta el bucket de lo que espera.
- **Una reordenación a 1M bajo inserts:** las seis corrieron, y la mayor copió 640 000 vectores; no está contado cuánto tardó cada una ni cuánto frenó a los inserts.

### Pieza 2: limpieza de deletes

**Qué hay.** La pasada de la celda B de v6.2, en disco (`grow_clean.go`). Saca del índice los vectores que estaban marcados cuando empezó, en tres pasos, posting a posting:

1. **Sustitución.** Cada arista de un vector que se queda y que lleva a uno que se va recibe un sustituto: de la lista del que se va, el más cercano al vector por la distancia entre sus dos códigos, que esté vivo, fuera de su posting y que no tenga ya. Sin sustituto, la arista se quita. No hay búsquedas.
2. **Eliminación.** Cada posting se reescribe sin los que se van. Tras el paso 1 ninguna lista lleva a ellos, así que entre dos postings lo que hay en disco es un índice.
3. **Fusión.** Un posting que queda por debajo de 177 miembros se une al de centroide más cercano si los dos caben en el umbral de split (177 de 360, escalado con el umbral).

- **Lo que el disco añade a lo medido en memoria** (*mío*): el índice guarda la distancia exacta de cada arista, que usan las aristas inversas de los inserts. La de un sustituto se calcula leyendo los dos vectores: una distancia exacta por arista sustituida.
- **La distancia entre dos códigos** es la del cuantizador sobre los bits de signo que el código guarda. El prototipo la tenía solo con el código completo en RAM; el índice del shard tiene 512 bits, y la usa sobre esos (`codec.between`). El ángulo se estima igual con los primeros bits, con más ruido. Lo que eso cambia en la elección del sustituto lo dice la verificación a 1M, no un test.
- **Un miembro que se va sale de la fila de su posting con una lápida.** Hasta que el store la compacta, la fila guarda más valores que miembros tiene el posting, y se lee resolviendo lápidas. Sin limpieza las dos cuentas coinciden y la lectura es la de siempre. Lo que cuesta esa lectura no está medido.
- **Una fusión no toca aristas,** como en el prototipo: desde la primera, un vector puede tener una arista a otro de su propio posting, que una búsqueda salta.
- **Un doc id que se fue queda libre** y se puede dar otra vez.
- **Cuándo:** cuando los marcados llegan al 3 % de los vectores, las rondas del churn de v6.2 (*mío*). Los que se borran durante una pasada esperan a la siguiente, que solo arranca si vuelven a ser el 3 %.
- **Un índice del hito 1, con un valor por lista, no se limpia:** sigue con la marca sola.

**Tests.** La pasada en disco, sin fusiones, es la del gemelo en memoria: se van los mismos vectores, cada arista recibe el mismo sustituto, y los dos índices son el mismo antes, después y a lo largo de los inserts que siguen, que leen las distancias guardadas para los sustitutos (`TestGrowCleanEqualsMemory`, seis casos, con rondas, reaperturas y compactación). Las fusiones van por invariantes y no contra el gemelo: los dos comparten el grafo de centroides, y las fusiones del gemelo cambian el vecino más cercano que vería el disco. Una pasada cortada entre dos pasos deja un índice, y la siguiente lo acaba. Por el índice del shard, con `-race`: la limpieza sola, junto a búsquedas, inserts y deletes, y un doc id dado otra vez.

**Verificación a 1M: cumple** (*21 de noviembre, de 18:54 a 21:19; código de `cfc9eb8e75`; sin cronometrar, con el build de 10M en la máquina; resultado en `testdata/bench/integration-m2/piece2-churn-dbpedia-1m.json`, con su log al lado*). El churn de v6.2 sobre el índice integrado, en disco: una base de 760 000 vectores construida en lote, y diez rondas que borran al azar el 3 % de ella (22 800), insertan otros tantos nuevos y pasan la limpieza. Al final se mide en la malla de ef de v6.2 hasta 320, reabierto, y junto a un índice cuya base es el lote de los 760 000 vectores que quedan, construido y leído por el mismo código.

| | criterio | prototipo, celda B | integrado |
|---|---|---|---|
| peor recall a igual ef, contra el lote | −0.005 o mejor | −0.0004 en ef 20 | −0.0011 en ef 10 |
| cargas a 0.95, contra el lote | +15 % o menos | +2.7 % | +3.1 % (17.33 frente a 16.82) |
| cargas a 0.99, contra el lote | +10 % o menos | +1.8 % | −3.2 % (63.20 frente a 65.27) |
| aristas con sustituto | | 94.4 % | 94.5 % (1 815 917 de 1 921 562) |
| fusiones | | 677 | 674 |
| aristas por vector, de la ronda 1 a la 10 | | 8.84 | de 8.85 a 8.83 |
| postings vivos al final | | 3 191 | 3 186 |
| distancias entre códigos por borrado | | 62.8 | 59.4 |

| ef | recall, con churn | recall, lote | cargas, con churn | cargas, lote |
|---|---|---|---|---|
| 10 | 0.9217 | 0.9228 | 12.09 | 11.89 |
| 20 | 0.9517 | 0.9527 | 17.64 | 17.30 |
| 40 | 0.9719 | 0.9711 | 26.74 | 26.25 |
| 80 | 0.9838 | 0.9831 | 41.65 | 41.18 |
| 160 | 0.9909 | 0.9901 | 66.32 | 65.61 |
| 320 | 0.9946 | 0.9939 | 107.81 | 106.35 |

- **Las tres condiciones entran con margen,** y las diferencias con el lote son del tamaño del suelo de ruido de v6.1 y v6.2 (0.0012 de recall, 1.1 % de cargas a 0.95 y 3.0 % a 0.99). El −3.2 % a 0.99 no es una mejora: a igual ef el índice con churn carga entre un 1.1 y un 1.9 % más, y cruza 0.99 un poco antes.
- **La pasada hace lo que hacía en memoria:** las mismas fusiones, la misma fracción de aristas sustituidas y las mismas aristas por vector, ronda a ronda.
- **Reabierto, devuelve lo mismo:** ninguna de las 5 000 queries cambia, en ningún ef.
- **Lo que el disco añade,** por vector borrado: 8.0 distancias exactas, una por arista sustituida, y 14.8 lecturas de vectores para calcularlas.
- **Una diferencia con el prototipo que no es de esta pieza, y que no he perseguido:** las cargas absolutas no son las suyas. El lote de 760 000 da aquí 16.82 cargas a 0.95 y 65.27 a 0.99; el de v6.2, en memoria, 9.41 y 44.52. El índice con churn y su lote se mueven juntos, así que el criterio, que es relativo, no cambia. La corrida no dice de dónde sale: los prototipos de v6.x median en memoria, y el índice integrado es el de disco de v5.0, que a 1M da 19.6 cargas a 0.95.
- **El fichero de vectores no encoge con la limpieza:** al final tiene 988 000 slots para 760 000 vectores, 6.4 GB frente a los 4.8 GB del lote. Los slots de los borrados se recuperan en la reordenación siguiente, que no tocó: el índice no dobló.

**Sin verificar de la pieza 2:**

- **La limpieza en línea con las escrituras, a 1M.** En esta corrida las pasadas las llama el arnés, una por ronda, como en el protocolo de v6.2. Que el índice las arranque solo al 3 % y las pague en línea está en los tests, y a 1M en la etapa de borrado de la corrida de la pieza 4.
- **El churn largo y el borrado por regiones de v6.2b.**
- **Lo que cuesta leer una fila con lápidas de miembros** hasta que el store la compacta.
- **Nada cronometrado:** ni lo que tarda una pasada con la máquina sola, ni lo que frena a los inserts.

### Pieza 3: refresco de aristas al doblar

**Qué hay.** El refresco de v2.4, en disco (`grow_refresh.go`). El índice guarda, por doc id, cuántos vectores tenía cuando se construyeron sus aristas por última vez (`built.u32`): la base con la base, un insert con los que había con él, un refresco con los de entonces. Cuando el índice tiene el doble de los que tenía al empezar el refresco anterior, una pasada refresca los vectores con aristas de la mitad o menos.

- **Refrescar un vector es repetir el paso de aristas de su insert:** la búsqueda, que entra por su propio posting y solo lee, y después su lista y su sitio en las listas de sus vecinos. El vector no se mueve.
- **Va en línea:** un vector refrescado por insert mientras la pasada dura. Una pasada toma como mucho la mitad del índice, así que acaba cuando el índice ha crecido la mitad.
- **Lo que cuesta:** al crecer de 10 000 a 1M son unos 630 000 refrescos para 980 000 inserts. Cada uno es la búsqueda de un insert.
- **Un vector marcado como borrado no se refresca,** y uno que se fue no tiene fecha.

**Tests.** El refresco en disco es el del gemelo en memoria: en cada doblado están viejos los mismos vectores, cada refresco encuentra los mismos vecinos, escribe la misma lista y entra en las mismas listas, y los dos índices siguen siendo el mismo (`TestGrowRefreshEqualsMemory`, con las dos formas de guardar aristas, dos doblados y una reapertura entre ellos). Por el índice del shard, con `-race`: cuántos vectores refresca cada insert y dónde acaba cada pasada, una pasada que la cierra el fondo tras una reapertura, y junto a búsquedas, inserts y deletes.

### Pieza 4: el cálculo de los inserts en los workers del nodo

**Qué hay.** Varios llamadores de `AddBatch` a la vez, que es como llegan los inserts a un índice del shard: los workers compartidos del nodo traen cada uno una parte de lo que hay en la cola, y las goroutines de un import también. Con la base construida, la búsqueda de cada insert corre en la goroutine que llama, junto a las de los demás y junto a las queries, y solo las escrituras van de una en una. Es el reparto de v5.2.

- **Ventanas de cuatro** (*mío*): un llamador busca para cuatro vectores suyos y después los escribe. Con ocho llamadores, la búsqueda de un insert no ve unos treinta y dos que aún no han escrito, que es el lote de 32 que midió v5.2. Con otro número de workers el lote efectivo es otro.
- **Lo que se escribió entre la búsqueda y la escritura se vuelve a mirar.** Cada escritura lleva un número de orden, y el plan de un insert guarda el de su búsqueda:
  - **Un posting que se partió** desde entonces: el insert vuelve a elegir posting, como ya hacía el lote de v5.2.
  - **Una lista de aristas que otra escritura cambió** desde entonces: se lee otra vez en lugar de usar la que guardó la búsqueda.
  - **Un vector que salió del índice** desde entonces (una pasada de limpieza): la búsqueda se repite entera, con el cerrojo de escritura.
- **Un doc id se da una vez:** de dos llamadores que lo traen a la vez, el segundo recibe el rechazo de siempre.
- **Lo que el índice hace por su cuenta sigue en su goroutine** y en línea con las escrituras. No está en una cola del scheduler del nodo.

**Tests.** Ocho llamadores con deletes y búsquedas a la vez, con `-race`: el índice comprueba lista a lista que nada de lo que una escritura toma de su búsqueda está viejo (10 921 listas comprobadas, 3 565 releídas y 33 postings elegidos otra vez en una pasada del test), y al final el índice está entero, con cada vector el que se le dio, y devuelve lo mismo reabierto (`TestPersistentAddsFromSeveralCallers`). El mismo doc id desde cuatro llamadores. Y por debajo, que un plan cuya lista cambió después de su búsqueda escribe sobre la lista nueva (`TestGrowPlansWriteAfterOtherWrites`, que falla si se quita la comprobación).

**Por el shard, con indexación asíncrona** (`TestPostingEdge_ThroughAShardWithAsyncIndexing`, con `-race`): con `ASYNC_INDEXING` los vectores y los deletes llegan al índice desde los workers compartidos del nodo, varios a la vez, cada uno con una parte de la cola del shard. Una clase así pasa los mismos pasos que una sin ella: importar, buscar, borrar, actualizar, reiniciar y seguir. Son 3 200 objetos de 64 dimensiones, no una prueba de escala.

- **Lo que destapó:** `Settle` volvía sin esperar a una base que se estaba construyendo, un fallo de la pieza 5 de unas horas antes. Arreglado, con su test (`TestPersistentSettleWaitsForItsBase`).

### Pieza 6: el grafo de centroides

**Qué hay.** El grafo de centroides es un HNSW, y lo mantienen los ciclos del nodo que mantienen cualquier HNSW. El shard le pasa los dos, con el límite de log del grafo de centroides de HFresh:

- **El ciclo de tombstones** saca del grafo, del todo, el centroide de cada posting que se partió o se fusionó. Hasta ahora se quedaban como nodos borrados para siempre: uno por split. El arnés de los experimentos los limpiaba a mano antes de medir.
- **El ciclo de los commit logs** condensa y combina los ficheros del log del grafo, que antes crecía sin límite.
- **Cada cuánto:** el ciclo de tombstones de un HNSW corre como mucho una vez cada 300 s por grafo, su valor por defecto. No lo he cambiado.

**Un bug, encontrado al cablearlo, arreglado.** El grafo leía los centroides del slice del índice desde goroutines que no tienen ningún cerrojo suyo, mientras un split sustituye ese slice con el índice bloqueado.

- **Quién leía así:** la goroutine que llena la caché del grafo al abrirlo, que existe desde el hito 1, y desde esta pieza el ciclo de tombstones del nodo.
- **Qué podía pasar:** una lectura del slice a medio sustituir. En la práctica el grafo tiene los centroides en su caché y casi nunca los pide; el detector de carreras no lo veía por las pasadas enteras del índice.
- **El arreglo:** el grafo lee de una copia que se publica cada vez que los centroides cambian y que nadie escribe (`centroidIndex.publish`). Con su test (`TestCentroidGraphReadsItsCentroidsWhileTheyGrow`), que con el código anterior hace saltar el detector de carreras.

**Tests.** Con los ciclos, un pase del de tombstones deja el grafo sin ningún centroide borrado, el índice encuentra lo mismo y abre igual; sin ellos se quedan todos, uno por split (`TestPersistentKeepsItsCentroidGraph`). Los dos ciclos corriendo sin parar junto a inserts, deletes con sus fusiones y búsquedas, con el índice reabierto a la mitad y el log partido en ficheros pequeños, con `-race` (`TestPersistentCentroidGraphIsKeptWhileTheIndexWrites`).

**Sin verificar de la pieza 6:**

- **A 1M no ha corrido nada con los ciclos puestos.** El arnés abre el índice sin ellos y limpia el grafo a mano antes de medir, como los experimentos. Cuántos centroides borrados acumula un índice entre dos pases, y qué le cuestan a una búsqueda, no está medido.
- **Por el shard:** el test de extremo a extremo abre el índice con los ciclos del shard, pero no comprueba que el grafo quede limpio.

### Pieza 5: una caída a mitad de una escritura

**El problema.** Una escritura del índice son muchas: un insert pone un miembro en una fila, una entrada en el mapa, un código de ruteo, las distancias de sus aristas, cambios en las listas de sus vecinos, su vector en un fichero y su slot en otro; y si su posting se parte, dos filas más, una entrada de mapa por miembro y dos centroides. Cada bucket tiene su log, que llega al sistema cuando se llena su buffer. Un proceso que muere en medio deja unas escrituras y otras no, y lo que queda no es un índice: hasta ahora no abría.

**Qué hay: un diario de operaciones delante de los buckets** (`grow_journal.go`, *mío*).

- **Un paso** es una escritura entera del índice: un insert con su split, las marcas de borrado de una llamada, un paso de la pasada de limpieza, el refresco de un vector.
- **Mientras el paso corre, sus escrituras esperan en memoria,** donde el propio paso las lee. Al acabar se añaden al diario como un registro, con el meta que el índice tiene después, en una sola escritura al sistema. Solo entonces van a los buckets y a los ficheros.
- **Al abrir, el índice aplica otra vez los registros del diario,** lo que no cambia nada donde ya estaban aplicados, y descarta un registro que no esté entero: ese paso no ocurrió. Tras una caída el índice es el que era después del último paso con su registro escrito.
- **El diario se vacía** cuando lo que cuenta ha llegado al sistema por su camino: en cada `Flush` del shard, antes de cambiar de generación de vectores, y por su cuenta si pasa de 64 MB.
- **El grafo de centroides no va en el diario.** Tiene su propio commit log, que una caída puede dejar por delante o por detrás del índice, o con un centroide sin enlaces. Un fichero marca que el grafo ha cambiado desde el último vaciado, y un índice que abre con la marca reconstruye el grafo desde los centroides. El grafo que sale no es el de antes enlace a enlace.
- **Lo que cuesta:** cada escritura va dos veces, al diario y a su bucket. En el test son unos 6.8 KB de diario por paso, con vectores de 64 dimensiones; a 1536 dimensiones el vector solo son 6 KB. Sin medir a 1M.

**Lo que no cubre:**

- **La caída de la máquina.** El diario protege de la muerte del proceso, que es lo que pierde los buffers de los logs. Contra un corte de corriente da lo que dan los logs del store, que tampoco se sincronizan.
- **El meta sigue escribiéndose con un rename, sin `fsync`.**
- **Un insert que falla al entregar sus escrituras** (disco lleno, por ejemplo) ya tiene su registro en el diario: el índice se para, y al reabrir ese insert está hecho, aunque quien lo pidió recibió un error.
- **La construcción de la base** no pasa por el diario: no tiene meta hasta que está entera, y una base a medias se descarta al abrir, como antes.

**Tests.** El formato del diario: los registros vuelven enteros y en orden, y de un registro cortado en cualquier byte, o con un byte cambiado, no vuelve nada y los anteriores sí. Por el índice: una copia del directorio en cada punto donde un proceso puede morir al acabar un paso (antes de su registro, después, y tras entregar cada escritura), 487 copias por caso; cada una abre como el índice de antes del paso o como el de después, comparados por todo lo que tienen (`TestGrowOpensWhatACrashLeft`, con inserts, splits, marcas, y la pasada de limpieza con sus fusiones). Sin escribir el registro, el test falla en el primer insert: la copia no abre. Por el índice del shard: 179 caídas a lo largo de lotes, marcas, la limpieza y el refresco en línea; cada copia abre con todo lo de las llamadas anteriores, con los primeros vectores del lote que estaba entrando y ninguno suelto, responde y sigue aceptando inserts (`TestPersistentOpensWhatACrashLeft`). Todos los tests del formato del shard corren ahora con el diario puesto, los de equivalencia con el gemelo en memoria también.

**La base, en segundo plano** (`persistent_base.go`). El usuario lo fijó para este hito: la construcción de la base no puede correr dentro de un insert. Con 10 000 vectores de 1536 dimensiones eran unos 3 s de una petición, y con 99 000, 1 min 19 s, con las búsquedas esperando.

- **Quién la construye:** el trabajo de fondo del índice, sin ninguno de sus cerrojos. El insert que trae el vector que la completa vuelve como cualquier otro.
- **Mientras se construye,** los vectores que llegan se apartan como los anteriores, y las búsquedas siguen leyendo todos los apartados, exactas.
- **Un vector borrado mientras se construye** puede estar dentro de la base. Deja de devolverse en el momento, y en el bucket queda una marca en su lugar, que pasa a ser una marca de borrado del índice cuando la base lo es. Su doc id no se puede dar otra vez hasta entonces: un insert con él se rechaza (*mío*; un shard no reutiliza doc ids).
- **Cuando la base es el índice,** los vectores que llegaron mientras tanto siguen apartados. El índice los toma de uno en uno, como cualquier insert: de fondo, y dos con cada insert nuevo (*mío*), pagando su parte del refresco y de la limpieza. Hasta que no queda ninguno, una búsqueda los lee todos, exactos, y los mezcla con lo que encuentra en el índice.
- **Solo el cambio de «apartados» a «índice» deja a todos fuera,** el tiempo de abrir la base y soltar los vectores con los que se construyó.
- **Si la construcción falla,** los vectores siguen apartados y se intenta otra vez cuando hayan llegado más.
- **Tras una caída:** sin meta no hay base, lo que haya de una se descarta al abrir y los vectores siguen apartados, como antes. Con meta, el bucket de apartados puede tener vectores que la base ya tiene, que se sueltan al abrir; marcas, que se llevan al índice; y los que llegaron después, que el índice sigue tomando.
- **Sin trabajo de fondo** (los ganchos de test) la base se construye dentro del insert, como antes.

**Tests de la base.** Con la construcción parada en cada uno de sus puntos: inserts, deletes y búsquedas mientras se construye y con la base ya escrita; el doc id borrado que no se puede volver a dar; las búsquedas junto a los vectores que aún esperan; y una copia de los ficheros en tres momentos (construyendo, con la base escrita, y con cinco vectores tomados), que abierta acaba con los mismos vectores (`TestPersistentBuildsItsBaseInTheBackground`). Falla si la búsqueda no mezcla los apartados, y si las marcas no se llevan al índice. Una construcción que falla y se repite, y un apagado a mitad de la construcción.

**Sin verificar de la pieza 5:**

- **A 1M:** que el índice crecido con el diario es el mismo, cuánto escribe el diario por insert, y una muerte de verdad del proceso (`kill -9`) a mitad de los inserts, reabierta.
- **La base en segundo plano, a 1M y por el shard:** el arnés espera a la base antes de seguir, para que sus corridas se comparen con las anteriores; el test de extremo a extremo pasa, y no mira qué ocurre mientras se construye.
- **Lo que tarda una búsqueda con miles de vectores apartados:** los lee todos del bucket en cada query, como antes de la base. Con una base grande y muchos inserts durante su construcción puede ser lento un rato.
