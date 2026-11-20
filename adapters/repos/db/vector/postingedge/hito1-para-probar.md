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
| `rq.cacheMode: "none"` | La clase no se crea: «not available yet» |
| Compresión `pq`, `sq`, `bq`, o `rq` con otros bits | La clase no se crea |

## Con qué te vas a encontrar

- **Hasta 10 000 vectores la búsqueda es exacta,** leyendo todos los vectores en cada query. Al llegar a 10 000 el índice construye sus postings, **dentro del insert que los completa:** esa petición tarda lo que tarde el build, y las búsquedas esperan mientras tanto. Con 10 000 vectores no está medido; con 99 000 de 1536 dimensiones fueron 1 min 19 s.
- **Un borrado no libera nada.** El vector se marca, deja de devolverse y sigue en su sitio. Con muchos borrados las búsquedas trabajan más para el mismo recall.
- **Un update gasta sitio:** es un borrado más un insert.
- **Los inserts van de uno en uno** y son lentos: unos 180 por segundo a 1M y 1536 dimensiones en un M1 Max, medido en los experimentos con la máquina sola. Cada insert bloquea las búsquedas un momento, mientras escribe.
- **No hay recuperación tras una caída a mitad de un insert.** Si el proceso muere entre dos peticiones, el índice abre bien. Si muere mientras escribe un insert, el índice puede negarse a abrir, y entonces el shard no carga. No hay reparación automática: hay que borrar la clase y volver a importar.
- **Si una escritura falla a medias** (disco lleno, por ejemplo), el índice se para y devuelve error en cada llamada hasta que el shard se reinicia.
- **El disco:** además de los objetos, el índice guarda su propia copia de los vectores, 6.1 GB por millón a 1536 dimensiones, y unos 0.7 GB por millón en sus buckets.
- **Sin métricas de Prometheus** propias, y el endpoint de debug que reinicia un índice no lo conoce.

## Lo medido a 1M

*dbpedia-openai, 990 000 vectores de 1536 dimensiones, 5 000 queries, k = 10. El índice integrado, crecido desde vacío por inserts, sin el shard alrededor. En conteos, sin cronometrar.*

**Con la base por defecto de 10 000, que es lo que vas a usar tú: PENDIENTE.** Es la corrida que dice cuánto se degrada el índice al crecer cien veces desde su base sin refresco. Hasta que esté, los números de abajo no son los de una clase creada sin tocar nada.

**Con la base forzada a 99 000 por un gancho de test,** que es el camino que midieron los experimentos:

| ef | recall | postings leídos por query | en los experimentos, dos corridas: recall | postings |
|---|---|---|---|---|
| 21 | 0.9523 | 19.6 | 0.9504 y 0.9509 | 19.6 y 19.5 |
| 158 | 0.9902 | 71.6 | 0.9900 y 0.9901 | 72.1 y 71.9 |

- **El índice integrado es el de los experimentos:** los mismos postings leídos, los mismos splits (3 673 frente a 3 663 y 3 684) y cobertura completa.
- **El recall a ef 21 sale 0.0014 y 0.0018 por encima,** algo más que el ruido esperado de 0.001. No hay explicación comprobada.

**Tras borrar el 30 % al azar:**

- **Ningún borrado vuelve en los resultados,** en 30 000 búsquedas.
- **El recall a igual ef no baja:** sube entre 0.002 y 0.005.
- **Cada búsqueda lee entre un 23 % y un 28 % más de postings,** porque los borrados siguen en su sitio.

**Sin medir a 1M:** reabrir el índice, nada a través del shard, la latencia, y los inserts por segundo con la máquina sola.

## Qué viene en el hito 2

Limpieza de borrados, refresco de aristas, tareas de fondo (el build de la base entre ellas), el formato definitivo en disco y el modo sin códigos en RAM.
