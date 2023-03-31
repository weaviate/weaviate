
# GSI Technology's Gemini Plugin

The Gemini Plugin provides an alternative to Weaviate's native HNSW ANN implementation:
* It serves as a bridge between Weaviate and GSI Technology's Fast Vector Search (FVS)
* FVS provides efficient hardware accelerated vector search and is suitable for large scale datasets

# Plugin Architecture

All of the code for the Gemini support in Weaviate lives in two primary places:
* In the core Weaviate codebase including:
  * A "gemini" entity which sits alongside the native HNSW entity, living under "entities/vectorIndex" in the main Weaviate codebase.
  * A "gemini" index stub which sits alongside the native HNSW index code, living under "adapters/repos/db/index" in the main Weaviate codebase.
* In this Golang module directory:
  * It includes an FVS REST API wrapper written in pure Golang [\(fvs.go\)](./fvs.go)
  * It includes code which implements the Weaviate index "interface" called from the "gemini" index stub described above [\(index.go\)](./index.go).
  * Please note that this module may end up in its own separately named repository in the future.



