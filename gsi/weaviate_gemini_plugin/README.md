
# GSI Technology's Gemini Plugin

The Gemini Plugin provides an alternative to Weaviate's native HNSW ANN implementation:
* It serves as a bridge between Weaviate and GSI Technology's Fast Vector Search (FVS)
* FVS provides efficient hardware accelerated vector search and is suitable for large scale datasets

# Architecture

All of the code for the Gemini support in Weaviate lives in two primary places:
* In the core Weaviate codebase including:
  * A [Gemini entity](../../entities/vectorindex/gemini/config.go) which sits alongside the native HNSW entity in the main Weaviate codebase.
  * A [Gemini index stub](../../adapters/repos/db/vector/gemini/) which sits alongside the native HNSW index code in the main Weaviate codebase.
* In this Golang module directory:
  * It includes an FVS REST API wrapper written in pure Golang [\(fvs.go\)](./fvs.go)
  * It includes code which implements the Weaviate index "interface" called from the Geminiindex stub described above [\(index.go\)](./index.go).
  * Please note that this module may end up in its own separate repository in the future.



