package schema

type InvertedIndexConfig struct {
	CleanupIntervalSeconds int64
	BM25                   BM25Config
}

type BM25Config struct {
	K1 float64
	B  float64
}
