package traverser

// QueryVectorHolder is a simple struct to hold the query vector.
// passing a pointer to this struct in the context allows us to modify the content.
type QueryVectorHolder struct {
	Vectors map[string][]float32
}

func NewQueryVectorHolder() *QueryVectorHolder {
	return &QueryVectorHolder{
		Vectors: make(map[string][]float32),
	}
}

func (h *QueryVectorHolder) Set(targetVector string, vector []float32) {
	if h.Vectors == nil {
		h.Vectors = make(map[string][]float32)
	}
	h.Vectors[targetVector] = vector
}

// ContextKeyQueryVector is the key used to store the QueryVectorHolder in the context.
const ContextKeyQueryVector = "weaviate_query_vector_holder"
