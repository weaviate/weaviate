package checks

import "github.com/weaviate/weaviate/entities/models"

// HasLegacyVectorIndex checks whether there is a configured legacy vector index on a class.
func HasLegacyVectorIndex(class *models.Class) bool {
	return class.Vectorizer != "" || class.VectorIndexConfig != nil || class.VectorIndexType != ""
}
