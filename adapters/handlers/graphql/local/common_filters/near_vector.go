package common_filters

import (
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// ExtractNearVector arguments, such as "vector" and "certainty"
func ExtractNearVector(source map[string]interface{}) traverser.NearVectorParams {
	var args traverser.NearVectorParams

	// vector is a required argument, so we don't need to check for its existing
	vector := source["vector"].([]interface{})
	args.Vector = make([]float32, len(vector))
	for i, value := range vector {
		args.Vector[i] = float32(value.(float64))
	}

	certainty, ok := source["certainty"]
	if ok {
		args.Certainty = certainty.(float64)
	}

	return args
}
