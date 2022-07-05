package traverser

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func (t Traverser) validateCrossClassDistanceCompatibility() error {
	s := t.schemaGetter.GetSchemaSkipAuth()
	if s.Objects == nil {
		return nil
	}

	var (
		// a set used to determine the discrete number
		// of vector index distance types used across
		// all classes. if more than one type exists,
		// a cross-class vector search is not possible
		distancerTypes = make(map[string]struct{})

		// a mapping of class name to vector index distance
		// type. used to emit an error if more than one
		// distance type is found
		classDistanceConfigs = make(map[string]string)
	)

	for _, class := range s.Objects.Classes {
		if class == nil {
			continue
		}

		hnswConfig, err := typeAssertVectorIndex(class)
		if err != nil {
			return err
		}

		distancerTypes[hnswConfig.Distance] = struct{}{}
		classDistanceConfigs[class.Class] = hnswConfig.Distance
	}

	if len(distancerTypes) != 1 {
		return buildCrossClassDistCompatError(classDistanceConfigs)
	}

	return nil
}

func (t *Traverser) validateGetDistanceParams(params GetParams) error {
	sch := t.schemaGetter.GetSchemaSkipAuth()
	class := sch.GetClass(schema.ClassName(params.ClassName))
	if class == nil {
		return fmt.Errorf("failed to find class '%s' in schema", params.ClassName)
	}

	hnswConfig, err := typeAssertVectorIndex(class)
	if err != nil {
		return err
	}

	if hnswConfig.Distance != hnsw.DistanceCosine {
		return errors.Errorf(
			"can't use certainty when vector index is configured with %s distance",
			hnswConfig.Distance)
	}

	return nil
}

func typeAssertVectorIndex(class *models.Class) (hnsw.UserConfig, error) {
	hnswConfig, ok := class.VectorIndexConfig.(hnsw.UserConfig)
	if !ok {
		return hnsw.UserConfig{}, fmt.Errorf("class '%s' vector index: config is not hnsw.UserConfig: %T",
			class.Class, class.VectorIndexConfig)
	}

	return hnswConfig, nil
}

func buildCrossClassDistCompatError(classDistanceConfigs map[string]string) error {
	errorMsg := "vector search across classes not possible: found different distance metrics:"
	for class, dist := range classDistanceConfigs {
		errorMsg = fmt.Sprintf("%s class '%s' uses distance metric '%s',", errorMsg, class, dist)
	}
	errorMsg = strings.TrimSuffix(errorMsg, ",")

	return fmt.Errorf(errorMsg)
}
