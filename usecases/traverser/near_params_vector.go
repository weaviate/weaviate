//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package traverser

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/modulecomponents/generictypes"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

type nearParamsVector struct {
	modulesProvider ModulesProvider
	search          nearParamsSearcher
}

type nearParamsSearcher interface {
	Object(ctx context.Context, className string, id strfmt.UUID,
		props search.SelectProperties, additional additional.Properties,
		repl *additional.ReplicationProperties, tenant string) (*search.Result, error)
	ObjectsByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties,
		additional additional.Properties, tenant string) (search.Results, error)
}

func newNearParamsVector(modulesProvider ModulesProvider, search nearParamsSearcher) *nearParamsVector {
	return &nearParamsVector{modulesProvider, search}
}

func (v *nearParamsVector) targetFromParams(ctx context.Context,
	nearVector *searchparams.NearVector, nearObject *searchparams.NearObject,
	moduleParams map[string]interface{}, className, tenant string,
) ([]string, error) {
	err := v.validateNearParams(nearVector, nearObject, moduleParams, className)
	if err != nil {
		return nil, err
	}

	if len(moduleParams) == 1 {
		for _, value := range moduleParams {
			return v.targetFromModules(className, value)
		}
	}

	if nearVector != nil {
		var targetVector []string
		if len(nearVector.TargetVectors) > 0 {
			targetVector = nearVector.TargetVectors
		}
		return targetVector, nil
	}

	if nearObject != nil {
		var targetVector []string
		if len(nearObject.TargetVectors) > 0 {
			targetVector = nearObject.TargetVectors
		}
		return targetVector, nil
	}

	// either nearObject or nearVector or module search param has to be set,
	// so if we land here, something has gone very wrong
	return nil, errors.Errorf("targetFromParams was called without any known params present")
}

func (v *nearParamsVector) vectorFromParams(ctx context.Context,
	nearVector *searchparams.NearVector, nearObject *searchparams.NearObject,
	moduleParams map[string]interface{}, className, tenant, targetVector string, index int,
) (models.Vector, error) {
	err := v.validateNearParams(nearVector, nearObject, moduleParams, className)
	if err != nil {
		return nil, err
	}

	if len(moduleParams) == 1 {
		for name, value := range moduleParams {
			return v.vectorFromModules(ctx, className, name, value, tenant, targetVector)
		}
	}

	if nearVector != nil {
		if index >= len(nearVector.Vectors) {
			return nil, fmt.Errorf("nearVector.vectorFromParams was called with invalid index")
		}
		return nearVector.Vectors[index], nil
	}

	if nearObject != nil {
		vector, _, err := v.vectorFromNearObjectParams(ctx, className, nearObject, tenant, targetVector)
		if err != nil {
			return nil, errors.Errorf("nearObject params: %v", err)
		}

		return vector, nil
	}

	// either nearObject or nearVector or module search param has to be set,
	// so if we land here, something has gone very wrong
	return []float32{}, errors.Errorf("vectorFromParams was called without any known params present")
}

func (v *nearParamsVector) validateNearParams(nearVector *searchparams.NearVector,
	nearObject *searchparams.NearObject,
	moduleParams map[string]interface{}, className ...string,
) error {
	if len(moduleParams) == 1 && nearVector != nil && nearObject != nil {
		return errors.Errorf("found 'nearText' and 'nearVector' and 'nearObject' parameters " +
			"which are conflicting, choose one instead")
	}

	if len(moduleParams) == 1 && nearVector != nil {
		return errors.Errorf("found both 'nearText' and 'nearVector' parameters " +
			"which are conflicting, choose one instead")
	}

	if len(moduleParams) == 1 && nearObject != nil {
		return errors.Errorf("found both 'nearText' and 'nearObject' parameters " +
			"which are conflicting, choose one instead")
	}

	if nearVector != nil && nearObject != nil {
		return errors.Errorf("found both 'nearVector' and 'nearObject' parameters " +
			"which are conflicting, choose one instead")
	}

	if v.modulesProvider != nil {
		if len(moduleParams) > 1 {
			params := make([]string, 0, len(moduleParams))
			for p := range moduleParams {
				params = append(params, fmt.Sprintf("'%s'", p))
			}
			return errors.Errorf("found more than one module params: %s which are conflicting "+
				"choose one instead", strings.Join(params, ", "))
		}

		for name, value := range moduleParams {
			if len(className) == 1 {
				err := v.modulesProvider.ValidateSearchParam(name, value, className[0])
				if err != nil {
					return err
				}
			} else {
				err := v.modulesProvider.CrossClassValidateSearchParam(name, value)
				if err != nil {
					return err
				}
			}
		}
	}

	if nearVector != nil {
		if nearVector.Certainty != 0 && nearVector.Distance != 0 {
			return errors.Errorf("found 'certainty' and 'distance' set in nearVector " +
				"which are conflicting, choose one instead")
		}
	}

	if nearObject != nil {
		if nearObject.Certainty != 0 && nearObject.Distance != 0 {
			return errors.Errorf("found 'certainty' and 'distance' set in nearObject " +
				"which are conflicting, choose one instead")
		}
	}

	return nil
}

func (v *nearParamsVector) targetFromModules(className string, paramValue interface{}) ([]string, error) {
	if v.modulesProvider != nil {
		targetVector, err := v.modulesProvider.TargetsFromSearchParam(className, paramValue)
		if err != nil {
			return nil, errors.Errorf("vectorize params: %v", err)
		}
		return targetVector, nil
	}
	return nil, errors.New("no modules defined")
}

func (v *nearParamsVector) vectorFromModules(ctx context.Context,
	className, paramName string, paramValue interface{}, tenant string, targetVector string,
) (models.Vector, error) {
	if v.modulesProvider != nil {
		isMultiVector, err := v.modulesProvider.IsTargetVectorMultiVector(className, targetVector)
		if err != nil {
			return nil, errors.Errorf("is target vector: %s multi vector: %v", targetVector, err)
		}

		if isMultiVector {
			vector, err := v.modulesProvider.MultiVectorFromSearchParam(ctx,
				className, targetVector, tenant, paramName, paramValue, generictypes.FindMultiVectorFn(v.findMultiVector),
			)
			if err != nil {
				return nil, errors.Errorf("vectorize params: %v", err)
			}
			return vector, nil
		} else {
			vector, err := v.modulesProvider.VectorFromSearchParam(ctx,
				className, targetVector, tenant, paramName, paramValue, generictypes.FindVectorFn(v.findVector),
			)
			if err != nil {
				return nil, errors.Errorf("vectorize params: %v", err)
			}
			return vector, nil
		}
	}
	return nil, errors.New("no modules defined")
}

// TODO:colbert unify findVector and findMultiVector
func (v *nearParamsVector) findVectorForNearObject(ctx context.Context,
	className string, id strfmt.UUID, tenant, targetVector string,
) (models.Vector, string, error) {
	if multiVector, targetVector, err := v.findMultiVector(ctx, className, id, tenant, targetVector); err == nil && len(multiVector) > 0 {
		return multiVector, targetVector, nil
	}
	return v.findVector(ctx, className, id, tenant, targetVector)
}

func (v *nearParamsVector) findVector(ctx context.Context, className string, id strfmt.UUID, tenant, targetVector string) ([]float32, string, error) {
	switch className {
	case "":
		// Explore cross class searches where we don't have class context
		return v.crossClassFindVector(ctx, id, targetVector)
	default:
		return v.classFindVector(ctx, className, id, tenant, targetVector)
	}
}

func (v *nearParamsVector) findMultiVector(ctx context.Context, className string, id strfmt.UUID, tenant, targetVector string) ([][]float32, string, error) {
	switch className {
	case "":
		// Explore cross class searches where we don't have class context
		return v.crossClassFindMultiVector(ctx, id, targetVector)
	default:
		return v.classFindMultiVector(ctx, className, id, tenant, targetVector)
	}
}

func (v *nearParamsVector) classFindVector(ctx context.Context, className string,
	id strfmt.UUID, tenant, targetVector string,
) ([]float32, string, error) {
	res, err := v.search.Object(ctx, className, id, search.SelectProperties{}, additional.Properties{}, nil, tenant)
	if err != nil {
		return nil, "", err
	}
	if res == nil {
		return nil, "", errors.New("vector not found")
	}
	if targetVector != "" {
		if targetVector == modelsext.DefaultNamedVectorName && len(res.Vector) > 0 {
			return res.Vector, "", nil
		}

		if res.Vectors[targetVector] == nil {
			return nil, "", fmt.Errorf("vector not found for target: %v", targetVector)
		}
		vec, ok := res.Vectors[targetVector].([]float32)
		if !ok {
			return nil, "", fmt.Errorf("unrecognized type: %T for target: %v", res.Vectors[targetVector], targetVector)
		}
		return vec, targetVector, nil
	} else {
		if len(res.Vector) > 0 {
			return res.Vector, "", nil
		}

		if len(res.Vectors) == 1 {
			for key, vec := range res.Vectors {
				switch v := vec.(type) {
				case []float32:
					return v, key, nil
				default:
					return nil, "", fmt.Errorf("unrecognized type: %T target: %v", vec, key)
				}
			}
		} else if len(res.Vectors) > 1 {
			return nil, "", errors.New("multiple vectors found, specify target vector")
		}
	}

	return nil, "", fmt.Errorf("nearObject search-object with id %v has no vector", id)
}

// TODO:colbert try to unify
func (v *nearParamsVector) classFindMultiVector(ctx context.Context, className string,
	id strfmt.UUID, tenant, targetVector string,
) ([][]float32, string, error) {
	res, err := v.search.Object(ctx, className, id, search.SelectProperties{}, additional.Properties{}, nil, tenant)
	if err != nil {
		return nil, "", err
	}
	if res == nil {
		return nil, "", errors.New("vector not found")
	}
	if targetVector != "" {
		if len(res.Vectors) == 0 || res.Vectors[targetVector] == nil {
			return nil, "", fmt.Errorf("vector not found for target: %v", targetVector)
		}
		multiVector, ok := res.Vectors[targetVector].([][]float32)
		if !ok {
			return nil, "", fmt.Errorf("unrecognized type: %T target: %v", res.Vectors[targetVector], targetVector)
		}
		return multiVector, targetVector, nil
	} else {
		if len(res.Vectors) == 1 {
			for key, vec := range res.Vectors {
				switch v := vec.(type) {
				case [][]float32:
					return v, key, nil
				default:
					return nil, "", fmt.Errorf("unrecognized type: %T target: %v", vec, key)
				}
			}
		} else if len(res.Vectors) > 1 {
			return nil, "", errors.New("multiple vectors found, specify target vector")
		}
	}
	return nil, "", fmt.Errorf("nearObject search-object with id %v has no vector", id)
}

func (v *nearParamsVector) crossClassFindVector(ctx context.Context, id strfmt.UUID, targetVector string) ([]float32, string, error) {
	res, err := v.search.ObjectsByID(ctx, id, search.SelectProperties{}, additional.Properties{}, "")
	if err != nil {
		return nil, "", errors.Wrap(err, "find objects")
	}
	switch len(res) {
	case 0:
		return nil, "", errors.New("vector not found")
	case 1:
		if targetVector != "" {
			if len(res[0].Vectors) == 0 || res[0].Vectors[targetVector] == nil {
				return nil, "", fmt.Errorf("vector not found for target: %v", targetVector)
			}
			vec, ok := res[0].Vectors[targetVector].([]float32)
			if !ok {
				return nil, "", fmt.Errorf("unrecognized vector type: %T", vec)
			}
			return vec, targetVector, nil
		}

		if len(res[0].Vector) > 0 {
			return res[0].Vector, "", nil
		}

		if len(res[0].Vectors) == 0 {
			return nil, "", nil
		}

		if len(res[0].Vectors) == 1 {
			for key, vec := range res[0].Vectors {
				v, ok := vec.([]float32)
				if !ok {
					return nil, "", fmt.Errorf("unrecognized vector type: %T", vec)
				}
				return v, key, nil
			}
		}

		return nil, "", errors.New("multiple vectors found, specify target vector")
	default:
		if targetVector == "" {
			vectors := make([][]float32, len(res))
			for i := range res {
				vectors[i] = res[i].Vector
			}
			return libvectorizer.CombineVectors(vectors), targetVector, nil
		}
		vectors := [][]float32{}
		vectorDims := map[int]bool{}
		for i := range res {
			if len(res[i].Vectors) > 0 {
				if vec, ok := res[i].Vectors[targetVector]; ok {
					switch v := vec.(type) {
					case []float32:
						vectors = append(vectors, v)
						if _, exists := vectorDims[len(v)]; !exists {
							vectorDims[len(v)] = true
						}
					default:
						return nil, "", fmt.Errorf("unrecognized vector type: %T for target vector: %s", vec, targetVector)
					}
				}
			}
		}
		if len(vectorDims) != 1 {
			return nil, "", fmt.Errorf("vectors with incompatible dimensions found for target: %s", targetVector)
		}
		return libvectorizer.CombineVectors(vectors), targetVector, nil
	}
}

func (v *nearParamsVector) crossClassFindMultiVector(ctx context.Context, id strfmt.UUID, targetVector string) ([][]float32, string, error) {
	res, err := v.search.ObjectsByID(ctx, id, search.SelectProperties{}, additional.Properties{}, "")
	if err != nil {
		return nil, "", errors.Wrap(err, "find objects")
	}
	switch len(res) {
	case 0:
		return nil, "", errors.New("multi vector not found")
	case 1:
		if targetVector != "" {
			if len(res[0].Vectors) == 0 || res[0].Vectors[targetVector] == nil {
				return nil, "", fmt.Errorf("multi vector not found for target: %v", targetVector)
			}
		} else {
			if len(res[0].Vectors) == 1 {
				for key, vec := range res[0].Vectors {
					v, ok := vec.([][]float32)
					if !ok {
						return nil, "", fmt.Errorf("unrecognized multi vector type: %T", vec)
					}
					return v, key, nil
				}
			} else if len(res[0].Vectors) > 1 {
				return nil, "", errors.New("multiple multi vectors found, specify target vector")
			}
		}
		return nil, "", fmt.Errorf("multi vector not found for target: %v", targetVector)
	default:
		return nil, "", fmt.Errorf("multiple multi vectors with incompatible dimensions found for target: %s", targetVector)
	}
}

func (v *nearParamsVector) crossClassVectorFromNearObjectParams(ctx context.Context,
	params *searchparams.NearObject,
) (models.Vector, string, error) {
	return v.vectorFromNearObjectParams(ctx, "", params, "", "")
}

func (v *nearParamsVector) vectorFromNearObjectParams(ctx context.Context,
	className string, params *searchparams.NearObject, tenant, targetVector string,
) (models.Vector, string, error) {
	if len(params.ID) == 0 && len(params.Beacon) == 0 {
		return nil, "", errors.New("empty id and beacon")
	}

	var id strfmt.UUID
	targetClassName := className

	if len(params.ID) > 0 {
		id = strfmt.UUID(params.ID)
	} else {
		ref, err := crossref.Parse(params.Beacon)
		if err != nil {
			return nil, "", err
		}
		id = ref.TargetID
		if ref.Class != "" {
			targetClassName = ref.Class
		}
	}

	if targetVector == "" && len(params.TargetVectors) >= 1 {
		targetVector = params.TargetVectors[0]
	}

	return v.findVectorForNearObject(ctx, targetClassName, id, tenant, targetVector)
}

func (v *nearParamsVector) extractCertaintyFromParams(nearVector *searchparams.NearVector,
	nearObject *searchparams.NearObject, moduleParams map[string]interface{}, hybrid *searchparams.HybridSearch,
) float64 {
	if nearVector != nil {
		if nearVector.Certainty != 0 {
			return nearVector.Certainty
		} else if nearVector.WithDistance {
			return additional.DistToCertainty(nearVector.Distance)
		}
	}

	if nearObject != nil {
		if nearObject.Certainty != 0 {
			return nearObject.Certainty
		} else if nearObject.WithDistance {
			return additional.DistToCertainty(nearObject.Distance)
		}
	}

	if hybrid != nil {
		if hybrid.WithDistance {
			return additional.DistToCertainty(float64(hybrid.Distance))
		}
		if hybrid.NearVectorParams != nil {
			if hybrid.NearVectorParams.Certainty != 0 {
				return hybrid.NearVectorParams.Certainty
			} else if hybrid.NearVectorParams.WithDistance {
				return additional.DistToCertainty(hybrid.NearVectorParams.Distance)
			}
		}
		if hybrid.NearTextParams != nil {
			if hybrid.NearTextParams.Certainty != 0 {
				return hybrid.NearTextParams.Certainty
			} else if hybrid.NearTextParams.WithDistance {
				return additional.DistToCertainty(hybrid.NearTextParams.Distance)
			}
		}
	}

	if len(moduleParams) == 1 {
		return v.extractCertaintyFromModuleParams(moduleParams)
	}

	return 0
}

func (v *nearParamsVector) extractCertaintyFromModuleParams(moduleParams map[string]interface{}) float64 {
	for _, param := range moduleParams {
		if nearParam, ok := param.(modulecapabilities.NearParam); ok {
			if nearParam.SimilarityMetricProvided() {
				if certainty := nearParam.GetCertainty(); certainty != 0 {
					return certainty
				} else {
					return additional.DistToCertainty(nearParam.GetDistance())
				}
			}
		}
	}

	return 0
}
