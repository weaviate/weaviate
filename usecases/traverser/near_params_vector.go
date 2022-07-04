//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
)

type nearParamsVector struct {
	modulesProvider ModulesProvider
	search          nearParamsSearcher
}

type nearParamsSearcher interface {
	ObjectByID(ctx context.Context, id strfmt.UUID,
		props search.SelectProperties, additional additional.Properties) (*search.Result, error)
}

func newNearParamsVector(modulesProvider ModulesProvider, search nearParamsSearcher) *nearParamsVector {
	return &nearParamsVector{modulesProvider, search}
}

func (v *nearParamsVector) vectorFromParams(ctx context.Context,
	nearVector *searchparams.NearVector, nearObject *searchparams.NearObject,
	moduleParams map[string]interface{}, className string) ([]float32, error) {
	err := v.validateNearParams(nearVector, nearObject, moduleParams, className)
	if err != nil {
		return nil, err
	}

	if len(moduleParams) == 1 {
		for name, value := range moduleParams {
			return v.vectorFromModules(ctx, className, name, value)
		}
	}

	if nearVector != nil {
		return nearVector.Vector, nil
	}

	if nearObject != nil {
		vector, err := v.vectorFromNearObjectParams(ctx, nearObject)
		if err != nil {
			return nil, errors.Errorf("nearObject params: %v", err)
		}

		return vector, nil
	}

	// either nearObject or nearVector or module search param has to be set,
	// so if we land here, something has gone very wrong
	panic("vectorFromParams was called without any known params present")
}

func (v *nearParamsVector) validateNearParams(nearVector *searchparams.NearVector,
	nearObject *searchparams.NearObject,
	moduleParams map[string]interface{}, className ...string) error {
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
			params := []string{}
			for p := range moduleParams {
				params = append(params, fmt.Sprintf("'%s'", p))
			}
			return errors.Errorf("found more then one module param: %s which are conflicting "+
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

func (v *nearParamsVector) vectorFromModules(ctx context.Context,
	className, paramName string, paramValue interface{}) ([]float32, error) {
	if v.modulesProvider != nil {
		vector, err := v.modulesProvider.VectorFromSearchParam(ctx,
			className, paramName, paramValue, v.findVector,
		)
		if err != nil {
			return nil, errors.Errorf("vectorize params: %v", err)
		}
		return vector, nil
	}
	return nil, errors.New("no modules defined")
}

func (v *nearParamsVector) findVector(ctx context.Context, id strfmt.UUID) ([]float32, error) {
	res, err := v.search.ObjectByID(ctx, id, search.SelectProperties{}, additional.Properties{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, errors.New("vector not found")
	}

	return res.Vector, nil
}

func (v *nearParamsVector) vectorFromNearObjectParams(ctx context.Context,
	params *searchparams.NearObject) ([]float32, error) {
	if len(params.ID) == 0 && len(params.Beacon) == 0 {
		return nil, errors.New("empty id and beacon")
	}

	var id strfmt.UUID
	if len(params.ID) > 0 {
		id = strfmt.UUID(params.ID)
	} else {
		ref, err := crossref.Parse(params.Beacon)
		if err != nil {
			return nil, err
		}
		id = ref.TargetID
	}

	return v.findVector(ctx, id)
}

func (v *nearParamsVector) extractCertaintyFromParams(nearVector *searchparams.NearVector,
	nearObject *searchparams.NearObject, moduleParams map[string]interface{}) float64 {
	if nearVector != nil {
		if nearVector.Certainty != 0 {
			return nearVector.Certainty
		} else if nearVector.Distance != 0 {
			return 1 - nearVector.Distance/2
		}
	}

	if nearObject != nil {
		if nearObject.Certainty != 0 {
			return nearObject.Certainty
		} else if nearObject.Distance != 0 {
			return 1 - nearObject.Distance/2
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
					return 1 - nearParam.GetDistance()/2
				}
			}
		}
	}

	return 0
}
