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

package interpretation

import (
	"context"

	"github.com/graphql-go/graphql/language/ast"
	"github.com/semi-technologies/weaviate/entities/search"
)

type Interpretation struct{}

func New() *Interpretation {
	return &Interpretation{}
}

func (e *Interpretation) AdditonalPropertyDefaultValue() interface{} {
	return true
}

func (e *Interpretation) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	// this is a special case additional value
	// this value is being added to storage object in vectorization process
	// interpretation is being saved in DB when making vectorization
	// interpretation is being extracted and added to the result
	// when it's being read from DB (see storage_object.go)
	return in, nil
}

func (e *Interpretation) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return true
}
