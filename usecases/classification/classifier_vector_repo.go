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

package classification

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type vectorClassSearchRepo struct {
	vectorRepo vectorRepo
}

func newVectorClassSearchRepo(vectorRepo vectorRepo) *vectorClassSearchRepo {
	return &vectorClassSearchRepo{vectorRepo}
}

func (r *vectorClassSearchRepo) VectorClassSearch(ctx context.Context,
	params modulecapabilities.VectorClassSearchParams) ([]search.Result, error) {
	return r.vectorRepo.VectorClassSearch(ctx, traverser.GetParams{
		Filters:    params.Filters,
		Pagination: params.Pagination,
		ClassName:  params.ClassName,
		Properties: r.getProperties(params.Properties),
	})
}

func (r *vectorClassSearchRepo) getProperties(properties []string) search.SelectProperties {
	if len(properties) > 0 {
		props := search.SelectProperties{}
		for i := range properties {
			props = append(props, search.SelectProperty{Name: properties[i]})
		}
		return props
	}
	return nil
}
