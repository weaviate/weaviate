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

package nearText

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

type Searcher struct {
	vectorizer vectorizer
	movements  *movements
}

func NewSearcher(vectorizer vectorizer) *Searcher {
	return &Searcher{vectorizer, newMovements()}
}

type vectorizer interface {
	Texts(ctx context.Context, input []string, cfg moduletools.ClassConfig) ([]float32, error)
}

func (s *Searcher) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}
	vectorSearches["nearText"] = s.vectorForNearTextParam
	return vectorSearches
}

func (s *Searcher) vectorForNearTextParam(ctx context.Context, params interface{}, className string,
	findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	return s.vectorFromNearTextParam(ctx, params.(*NearTextParams), className, findVectorFn, cfg)
}

func (s *Searcher) vectorFromNearTextParam(ctx context.Context,
	params *NearTextParams, className string, findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	tenant := cfg.Tenant()
	vector, err := s.vectorizer.Texts(ctx, params.Values, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "vectorize keywords")
	}

	moveTo := params.MoveTo
	if moveTo.Force > 0 && (len(moveTo.Values) > 0 || len(moveTo.Objects) > 0) {
		moveToVector, err := s.vectorFromValuesAndObjects(ctx, moveTo.Values,
			moveTo.Objects, className, findVectorFn, cfg, tenant)
		if err != nil {
			return nil, errors.Wrap(err, "vectorize move to")
		}

		afterMoveTo, err := s.movements.MoveTo(vector, moveToVector, moveTo.Force)
		if err != nil {
			return nil, err
		}
		vector = afterMoveTo
	}

	moveAway := params.MoveAwayFrom
	if moveAway.Force > 0 && (len(moveAway.Values) > 0 || len(moveAway.Objects) > 0) {
		moveAwayVector, err := s.vectorFromValuesAndObjects(ctx, moveAway.Values,
			moveAway.Objects, className, findVectorFn, cfg, tenant)
		if err != nil {
			return nil, errors.Wrap(err, "vectorize move away from")
		}

		afterMoveFrom, err := s.movements.MoveAwayFrom(vector, moveAwayVector, moveAway.Force)
		if err != nil {
			return nil, err
		}
		vector = afterMoveFrom
	}

	return vector, nil
}

func (s *Searcher) vectorFromValuesAndObjects(ctx context.Context,
	values []string, objects []ObjectMove,
	className string,
	findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig, tenant string,
) ([]float32, error) {
	var objectVectors [][]float32

	if len(values) > 0 {
		moveToVector, err := s.vectorizer.Texts(ctx, values, cfg)
		if err != nil {
			return nil, errors.Wrap(err, "vectorize move to")
		}
		objectVectors = append(objectVectors, moveToVector)
	}

	if len(objects) > 0 {
		var id strfmt.UUID
		for _, obj := range objects {
			if len(obj.ID) > 0 {
				id = strfmt.UUID(obj.ID)
			}
			if len(obj.Beacon) > 0 {
				ref, err := crossref.Parse(obj.Beacon)
				if err != nil {
					return nil, err
				}
				id = ref.TargetID
			}

			vector, _, err := findVectorFn(ctx, className, id, tenant, cfg.TargetVector())
			if err != nil {
				return nil, err
			}

			objectVectors = append(objectVectors, vector)
		}
	}

	return libvectorizer.CombineVectors(objectVectors), nil
}
