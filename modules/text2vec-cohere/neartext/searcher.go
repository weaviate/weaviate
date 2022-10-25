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

package neartext

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	localvectorizer "github.com/semi-technologies/weaviate/modules/text2vec-cohere/vectorizer"
)

type Searcher struct {
	vectorizer vectorizer
}

func NewSearcher(vectorizer vectorizer) *Searcher {
	return &Searcher{vectorizer}
}

type vectorizer interface {
	Texts(ctx context.Context, input []string,
		settings localvectorizer.ClassSettings) ([]float32, error)
	MoveTo(source, target []float32, weight float32) ([]float32, error)
	MoveAwayFrom(source, target []float32, weight float32) ([]float32, error)
	CombineVectors(vectors [][]float32) []float32
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
	// it is safe to call NewClassSettings even knowing that cfg can be nil, it
	// is to built to work with all defaults in the case of a nil-config, see
	// vectorizer/class_settings_test.go for details.
	settings := localvectorizer.NewClassSettings(cfg)
	vector, err := s.vectorizer.Texts(ctx, params.Values, settings)
	if err != nil {
		return nil, errors.Errorf("vectorize keywords: %v", err)
	}

	moveTo := params.MoveTo
	if moveTo.Force > 0 && (len(moveTo.Values) > 0 || len(moveTo.Objects) > 0) {
		moveToVector, err := s.vectorFromValuesAndObjects(ctx, moveTo.Values,
			moveTo.Objects, className, findVectorFn, settings)
		if err != nil {
			return nil, errors.Errorf("vectorize move to: %v", err)
		}

		afterMoveTo, err := s.vectorizer.MoveTo(vector, moveToVector, moveTo.Force)
		if err != nil {
			return nil, err
		}
		vector = afterMoveTo
	}

	moveAway := params.MoveAwayFrom
	if moveAway.Force > 0 && (len(moveAway.Values) > 0 || len(moveAway.Objects) > 0) {
		moveAwayVector, err := s.vectorFromValuesAndObjects(ctx, moveAway.Values,
			moveAway.Objects, className, findVectorFn, settings)
		if err != nil {
			return nil, errors.Errorf("vectorize move away from: %v", err)
		}

		afterMoveFrom, err := s.vectorizer.MoveAwayFrom(vector, moveAwayVector, moveAway.Force)
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
	settings localvectorizer.ClassSettings,
) ([]float32, error) {
	var objectVectors [][]float32
	class := className
	if len(values) > 0 {
		moveToVector, err := s.vectorizer.Texts(ctx, values, settings)
		if err != nil {
			return nil, errors.Errorf("vectorize move to: %v", err)
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
				if ref.Class != "" {
					class = ref.Class
				}
			}

			vector, err := findVectorFn(ctx, class, id)
			if err != nil {
				return nil, err
			}

			objectVectors = append(objectVectors, vector)
		}
	}

	return s.vectorizer.CombineVectors(objectVectors), nil
}
