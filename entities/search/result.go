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

package search

import (
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
)

// Result contains some info of a concept (kind), but not all. For
// additional info the ID can be used to retrieve the full concept from the
// connector storage
type Result struct {
	ID                   strfmt.UUID
	ClassName            string
	Score                float32
	Dist                 float32
	Vector               []float32
	Beacon               string
	Certainty            float32
	Schema               models.PropertySchema
	Created              int64
	Updated              int64
	AdditionalProperties models.AdditionalProperties
	VectorWeights        map[string]string
}

type Results []Result

func (r Result) Object() *models.Object {
	return r.ObjectWithVector(true)
}

func (r Result) ObjectWithVector(includeVector bool) *models.Object {
	schema, ok := r.Schema.(map[string]interface{})
	if ok {
		delete(schema, "id")
	}

	t := &models.Object{
		Class:              r.ClassName,
		ID:                 r.ID,
		Properties:         schema,
		CreationTimeUnix:   r.Created,
		LastUpdateTimeUnix: r.Updated,
		VectorWeights:      r.VectorWeights,
	}

	if r.AdditionalProperties != nil {
		t.Additional = r.AdditionalProperties
	}

	if includeVector {
		t.Vector = r.Vector
	}

	return t
}

func (rs Results) Objects() []*models.Object {
	return rs.ObjectsWithVector(true)
}

func (rs Results) ObjectsWithVector(includeVector bool) []*models.Object {
	objects := make([]*models.Object, len(rs))
	for i, res := range rs {
		objects[i] = res.ObjectWithVector(includeVector)
	}

	return objects
}
