//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
)

// The outbound ports the object paths drive: the store objects live in, the module
// hooks that extend and vectorize them, and the clock that stamps them.

type VectorRepo interface {
	PutObject(ctx context.Context, concept *models.Object, vector []float32,
		vectors map[string][]float32, multiVectors map[string][][]float32,
		repl *additional.ReplicationProperties, schemaVersion uint64) error
	DeleteObject(ctx context.Context, className string, id strfmt.UUID, deletionTime time.Time,
		repl *additional.ReplicationProperties, tenant string, schemaVersion uint64) error
	// Object returns object of the specified class giving by its id
	Object(ctx context.Context, class string, id strfmt.UUID, props search.SelectProperties,
		additional additional.Properties, repl *additional.ReplicationProperties,
		tenant string) (*search.Result, error)
	// Exists returns true if an object of a giving class exists
	Exists(ctx context.Context, class string, id strfmt.UUID,
		repl *additional.ReplicationProperties, tenant string) (bool, error)
	ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties,
		additional additional.Properties, tenant string) (*search.Result, error)
	ObjectSearch(ctx context.Context, offset, limit int, filters *filters.LocalFilter,
		sort []filters.Sort, additional additional.Properties, tenant string) (search.Results, error)
	AddReference(ctx context.Context, source *crossref.RefSource,
		target *crossref.Ref, repl *additional.ReplicationProperties, tenant string, schemaVersion uint64) error
	Merge(ctx context.Context, merge MergeDocument, repl *additional.ReplicationProperties, tenant string, schemaVersion uint64) error
	Query(context.Context, *QueryInput) (search.Results, *Error)
}

type ModulesProvider interface {
	GetObjectAdditionalExtend(ctx context.Context, in *search.Result,
		moduleParams map[string]interface{}) (*search.Result, error)
	ListObjectsAdditionalExtend(ctx context.Context, in search.Results,
		moduleParams map[string]interface{}) (search.Results, error)
	UsingRef2Vec(className string) bool
	UpdateVector(ctx context.Context, object *models.Object, class *models.Class, repo modulecapabilities.FindObjectFn,
		logger logrus.FieldLogger) error
	BatchUpdateVector(ctx context.Context, class *models.Class, objects []*models.Object,
		findObjectFn modulecapabilities.FindObjectFn,
		logger logrus.FieldLogger) (map[int]error, error)
	VectorizerName(className string) (string, error)
}

// timeSource stamps objects with their creation and update times. Manager and
// BatchManager both take defaultTimeSource; tests substitute a fixed clock.
type timeSource interface {
	Now() int64
}

type defaultTimeSource struct{}

func (ts defaultTimeSource) Now() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
