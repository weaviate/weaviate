//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package classification

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
)

type fakeSchemaGetter struct {
	schema schema.Schema
}

func (f *fakeSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

type fakeClassificationRepo struct {
	sync.Mutex
	db map[strfmt.UUID]models.Classification
}

func newFakeClassificationRepo() *fakeClassificationRepo {
	return &fakeClassificationRepo{
		db: map[strfmt.UUID]models.Classification{},
	}
}

func (f *fakeClassificationRepo) Put(ctx context.Context, class models.Classification) error {
	f.Lock()
	defer f.Unlock()

	f.db[class.ID] = class
	return nil
}

func (f *fakeClassificationRepo) Get(ctx context.Context, id strfmt.UUID) (*models.Classification, error) {
	f.Lock()
	defer f.Unlock()

	class, ok := f.db[id]
	if !ok {
		return nil, nil
	}

	return &class, nil
}

func newFakeVectorRepo(unclassified, classified search.Results) *fakeVectorRepo {
	return &fakeVectorRepo{
		unclassified: unclassified,
		classified:   classified,
		db:           map[strfmt.UUID]*models.Thing{},
	}
}

// read requests are specified throuh unclassified and classified,
// write requests (Put[Kind]) are stored in the db map
type fakeVectorRepo struct {
	sync.Mutex
	unclassified     []search.Result
	classified       []search.Result
	db               map[strfmt.UUID]*models.Thing
	errorOnAggregate error
}

func (f *fakeVectorRepo) GetUnclassified(ctx context.Context,
	k kind.Kind, class string,
	properties []string) ([]search.Result, error) {
	if k != kind.Thing {
		return nil, fmt.Errorf("unsupported kind in test fake: %v", k)
	}

	return f.unclassified, nil
}

func (f *fakeVectorRepo) AggregateNeighbors(ctx context.Context, vector []float32,
	ki kind.Kind, class string, properties []string, k int) ([]NeighborRef, error) {

	// simulate that this takes some time
	time.Sleep(5 * time.Millisecond)

	if ki != kind.Thing {
		return nil, fmt.Errorf("unsupported kind in test fake: %v", k)
	}

	if k != 1 {
		return nil, fmt.Errorf("fake vector repo only supports k=1")
	}

	results := f.classified
	sort.SliceStable(results, func(i, j int) bool {
		simI, err := cosineSim(results[i].Vector, vector)
		if err != nil {
			panic(err.Error())
		}

		simJ, err := cosineSim(results[j].Vector, vector)
		if err != nil {
			panic(err.Error())
		}
		return simI > simJ
	})

	var out []NeighborRef
	schema := results[0].Schema.(map[string]interface{})
	for _, propName := range properties {
		prop, ok := schema[propName]
		if !ok {
			return nil, fmt.Errorf("missing prop %s", propName)
		}

		refs := prop.(models.MultipleRef)
		if len(refs) != 1 {
			return nil, fmt.Errorf("wrong length %d", len(refs))
		}

		out = append(out, NeighborRef{
			Beacon:   refs[0].Beacon,
			Count:    1,
			Property: propName,
		})
	}

	return out, f.errorOnAggregate
}

func (f *fakeVectorRepo) PutThing(ctx context.Context, thing *models.Thing, vector []float32) error {
	f.Lock()
	defer f.Unlock()
	f.db[thing.ID] = thing
	return nil
}

func (f *fakeVectorRepo) PutAction(ctx context.Context, thing *models.Action, vector []float32) error {
	return fmt.Errorf("put action not implemented in fake")
}

func cosineSim(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("vectors have different dimensions")
	}

	var (
		sumProduct float64
		sumASquare float64
		sumBSquare float64
	)

	for i := range a {
		sumProduct += float64(a[i] * b[i])
		sumASquare += float64(a[i] * a[i])
		sumBSquare += float64(b[i] * b[i])
	}

	return float32(sumProduct / (math.Sqrt(sumASquare) * math.Sqrt(sumBSquare))), nil
}
