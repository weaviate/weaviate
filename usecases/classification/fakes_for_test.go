//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package classification

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	libfilters "github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
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

func newFakeVectorRepoKNN(unclassified, classified search.Results) *fakeVectorRepoKNN {
	return &fakeVectorRepoKNN{
		unclassified: unclassified,
		classified:   classified,
		db:           map[strfmt.UUID]*models.Thing{},
	}
}

// read requests are specified throuh unclassified and classified,
// write requests (Put[Kind]) are stored in the db map
type fakeVectorRepoKNN struct {
	sync.Mutex
	unclassified     []search.Result
	classified       []search.Result
	db               map[strfmt.UUID]*models.Thing
	errorOnAggregate error
}

func (f *fakeVectorRepoKNN) GetUnclassified(ctx context.Context,
	k kind.Kind, class string, properties []string,
	filter *libfilters.LocalFilter) ([]search.Result, error) {
	f.Lock()
	defer f.Unlock()
	if k != kind.Thing {
		return nil, fmt.Errorf("unsupported kind in test fake: %v", k)
	}

	return f.unclassified, nil
}

func (f *fakeVectorRepoKNN) AggregateNeighbors(ctx context.Context, vector []float32,
	ki kind.Kind, class string, properties []string, k int,
	filter *libfilters.LocalFilter) ([]NeighborRef, error) {
	f.Lock()
	defer f.Unlock()

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

func (f *fakeVectorRepoKNN) VectorClassSearch(ctx context.Context,
	params traverser.GetParams) ([]search.Result, error) {
	f.Lock()
	defer f.Unlock()
	return nil, fmt.Errorf("vector class search not implemented in fake")
}

func (f *fakeVectorRepoKNN) PutThing(ctx context.Context, thing *models.Thing, vector []float32) error {
	f.Lock()
	defer f.Unlock()
	f.db[thing.ID] = thing
	return nil
}

func (f *fakeVectorRepoKNN) PutAction(ctx context.Context, thing *models.Action, vector []float32) error {
	return fmt.Errorf("put action not implemented in fake")
}

func (f *fakeVectorRepoKNN) get(id strfmt.UUID) (*models.Thing, bool) {
	f.Lock()
	defer f.Unlock()
	t, ok := f.db[id]
	return t, ok
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return nil
}

func newFakeVectorRepoContextual(unclassified, targets search.Results) *fakeVectorRepoContextual {
	return &fakeVectorRepoContextual{
		unclassified: unclassified,
		targets:      targets,
		db:           map[strfmt.UUID]*models.Thing{},
	}
}

// read requests are specified throuh unclassified and classified,
// write requests (Put[Kind]) are stored in the db map
type fakeVectorRepoContextual struct {
	sync.Mutex
	unclassified     []search.Result
	targets          []search.Result
	db               map[strfmt.UUID]*models.Thing
	errorOnAggregate error
}

func (f *fakeVectorRepoContextual) get(id strfmt.UUID) (*models.Thing, bool) {
	f.Lock()
	defer f.Unlock()
	t, ok := f.db[id]
	return t, ok
}

func (f *fakeVectorRepoContextual) GetUnclassified(ctx context.Context,
	k kind.Kind, class string, properties []string,
	filter *libfilters.LocalFilter) ([]search.Result, error) {
	if k != kind.Thing {
		return nil, fmt.Errorf("unsupported kind in test fake: %v", k)
	}

	return f.unclassified, nil
}

func (f *fakeVectorRepoContextual) AggregateNeighbors(ctx context.Context, vector []float32,
	ki kind.Kind, class string, properties []string, k int,
	filter *libfilters.LocalFilter) ([]NeighborRef, error) {
	panic("not implemented")
}

func (f *fakeVectorRepoContextual) PutThing(ctx context.Context, thing *models.Thing, vector []float32) error {
	f.Lock()
	defer f.Unlock()
	f.db[thing.ID] = thing
	return nil
}

func (f *fakeVectorRepoContextual) PutAction(ctx context.Context, thing *models.Action, vector []float32) error {
	return fmt.Errorf("put action not implemented in fake")
}

func (f *fakeVectorRepoContextual) VectorClassSearch(ctx context.Context,
	params traverser.GetParams) ([]search.Result, error) {
	if params.SearchVector == nil {
		filteredTargets := matchClassName(f.targets, params.ClassName)
		return filteredTargets, nil
	}

	// simulate that this takes some time
	time.Sleep(5 * time.Millisecond)

	if params.Kind != kind.Thing {
		return nil, fmt.Errorf("unsupported kind in test fake: %v", params.Kind)
	}

	filteredTargets := matchClassName(f.targets, params.ClassName)
	results := filteredTargets
	sort.SliceStable(results, func(i, j int) bool {
		simI, err := cosineSim(results[i].Vector, params.SearchVector)
		if err != nil {
			panic(err.Error())
		}

		simJ, err := cosineSim(results[j].Vector, params.SearchVector)
		if err != nil {
			panic(err.Error())
		}
		return simI > simJ
	})

	if len(results) == 0 {
		return nil, f.errorOnAggregate
	}

	var out = []search.Result{
		results[0],
	}

	return out, f.errorOnAggregate
}

func matchClassName(in []search.Result, className string) []search.Result {
	var out []search.Result
	for _, item := range in {
		if item.ClassName == className {
			out = append(out, item)
		}
	}

	return out
}
