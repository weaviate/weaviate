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

package classification

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	libfilters "github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	usecasesclassfication "github.com/weaviate/weaviate/usecases/classification"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type fakeSchemaGetter struct {
	schema schema.Schema
}

func (f *fakeSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

func (f *fakeSchemaGetter) CopyShardingState(class string) *sharding.State {
	panic("not implemented")
}

func (f *fakeSchemaGetter) ShardOwner(class, shard string) (string, error)      { return "", nil }
func (f *fakeSchemaGetter) ShardReplicas(class, shard string) ([]string, error) { return nil, nil }

func (f *fakeSchemaGetter) TenantShard(class, tenant string) (string, string) {
	return tenant, models.TenantActivityStatusHOT
}
func (f *fakeSchemaGetter) ShardFromUUID(class string, uuid []byte) string { return "" }

func (f *fakeSchemaGetter) Nodes() []string {
	panic("not implemented")
}

func (f *fakeSchemaGetter) NodeName() string {
	panic("not implemented")
}

func (f *fakeSchemaGetter) ClusterHealthScore() int {
	panic("not implemented")
}

func (f *fakeSchemaGetter) ResolveParentNodes(string, string,
) (map[string]string, error) {
	panic("not implemented")
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
		db:           map[strfmt.UUID]*models.Object{},
	}
}

// read requests are specified through unclassified and classified,
// write requests (Put[Kind]) are stored in the db map
type fakeVectorRepoKNN struct {
	sync.Mutex
	unclassified      []search.Result
	classified        []search.Result
	db                map[strfmt.UUID]*models.Object
	errorOnAggregate  error
	batchStorageDelay time.Duration
}

func (f *fakeVectorRepoKNN) GetUnclassified(ctx context.Context,
	class string, properties []string,
	filter *libfilters.LocalFilter,
) ([]search.Result, error) {
	f.Lock()
	defer f.Unlock()
	return f.unclassified, nil
}

func (f *fakeVectorRepoKNN) AggregateNeighbors(ctx context.Context, vector []float32,
	class string, properties []string, k int,
	filter *libfilters.LocalFilter,
) ([]usecasesclassfication.NeighborRef, error) {
	f.Lock()
	defer f.Unlock()

	// simulate that this takes some time
	time.Sleep(1 * time.Millisecond)

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

	var out []usecasesclassfication.NeighborRef
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

		out = append(out, usecasesclassfication.NeighborRef{
			Beacon:       refs[0].Beacon,
			WinningCount: 1,
			OverallCount: 1,
			LosingCount:  1,
			Property:     propName,
		})
	}

	return out, f.errorOnAggregate
}

func (f *fakeVectorRepoKNN) ZeroShotSearch(ctx context.Context, vector []float32,
	class string, properties []string,
	filter *libfilters.LocalFilter,
) ([]search.Result, error) {
	panic("not implemented")
}

func (f *fakeVectorRepoKNN) VectorSearch(ctx context.Context,
	params dto.GetParams,
) ([]search.Result, error) {
	f.Lock()
	defer f.Unlock()
	return nil, fmt.Errorf("vector class search not implemented in fake")
}

func (f *fakeVectorRepoKNN) BatchPutObjects(ctx context.Context, objects objects.BatchObjects, repl *additional.ReplicationProperties) (objects.BatchObjects, error) {
	f.Lock()
	defer f.Unlock()

	if f.batchStorageDelay > 0 {
		time.Sleep(f.batchStorageDelay)
	}

	for _, batchObject := range objects {
		f.db[batchObject.Object.ID] = batchObject.Object
	}
	return objects, nil
}

func (f *fakeVectorRepoKNN) get(id strfmt.UUID) (*models.Object, bool) {
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
		db:           map[strfmt.UUID]*models.Object{},
	}
}

// read requests are specified through unclassified and classified,
// write requests (Put[Kind]) are stored in the db map
type fakeVectorRepoContextual struct {
	sync.Mutex
	unclassified     []search.Result
	targets          []search.Result
	db               map[strfmt.UUID]*models.Object
	errorOnAggregate error
}

func (f *fakeVectorRepoContextual) get(id strfmt.UUID) (*models.Object, bool) {
	f.Lock()
	defer f.Unlock()
	t, ok := f.db[id]
	return t, ok
}

func (f *fakeVectorRepoContextual) GetUnclassified(ctx context.Context,
	class string, properties []string,
	filter *libfilters.LocalFilter,
) ([]search.Result, error) {
	return f.unclassified, nil
}

func (f *fakeVectorRepoContextual) AggregateNeighbors(ctx context.Context, vector []float32,
	class string, properties []string, k int,
	filter *libfilters.LocalFilter,
) ([]usecasesclassfication.NeighborRef, error) {
	panic("not implemented")
}

func (f *fakeVectorRepoContextual) ZeroShotSearch(ctx context.Context, vector []float32,
	class string, properties []string,
	filter *libfilters.LocalFilter,
) ([]search.Result, error) {
	panic("not implemented")
}

func (f *fakeVectorRepoContextual) BatchPutObjects(ctx context.Context, objects objects.BatchObjects, repl *additional.ReplicationProperties) (objects.BatchObjects, error) {
	f.Lock()
	defer f.Unlock()
	for _, batchObject := range objects {
		f.db[batchObject.Object.ID] = batchObject.Object
	}
	return objects, nil
}

func (f *fakeVectorRepoContextual) VectorSearch(ctx context.Context,
	params dto.GetParams,
) ([]search.Result, error) {
	if params.SearchVector == nil {
		filteredTargets := matchClassName(f.targets, params.ClassName)
		return filteredTargets, nil
	}

	// simulate that this takes some time
	time.Sleep(5 * time.Millisecond)

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

	out := []search.Result{
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

type fakeModulesProvider struct {
	contextualClassifier modulecapabilities.Classifier
}

func (fmp *fakeModulesProvider) VectorFromInput(ctx context.Context, className string, input string) ([]float32, error) {
	panic("not implemented")
}

func NewFakeModulesProvider(vectorizer *fakeVectorizer) *fakeModulesProvider {
	return &fakeModulesProvider{New(vectorizer)}
}

func (fmp *fakeModulesProvider) ParseClassifierSettings(name string,
	params *models.Classification,
) error {
	return fmp.contextualClassifier.ParseClassifierSettings(params)
}

func (fmp *fakeModulesProvider) GetClassificationFn(className, name string,
	params modulecapabilities.ClassifyParams,
) (modulecapabilities.ClassifyItemFn, error) {
	return fmp.contextualClassifier.ClassifyFn(params)
}
