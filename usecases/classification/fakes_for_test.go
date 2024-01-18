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
	"math"
	"sort"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	libfilters "github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
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

func (f *fakeSchemaGetter) ShardOwner(class, shard string) (string, error) {
	return shard, nil
}

func (f *fakeSchemaGetter) ShardReplicas(class, shard string) ([]string, error) {
	return []string{shard}, nil
}

func (f *fakeSchemaGetter) TenantShard(class, tenant string) (string, string) {
	return tenant, models.TenantActivityStatusHOT
}
func (f *fakeSchemaGetter) ShardFromUUID(class string, uuid []byte) string { return string(uuid) }

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
) ([]NeighborRef, error) {
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
	return []search.Result{}, nil
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
) ([]NeighborRef, error) {
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

func matchClassName(in []search.Result, className string) []search.Result {
	var out []search.Result
	for _, item := range in {
		if item.ClassName == className {
			out = append(out, item)
		}
	}

	return out
}

type fakeModuleClassifyFn struct {
	fakeExactCategoryMappings map[string]string
	fakeMainCategoryMappings  map[string]string
}

func NewFakeModuleClassifyFn() *fakeModuleClassifyFn {
	return &fakeModuleClassifyFn{
		fakeExactCategoryMappings: map[string]string{
			"75ba35af-6a08-40ae-b442-3bec69b355f9": "1b204f16-7da6-44fd-bbd2-8cc4a7414bc3",
			"a2bbcbdc-76e1-477d-9e72-a6d2cfb50109": "ec500f39-1dc9-4580-9bd1-55a8ea8e37a2",
			"069410c3-4b9e-4f68-8034-32a066cb7997": "ec500f39-1dc9-4580-9bd1-55a8ea8e37a2",
			"06a1e824-889c-4649-97f9-1ed3fa401d8e": "027b708a-31ca-43ea-9001-88bec864c79c",
		},
		fakeMainCategoryMappings: map[string]string{
			"6402e649-b1e0-40ea-b192-a64eab0d5e56": "5a3d909a-4f0d-4168-8f5c-cd3074d1e79a",
			"f850439a-d3cd-4f17-8fbf-5a64405645cd": "39c6abe3-4bbe-4c4e-9e60-ca5e99ec6b4e",
			"069410c3-4b9e-4f68-8034-32a066cb7997": "39c6abe3-4bbe-4c4e-9e60-ca5e99ec6b4e",
		},
	}
}

func (c *fakeModuleClassifyFn) classifyFn(item search.Result, itemIndex int,
	params models.Classification, filters modulecapabilities.Filters, writer modulecapabilities.Writer,
) error {
	var classified []string

	classifiedProp := c.fakeClassification(&item, "exactCategory", c.fakeExactCategoryMappings)
	if len(classifiedProp) > 0 {
		classified = append(classified, classifiedProp)
	}

	classifiedProp = c.fakeClassification(&item, "mainCategory", c.fakeMainCategoryMappings)
	if len(classifiedProp) > 0 {
		classified = append(classified, classifiedProp)
	}

	c.extendItemWithObjectMeta(&item, params, classified)

	err := writer.Store(item)
	if err != nil {
		return fmt.Errorf("store %s/%s: %v", item.ClassName, item.ID, err)
	}
	return nil
}

func (c *fakeModuleClassifyFn) fakeClassification(item *search.Result, propName string,
	fakes map[string]string,
) string {
	if target, ok := fakes[item.ID.String()]; ok {
		beacon := "weaviate://localhost/" + target
		item.Schema.(map[string]interface{})[propName] = models.MultipleRef{
			&models.SingleRef{
				Beacon:         strfmt.URI(beacon),
				Classification: nil,
			},
		}
		return propName
	}
	return ""
}

func (c *fakeModuleClassifyFn) extendItemWithObjectMeta(item *search.Result,
	params models.Classification, classified []string,
) {
	if item.AdditionalProperties == nil {
		item.AdditionalProperties = models.AdditionalProperties{}
	}

	item.AdditionalProperties["classification"] = additional.Classification{
		ID:               params.ID,
		Scope:            params.ClassifyProperties,
		ClassifiedFields: classified,
		Completed:        strfmt.DateTime(time.Now()),
	}
}

type fakeModulesProvider struct {
	fakeModuleClassifyFn *fakeModuleClassifyFn
}

func NewFakeModulesProvider() *fakeModulesProvider {
	return &fakeModulesProvider{NewFakeModuleClassifyFn()}
}

func (m *fakeModulesProvider) ParseClassifierSettings(name string,
	params *models.Classification,
) error {
	return nil
}

func (m *fakeModulesProvider) GetClassificationFn(className, name string,
	params modulecapabilities.ClassifyParams,
) (modulecapabilities.ClassifyItemFn, error) {
	if name == "text2vec-contextionary-custom-contextual" {
		return m.fakeModuleClassifyFn.classifyFn, nil
	}
	return nil, errors.Errorf("classifier %s not found", name)
}
