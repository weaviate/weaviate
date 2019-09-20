package classification

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
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

func (f *fakeClassificationRepo) Put(class models.Classification) error {
	f.Lock()
	defer f.Unlock()

	f.db[class.ID] = class
	return nil
}

func (f *fakeClassificationRepo) Get(id strfmt.UUID) (*models.Classification, error) {
	class, ok := f.db[id]
	if !ok {
		return nil, nil
	}

	return &class, nil
}

// read requests are specified throuh unclassified and classified,
// write requests (Put[Kind]) are stored in the db map
type fakeVectorRepo struct {
	unclassified search.Result
	classified   search.Result
	dp           map[strfmt.UUID]*models.Thing
}

func (f *fakeVectorRepo) GetUnclassified(ctx context.Context, class string,
	properties []string) (*search.Result, error) {
	return &f.unclassified, nil
}

func (f *fakeVectorRepo) AggregateNeighbors(ctx context.Context, vector []float32,
	properties []string, k int) ([]NeighborRef, error) {
	if k != 1 {
		return nil, fmt.Errorf("fake vector repo only supports k=1")
	}

	return nil, nil
}
