/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

package traverser

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
)

type fakeRepo struct {
	GetThingResponse     *models.Thing
	UpdateThingParameter *models.Thing
}

func (f *fakeRepo) LocalAggregate(ctx context.Context, params *AggregateParams) (interface{}, error) {
	panic("not implemented")
}

func (f *fakeRepo) LocalFetchFuzzy(ctx context.Context, words []string) (interface{}, error) {
	panic("not implemented")
}

func (f *fakeRepo) LocalFetchKindClass(ctx context.Context, params *FetchParams) (interface{}, error) {
	panic("not implemented")
}

func (f *fakeRepo) LocalGetClass(ctx context.Context, params *LocalGetParams) (interface{}, error) {
	panic("not implemented")
}

func (f *fakeRepo) LocalGetMeta(ctx context.Context, params *GetMetaParams) (interface{}, error) {
	panic("not implemented")
}

type fakeSchemaManager struct {
	CalledWith struct {
		kind      kind.Kind
		fromClass string
		property  string
		toClass   string
	}
	GetSchemaResponse schema.Schema
}

func (f *fakeSchemaManager) UpdatePropertyAddDataType(ctx context.Context, principal *models.Principal,
	k kind.Kind, fromClass, property, toClass string) error {
	f.CalledWith = struct {
		kind      kind.Kind
		fromClass string
		property  string
		toClass   string
	}{
		kind:      k,
		fromClass: fromClass,
		property:  property,
		toClass:   toClass,
	}
	return nil
}

func (f *fakeSchemaManager) GetSchema(principal *models.Principal) (schema.Schema, error) {
	return f.GetSchemaResponse, nil
}

type fakeLocks struct{}

func (f *fakeLocks) LockConnector() (func() error, error) {
	return func() error { return nil }, nil
}

func (f *fakeLocks) LockSchema() (func() error, error) {
	return func() error { return nil }, nil
}

type fakeVectorizer struct{}

func (f *fakeVectorizer) Thing(ctx context.Context, thing *models.Thing) ([]float32, error) {
	panic("not implemented")
}

func (f *fakeVectorizer) Action(ctx context.Context, thing *models.Action) ([]float32, error) {
	panic("not implemented")
}

func (f *fakeVectorizer) Corpi(ctx context.Context, corpi []string) ([]float32, error) {
	panic("not implemented")
}

type fakeNetwork struct {
	peerURI string
}

func (f *fakeNetwork) ListPeers() (peers.Peers, error) {
	myPeers := peers.Peers{
		peers.Peer{
			Name: "BestWeaviate",
			URI:  strfmt.URI(f.peerURI),
			Schema: schema.Schema{
				Things: &models.SemanticSchema{
					Classes: []*models.SemanticSchemaClass{
						&models.SemanticSchemaClass{
							Class: "BestThing",
						},
					},
				},
			},
		},
	}

	return myPeers, nil
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return nil
}

type fakeC11y struct{}

func (f *fakeC11y) IsWordPresent(ctx context.Context, word string) (bool, error) {
	panic("not implemented")
}
func (f *fakeC11y) SafeGetSimilarWordsWithCertainty(ctx context.Context, word string, certainty float32) ([]string, error) {
	panic("not implemented")
}
func (f *fakeC11y) SchemaSearch(ctx context.Context, p SearchParams) (SearchResults, error) {
	panic("not implemented")
}

type fakeVectorRepo struct{}

func (f *fakeVectorRepo) PutThing(ctx context.Context, index string,
	concept *models.Thing, vector []float32) error {
	return nil
}
func (f *fakeVectorRepo) PutAction(ctx context.Context, index string,
	concept *models.Action, vector []float32) error {
	return nil
}
func (f *fakeVectorRepo) VectorSearch(ctx context.Context, index string,
	vector []float32) ([]VectorSearchResult, error) {
	return nil, nil
}
