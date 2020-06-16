//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package kinds

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/stretchr/testify/mock"
)

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

type fakeVectorizer struct {
	mock.Mock
}

func (f *fakeVectorizer) Thing(ctx context.Context, thing *models.Thing) ([]float32, error) {
	args := f.Called(thing)
	return args.Get(0).([]float32), args.Error(1)
}

func (f *fakeVectorizer) Action(ctx context.Context, action *models.Action) ([]float32, error) {
	args := f.Called(action)
	return args.Get(0).([]float32), args.Error(1)
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
				Things: &models.Schema{
					Classes: []*models.Class{
						&models.Class{
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

type fakeVectorRepo struct {
	mock.Mock
}

func (f *fakeVectorRepo) Exists(ctx context.Context,
	id strfmt.UUID) (bool, error) {
	args := f.Called(id)
	return args.Bool(0), args.Error(1)
}

func (f *fakeVectorRepo) ThingByID(ctx context.Context,
	id strfmt.UUID, props traverser.SelectProperties, underscores traverser.UnderscoreProperties) (*search.Result, error) {
	args := f.Called(id, props, underscores)
	return args.Get(0).(*search.Result), args.Error(1)
}

func (f *fakeVectorRepo) ActionByID(ctx context.Context,
	id strfmt.UUID, props traverser.SelectProperties, underscores traverser.UnderscoreProperties) (*search.Result, error) {
	args := f.Called(id, props, underscores)
	return args.Get(0).(*search.Result), args.Error(1)
}

func (f *fakeVectorRepo) ThingSearch(ctx context.Context, limit int,
	filters *filters.LocalFilter, underscores traverser.UnderscoreProperties) (search.Results, error) {
	return nil, nil
}

func (f *fakeVectorRepo) ActionSearch(ctx context.Context, limit int,
	filters *filters.LocalFilter, underscores traverser.UnderscoreProperties) (search.Results, error) {
	return nil, nil
}

func (f *fakeVectorRepo) PutThing(ctx context.Context,
	concept *models.Thing, vector []float32) error {
	args := f.Called(concept, vector)
	return args.Error(0)
}
func (f *fakeVectorRepo) PutAction(ctx context.Context,
	concept *models.Action, vector []float32) error {
	args := f.Called(concept, vector)
	return args.Error(0)
}

func (f *fakeVectorRepo) BatchPutThings(ctx context.Context, batch BatchThings) (BatchThings, error) {
	args := f.Called(batch)
	return batch, args.Error(0)
}

func (f *fakeVectorRepo) BatchPutActions(ctx context.Context, batch BatchActions) (BatchActions, error) {
	args := f.Called(batch)
	return batch, args.Error(0)
}

func (f *fakeVectorRepo) AddBatchReferences(ctx context.Context, batch BatchReferences) (BatchReferences, error) {
	args := f.Called(batch)
	return batch, args.Error(0)
}

func (f *fakeVectorRepo) Merge(ctx context.Context, merge MergeDocument) error {
	args := f.Called(merge)
	return args.Error(0)
}

func (f *fakeVectorRepo) DeleteAction(ctx context.Context,
	className string, id strfmt.UUID) error {
	args := f.Called(className, id)
	return args.Error(0)
}

func (f *fakeVectorRepo) DeleteThing(ctx context.Context,
	className string, id strfmt.UUID) error {
	args := f.Called(className, id)
	return args.Error(0)
}

func (f *fakeVectorRepo) AddReference(ctx context.Context, kind kind.Kind, source strfmt.UUID, prop string,
	ref *models.SingleRef) error {
	args := f.Called(kind, source, prop, ref)
	return args.Error(0)
}
