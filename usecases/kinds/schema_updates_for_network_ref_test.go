/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package kinds

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaUpdaterWithEmtpyRefSchema(t *testing.T) {
	err := newReferenceSchemaUpdater(context.TODO(), nil, nil, nil, "FooThing", kind.Thing).
		addNetworkDataTypes(nil)

	assert.Nil(t, err, "it does not error with an empty schema")
}

func TestSchemaUpdaterWithOnlyPrimitiveProps(t *testing.T) {
	err := newReferenceSchemaUpdater(context.TODO(), nil, nil, nil, "FooThing", kind.Thing).
		addNetworkDataTypes(map[string]interface{}{
			"foo":  "bar",
			"baz":  int64(100),
			"bang": true,
		})

	assert.Nil(t, err, "it does not error with a primitive schema")
}

func TestSchemaUpdaterWithOnlyLocalRefs(t *testing.T) {
	loc := "weaviate://localhost/things/fcc72dff-7feb-4a84-b580-fa0261aea776"
	err := newReferenceSchemaUpdater(context.TODO(), nil, nil, nil, "FooThing", kind.Thing).
		addNetworkDataTypes(map[string]interface{}{
			"fooRef": &models.SingleRef{
				NrDollarCref: strfmt.URI(loc),
			},
		})

	assert.Nil(t, err, "it does not error with a primitive schema")
}

func TestSchemaUpdaterWithSingleNetworkRefFromThingToThing(t *testing.T) {
	// arrange
	schemaManager := &fakeSchemaManager{}
	server := newFakeServer(t)
	network := &fakeNetwork{peerURI: server.server.URL}
	server.matchers = []http.HandlerFunc{happyPathHandler}

	// act
	refID := "30ad9bd2-1e33-460a-bea7-dcce72d086a1"
	loc := "http://BestWeaviate/things/" + refID
	err := newReferenceSchemaUpdater(context.TODO(), nil, schemaManager, network, "FooThing", kind.Thing).
		addNetworkDataTypes(map[string]interface{}{
			"fooRef": &models.SingleRef{
				NrDollarCref: strfmt.URI(loc),
			},
		})

	//assert
	t.Run("does not error", func(t *testing.T) {
		require.Nil(t, err, "it does not error with a primitive schema")
	})

	t.Run("correct schema udpate was triggered", func(t *testing.T) {
		call := schemaManager.CalledWith
		// assert.Equal(t, kind.Thing, call.kind,
		// 	"thing kind because the from class is a thing")
		assert.Equal(t, "FooThing", call.fromClass, "correct from class")
		assert.Equal(t, "fooRef", call.property, "correct property")
		assert.Equal(t, "BestWeaviate/BestThing", call.toClass, "correct to class")
	})
}

func TestSchemaUpdaterWithSingleNetworkRefFromActinToThing(t *testing.T) {
	// arrange
	schemaManager := &fakeSchemaManager{}
	server := newFakeServer(t)
	network := &fakeNetwork{peerURI: server.server.URL}
	server.matchers = []http.HandlerFunc{happyPathHandler}

	// act
	loc := "http://BestWeaviate/things/fbe157e9-3e4c-4be6-995d-d6d5ab49a84b"
	err := newReferenceSchemaUpdater(context.TODO(), nil, schemaManager, network, "FooAction", kind.Action).
		addNetworkDataTypes(map[string]interface{}{
			"fooRef": &models.SingleRef{
				NrDollarCref: strfmt.URI(loc),
			},
		})

	//assert
	t.Run("does not error", func(t *testing.T) {
		assert.Nil(t, err, "it does not error with a primitive schema")
	})

	t.Run("correct schema udpate was triggered", func(t *testing.T) {
		call := schemaManager.CalledWith
		assert.Equal(t, kind.Action, call.kind,
			"action kind because the from class is an action")
		assert.Equal(t, "FooAction", call.fromClass, "correct from class")
		assert.Equal(t, "fooRef", call.property, "correct property")
		assert.Equal(t, "BestWeaviate/BestThing", call.toClass, "correct to class")
	})
}

func newFakeServer(t *testing.T) *fakeServer {
	server := &fakeServer{t: t}
	server.server = httptest.NewServer(http.HandlerFunc(server.handle))
	return server
}

type fakeServer struct {
	t        *testing.T
	server   *httptest.Server
	matchers []http.HandlerFunc
}

func (f *fakeServer) handle(w http.ResponseWriter, r *http.Request) {
	for _, matcher := range f.matchers {
		matcher(w, r)
	}
}

var happyPathHandler = func(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	body := models.Thing{
		ID:    "best-reference",
		Class: "BestThing",
	}
	json.NewEncoder(w).Encode(body)
}
