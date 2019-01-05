/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */
package restapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
)

func TestSchemaUpdaterWithEmtpyRefSchema(t *testing.T) {
	err := newReferenceSchemaUpdater(nil, nil, "FooThing", kind.THING_KIND).
		addNetworkDataTypes(nil)

	assert.Nil(t, err, "it does not error with an empty schema")
}

func TestSchemaUpdaterWithOnlyPrimitiveProps(t *testing.T) {
	err := newReferenceSchemaUpdater(nil, nil, "FooThing", kind.THING_KIND).
		addNetworkDataTypes(map[string]interface{}{
			"foo":  "bar",
			"baz":  int64(100),
			"bang": true,
		})

	assert.Nil(t, err, "it does not error with a primitive schema")
}

func TestSchemaUpdaterWithOnlyLocalRefs(t *testing.T) {
	loc := "http://localhost"
	err := newReferenceSchemaUpdater(nil, nil, "FooThing", kind.THING_KIND).
		addNetworkDataTypes(map[string]interface{}{
			"fooRef": &models.SingleRef{
				LocationURL: &loc,
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
	loc := "http://BestWeaviate"
	err := newReferenceSchemaUpdater(schemaManager, network, "FooThing", kind.THING_KIND).
		addNetworkDataTypes(map[string]interface{}{
			"fooRef": &models.SingleRef{
				LocationURL:  &loc,
				NrDollarCref: strfmt.UUID("best-reference"),
				Type:         "NetworkThing",
			},
		})

	//assert
	t.Run("does not error", func(t *testing.T) {
		assert.Nil(t, err, "it does not error with a primitive schema")
	})

	t.Run("correct schema udpate was triggered", func(t *testing.T) {
		call := schemaManager.CalledWith
		assert.Equal(t, kind.THING_KIND, call.kind,
			"thing kind because the from class is a thing")
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
	loc := "http://BestWeaviate"
	err := newReferenceSchemaUpdater(schemaManager, network, "FooAction", kind.ACTION_KIND).
		addNetworkDataTypes(map[string]interface{}{
			"fooRef": &models.SingleRef{
				LocationURL:  &loc,
				NrDollarCref: strfmt.UUID("best-reference"),
				Type:         "NetworkThing",
			},
		})

	//assert
	t.Run("does not error", func(t *testing.T) {
		assert.Nil(t, err, "it does not error with a primitive schema")
	})

	t.Run("correct schema udpate was triggered", func(t *testing.T) {
		call := schemaManager.CalledWith
		assert.Equal(t, kind.ACTION_KIND, call.kind,
			"action kind because the from class is an action")
		assert.Equal(t, "FooAction", call.fromClass, "correct from class")
		assert.Equal(t, "fooRef", call.property, "correct property")
		assert.Equal(t, "BestWeaviate/BestThing", call.toClass, "correct to class")
	})
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

type fakeSchemaManager struct {
	CalledWith struct {
		kind      kind.Kind
		fromClass string
		property  string
		toClass   string
	}
}

func (f *fakeSchemaManager) UpdatePropertyAddDataType(k kind.Kind, fromClass, property, toClass string) error {
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
	body := models.ThingGetResponse{
		ThingID: "best-reference",
		Thing: models.Thing{
			ThingCreate: models.ThingCreate{
				AtClass: "BestThing",
			},
		},
	}
	json.NewEncoder(w).Encode(body)
}
