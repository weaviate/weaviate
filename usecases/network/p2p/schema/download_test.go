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

package schema

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/stretchr/testify/assert"
)

func TestDownloadSchemaFromPeer(t *testing.T) {
	var (
		result     schema.Schema
		err        error
		peerServer *httptest.Server
		peer       peers.Peer
	)

	arrange := func(matchers ...requestMatcher) {
		peerServer = fakePeerSchemaEndpoint(t, matchers...)
		peer = peers.Peer{
			Name: "bestPeer",
			URI:  strfmt.URI(peerServer.URL),
		}
	}

	act := func() {
		result, err = download(peer)
	}

	cleanUp := func() {
		peerServer.Close()
	}

	t.Run("no error should occur", func(t *testing.T) {
		arrange()
		act()
		cleanUp()
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}
	})

	t.Run("result should match the schema", func(t *testing.T) {
		arrange()
		act()
		cleanUp()
		assert.Equal(t, result, sampleSchema(), "result should match the schema")
	})

	t.Run("peer should be called", func(t *testing.T) {
		called := false
		matcher := func(t *testing.T, r *http.Request) {
			called = true
		}
		arrange(matcher)
		act()

		if called == false {
			t.Error("handler was never called")
		}

		cleanUp()
	})

	t.Run("request should be GET /schema", func(t *testing.T) {
		matcher := func(t *testing.T, r *http.Request) {
			if r.Method != "GET" {
				t.Fatalf("expected method to be GET, but was %s", r.Method)
			}

			expectedURI := "/v1/schema"
			if r.RequestURI != expectedURI {
				t.Fatalf("expected uri to be %s, but was %s",
					expectedURI, r.RequestURI)
			}
		}
		arrange(matcher)
		act()

		cleanUp()
	})

}

type requestMatcher func(t *testing.T, r *http.Request)

func fakePeerSchemaEndpoint(t *testing.T, matchers ...requestMatcher) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, matcher := range matchers {
			matcher(t, r)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sampleSchema())
	}))
	return ts
}

func sampleSchema() schema.Schema {
	return schema.Schema{
		Actions: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class: "Flight",
				},
			},
		},
		Things: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class: "Airplane",
				},
			},
		},
	}
}
