//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package p2p

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/genesis/client"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/sirupsen/logrus/hooks/test"
)

func TestGetExistingPeer(t *testing.T) {
	peer := peers.Peer{
		ID:   strfmt.UUID("some-id"),
		Name: "best-peer",
		URI:  "http://best-peer.com",
	}

	logger, _ := test.NewNullLogger()
	subject := network{
		peers:  []peers.Peer{peer},
		logger: logger,
	}

	actual, err := subject.GetPeerByName("best-peer")

	t.Run("should not error", func(t *testing.T) {
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("should return correct peer", func(t *testing.T) {
		if actual.ID != peer.ID {
			t.Errorf("%s does not match, wanted %s, gut got %s", "ID", peer.ID, actual.ID)
		}

		if actual.Name != peer.Name {
			t.Errorf("%s does not match, wanted %s, gut got %s", "Name", peer.Name, actual.Name)
		}

		if actual.URI != peer.URI {
			t.Errorf("%s does not match, wanted %s, gut got %s", "URI", peer.URI, actual.URI)
		}
	})

}

func TestGetWrongPeer(t *testing.T) {
	peer := peers.Peer{
		ID:   strfmt.UUID("some-id"),
		Name: "best-peer",
		URI:  "http://best-peer.com",
	}

	logger, _ := test.NewNullLogger()
	subject := network{
		peers:  []peers.Peer{peer},
		logger: logger,
	}

	_, err := subject.GetPeerByName("worst-peer")

	t.Run("should error with ErrPeerNotFound", func(t *testing.T) {
		if err != ErrPeerNotFound {
			t.Errorf("expected peer not found error, but got %s", err)
		}
	})
}

func TestPingPeer(t *testing.T) {
	var (
		subject      *network
		genesis      *httptest.Server
		schemaGetter *dummySchemaGetter
	)

	arrange := func(matchers ...requestMatcher) {
		genesis = fakeGenesis(t, matchers...)
		genesisURI, _ := url.Parse(genesis.URL)
		transportConfig := client.TransportConfig{
			Host:     genesisURI.Host,
			BasePath: genesisURI.Path,
			Schemes:  []string{genesisURI.Scheme},
		}
		genesisClient := client.NewHTTPClientWithConfig(nil, &transportConfig)

		logger, _ := test.NewNullLogger()
		subject = &network{
			client: *genesisClient,
			peerID: strfmt.UUID("2dd9195c-e321-4025-aace-8cb48522661f"),
			logger: logger,
		}
		schemaGetter = &dummySchemaGetter{schema: sampleSchema()}
		subject.RegisterSchemaGetter(schemaGetter)
	}

	act := func() {
		subject.ping()
	}

	cleanUp := func() {
		genesis.Close()
	}

	t.Run("genesis should be called", func(t *testing.T) {
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

	t.Run("contain schemaHash in body", func(t *testing.T) {
		expectedBody := "{\"schemaHash\":\"a6ad75fcd8fac815872c3abecacb31a8\"}\n"
		matcher := func(t *testing.T, res *http.Request) {
			defer res.Body.Close()
			bodyBytes, _ := ioutil.ReadAll(res.Body)
			if string(bodyBytes) != expectedBody {
				t.Fatalf("for body, wanted \n%#v\nbut got\n%#v\n", expectedBody, string(bodyBytes))
			}
		}
		arrange(matcher)
		act()
		cleanUp()
	})

	t.Run("contain an updated schemaHash if the schema changes", func(t *testing.T) {
		expectedBody := "{\"schemaHash\":\"b01ba8c02c4b8acb7857efa8067d2998\"}\n"
		matcher := func(t *testing.T, res *http.Request) {
			defer res.Body.Close()
			bodyBytes, _ := ioutil.ReadAll(res.Body)
			if string(bodyBytes) != expectedBody {
				t.Fatalf("for body, wanted \n%#v\nbut got\n%#v\n", string(bodyBytes),
					expectedBody)
			}
		}
		arrange(matcher)
		// now we have the initial schema
		schemaGetter.schema.Actions.Classes[0].Class = "UpdatedFlight"
		// now we changed it
		act()
		cleanUp()
	})

}

type dummySchemaGetter struct {
	schema schema.Schema
}

func (d *dummySchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return d.schema
}

func fakeGenesis(t *testing.T, matchers ...requestMatcher) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, matcher := range matchers {
			matcher(t, r)
		}
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
