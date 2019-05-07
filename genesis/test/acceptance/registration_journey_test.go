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
package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/genesis/client"
	"github.com/semi-technologies/weaviate/genesis/client/operations"
	"github.com/semi-technologies/weaviate/genesis/models"
	"github.com/stretchr/testify/assert"
)

func TestPeerRegistrationJourney(t *testing.T) {
	var (
		newPeerID     strfmt.UUID
		newPeerServer = newPeerServer()
		genesisClient = genesisSwaggerClient()
	)

	newPeerServer.start()
	defer newPeerServer.close()

	t.Run("peers should be empty in the beginning", func(t *testing.T) {
		peers, err := genesisClient.Operations.GenesisPeersList(nil)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		if len(peers.Payload) != 0 {
			t.Fatalf("expected get an empty list of peers at startup, but got \n%#v", peers.Payload)
		}
	})

	t.Run("it should be possible to register a peer", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		params := &operations.GenesisPeersRegisterParams{
			Body: &models.PeerUpdate{
				PeerName: "myFavoritePeer",
				PeerURI:  newPeerServer.url(),
			},
			Context: ctx,
		}
		ok, err := genesisClient.Operations.GenesisPeersRegister(params)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		// Store ID for later
		newPeerID = ok.Payload.Peer.ID
	})

	t.Run("peers should now contain added peer", func(t *testing.T) {
		peers, err := genesisClient.Operations.GenesisPeersList(nil)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		assert.Equal(t, 1, len(peers.Payload), "list of peers should have exactly one entry")
		assert.Equal(t, "myFavoritePeer", peers.Payload[0].PeerName, "peer should match what we registered before")
	})

	t.Run("the peer was informed of an update", func(t *testing.T) {
		// with our current test setup this tests would need to be able
		// to access the host machine that the tests are running on
		// this works locally, but unfortunately not on travis.
		// so we need to skip this particular test on travis until
		// we improve our testing setup.

		if os.Getenv("TRAVIS") == "true" {
			t.Skip()
		}

		equalsBeforeTimeout(t, 1, func() interface{} { return len(newPeerServer.requests()) },
			"there should be one request before the timeout", 10*time.Second)

		request := newPeerServer.requests()[0]
		var peers []map[string]string
		err := json.Unmarshal(request.body, &peers)
		assert.Equal(t, nil, err, "unmarshalling json should not error")
		assert.Equal(t, 1, len(peers), "expected exactly one peer in request")
		assert.Equal(t, string(newPeerID), peers[0]["id"], "expected correct peer id")
		assert.Equal(t, "myFavoritePeer", peers[0]["name"], "expected correct peer name")
	})

	t.Run("a peer can send a ping with a schema hash", func(t *testing.T) {
		params := operations.NewGenesisPeersPingParams().
			WithTimeout(1 * time.Second).
			WithBody(&models.PeerPing{
				SchemaHash: "some-new-schema-hash",
			}).
			WithPeerID(newPeerID)

		_, err := genesisClient.Operations.GenesisPeersPing(params)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}
	})

	t.Run("peers should now be updated", func(t *testing.T) {
		peers, err := genesisClient.Operations.GenesisPeersList(nil)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		assert.Equal(t, 1, len(peers.Payload),
			"list of peers should have exactly one entry")
		assert.Equal(t, "myFavoritePeer", peers.Payload[0].PeerName,
			"peer id should match what we registered before")
		assert.Equal(t, "some-new-schema-hash", peers.Payload[0].SchemaHash,
			"peer schema should match what we registered before")
	})

	t.Run("the peer was informed of an update again (schema update)", func(t *testing.T) {
		// with our current test setup this tests would need to be able
		// to access the host machine that the tests are running on
		// this works locally, but unfortunately not on travis.
		// so we need to skip this particular test on travis until
		// we improve our testing setup.

		if os.Getenv("TRAVIS") == "true" {
			t.Skip()
		}

		equalsBeforeTimeout(t, 2, func() interface{} { return len(newPeerServer.requests()) },
			"there should now be two requests before the timeout", 10*time.Second)

		request := newPeerServer.requests()[1]
		var peers []map[string]string
		err := json.Unmarshal(request.body, &peers)
		assert.Equal(t, nil, err, "unmarshalling json should not error")
		assert.Equal(t, 1, len(peers), "expected exactly one peer in request")
		assert.Equal(t, string(newPeerID), peers[0]["id"], "expected correct peer id")
		assert.Equal(t, "myFavoritePeer", peers[0]["name"], "expected correct peer name")
		assert.Equal(t, "some-new-schema-hash", peers[0]["schemaHash"], "expected correct schema_hash")
	})

	t.Run("peers can be deregistered", func(t *testing.T) {
		params := operations.NewGenesisPeersLeaveParams().
			WithTimeout(1 * time.Second).
			WithPeerID(newPeerID)
		_, err := genesisClient.Operations.GenesisPeersLeave(params)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}
	})
}

func genesisSwaggerClient() *client.WeaviateGenesisServer {
	genesisClient := client.NewHTTPClient(nil)
	transport := httptransport.New("localhost:8111", "/", []string{"http"})
	genesisClient.SetTransport(transport)
	return genesisClient
}

type request struct {
	origRequest *http.Request
	body        []byte
}

type peerServer struct {
	sync.Mutex
	server     *httptest.Server
	requestLog []request
}

func equalsBeforeTimeout(t *testing.T, expected interface{}, actual func() interface{}, message string, timeout time.Duration) {
	interval := 100 * time.Millisecond
	elapsed := time.Duration(0)
	for elapsed < timeout {
		if reflect.DeepEqual(expected, actual()) {
			return
		}

		time.Sleep(interval)
		elapsed = elapsed + interval
	}

	t.Fatalf("waited for %s, but never happened: %s", timeout, message)
}

func newPeerServer() *peerServer {
	return &peerServer{}
}

func (p *peerServer) start() {
	p.server = httptest.NewServer(http.HandlerFunc(p.handle))
}

func (p *peerServer) close() {
	p.server.Close()
}

func (p *peerServer) requests() []request {
	p.Lock()
	defer p.Unlock()
	return p.requestLog
}

func (p *peerServer) url() strfmt.URI {
	parsed, _ := url.Parse(p.server.URL)
	port := parsed.Port()
	return strfmt.URI(fmt.Sprintf("http://host.docker.internal:%s", port))
}

func (p *peerServer) handle(w http.ResponseWriter, r *http.Request) {
	p.Lock()
	defer p.Unlock()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic("could not read body: " + err.Error())
	}

	request := request{
		origRequest: r,
		body:        body,
	}
	p.requestLog = append(p.requestLog, request)
}
