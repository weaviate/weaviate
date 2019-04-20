/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

package get

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	test_helper "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
	"github.com/creativesoftwarefdn/weaviate/usecases/network/common/peers"
	"github.com/sirupsen/logrus/hooks/test"
)

type mockRequestsLog struct{}

func (m *mockRequestsLog) Register(first string, second string) {

}

type mockResolver struct {
	test_helper.MockResolver
}

func newMockResolver(peers peers.Peers) *mockResolver {
	logger, _ := test.NewNullLogger()
	field, err := Build(&test_helper.SimpleSchema, peers, logger)
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "Get"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": Resolver(mocker), "RequestsLog": RequestsLog(mockLog)}
	return mocker
}

func (m *mockResolver) LocalGetClass(params *kinds.LocalGetParams) (interface{}, error) {
	args := m.Called(params)
	return args.Get(0), args.Error(1)
}

func emptyPeers() peers.Peers {
	return peers.Peers{}
}

func newFakePeerServer(t *testing.T) *fakePeerServer {
	server := &fakePeerServer{t: t}
	server.server = httptest.NewServer(http.HandlerFunc(server.handle))
	return server
}

type fakePeerServer struct {
	t        *testing.T
	server   *httptest.Server
	matchers []http.HandlerFunc
}

func (f *fakePeerServer) handle(w http.ResponseWriter, r *http.Request) {
	for _, matcher := range f.matchers {
		matcher(w, r)
	}
}
