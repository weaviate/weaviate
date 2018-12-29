/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package local_get

import (
	"fmt"

	test_helper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
)

type mockResolver struct {
	test_helper.MockResolver
}

func newMockResolver(peers peers.Peers) *mockResolver {
	field, err := Build(&test_helper.SimpleSchema, peers)
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}
	mocker := &mockResolver{}
	mocker.RootFieldName = "Get"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": Resolver(mocker)}
	return mocker
}
func (m *mockResolver) LocalGetClass(params *LocalGetClassParams) (func() interface{}, error) {
	args := m.Called(params)
	return args.Get(0).(func() interface{}), args.Error(1)
}

func emptyPeers() peers.Peers {
	return peers.Peers{}
}
