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
package fetch

import (
	testhelper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
)

type mockResolver struct {
	testhelper.MockResolver
}

func newMockResolver() *mockResolver {
	field := Build()
	mocker := &mockResolver{}
	mocker.RootFieldName = "Fetch"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": Resolver(mocker)}
	return mocker
}

func (m *mockResolver) LocalFetchKindClass(params *Params) (interface{}, error) {
	args := m.Called(params)
	return args.Get(0), args.Error(1)
}
