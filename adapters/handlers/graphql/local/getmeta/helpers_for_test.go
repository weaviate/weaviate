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
package getmeta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/usecases/config"
	testhelper "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/test/helper"
)

type mockRequestsLog struct{}

func (m *mockRequestsLog) Register(first string, second string) {

}

type mockResolver struct {
	testhelper.MockResolver
}

func newMockResolver(cfg config.Config) *mockResolver {
	field, err := Build(&testhelper.CarSchema, cfg)
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "GetMeta"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{
		"Resolver":    Resolver(mocker),
		"RequestsLog": mockLog,
		"Config":      cfg,
	}
	return mocker
}

func (m *mockResolver) LocalGetMeta(params *Params) (interface{}, error) {
	args := m.Called(params)
	return args.Get(0), args.Error(1)
}
