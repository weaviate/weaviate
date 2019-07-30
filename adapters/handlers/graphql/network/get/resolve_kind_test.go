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

package network_get

import (
	"testing"

	"github.com/graphql-go/graphql"
)

func TestResolveThingProperInput(t *testing.T) {
	type City struct {
		foo string
	}

	source := map[string]interface{}{
		"Things": map[string]interface{}{
			"City": []City{
				{foo: "bar"},
				{foo: "baz"},
			},
		},
		"RequestsLog": &mockRequestsLog{},
	}

	params := graphql.ResolveParams{
		Source: source,
		Info: graphql.ResolveInfo{
			FieldName: "City",
		},
	}

	var (
		result interface{}
		err    error
	)

	result, err = ResolveThing(params)

	t.Run("should not error", func(t *testing.T) {
		if err != nil {
			t.Errorf("should not have errored, but got '%s'", err)
			return
		}
	})

	t.Run("should resolve to list of cities", func(t *testing.T) {
		cities, ok := result.([]City)
		if !ok {
			t.Error("result was not []City")
			return
		}

		if len(cities) != 2 {
			t.Errorf("expected cities to have len 2, but got %d", len(cities))
			return
		}

		if (cities[0] != City{foo: "bar"} && cities[1] != City{foo: "baz"}) {
			t.Errorf("expected list of cities, but got %#v", result)
			return
		}
	})
}

func TestResolveThingBadInput(t *testing.T) {
	source := map[string]interface{}{
		"Things":      "foobar",
		"RequestsLog": &mockRequestsLog{},
	}

	params := graphql.ResolveParams{
		Source: source,
		Info: graphql.ResolveInfo{
			FieldName: "City",
		},
	}

	var (
		err error
	)

	_, err = ResolveThing(params)

	t.Run("should error", func(t *testing.T) {
		if err == nil {
			t.Error("expected an error, but got none")
			return
		}
	})
}
