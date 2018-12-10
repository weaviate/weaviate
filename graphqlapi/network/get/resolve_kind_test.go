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
			t.Errorf("should not have errored, but got %s", err)
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
		"Things": "foobar",
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
