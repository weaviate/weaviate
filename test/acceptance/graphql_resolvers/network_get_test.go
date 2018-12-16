package test

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

func TestNetworkGetSimple(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, "{ Network { Get { RemoteWeaviateForAcceptanceTest { Things { City { name } } } } } }")
	cities := result.Get("Network", "Get", "RemoteWeaviateForAcceptanceTest", "Things", "City").AsSlice()

	expected := []interface{}{
		map[string]interface{}{"name": "Hamburg"},
		map[string]interface{}{"name": "New York"},
		map[string]interface{}{"name": "Neustadt an der Weinstraße"},
		map[string]interface{}{"name": "Tokyo"},
	}

	assert.ElementsMatch(t, expected, cities)
}
