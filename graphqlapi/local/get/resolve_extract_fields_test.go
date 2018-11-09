// These tests verify that the extraction of fields from a result is correct.
package local_get

import (
	//"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	common_local "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_resolver"
	test_helper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"testing"
  "encoding/json"
)

func TestExtractIntField(t *testing.T) {
  t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalGetClassParams{
		Kind:       kind.ACTION_KIND,
		ClassName:  "SomeAction",
		Filters:    &common_local.LocalFilters{},
		Properties: []SelectProperty{{Name: "intField"}},
	}

  expectedResponse := `{ "Root": { "Actions": { "SomeAction": [ { "intField": 42} ] } } }`
  var expected interface{}
  err := json.Unmarshal([]byte(`[ { "intField": 42} ]`), &expected)
  if err != nil { panic(err) }

  //oneResult := test_helper.SingletonThunk(LocalGetClassResult {
  //  Kind: kind.ACTION_KIND,
	//	ClassName:  schema.AssertValidClassName("SomeAction"),
  //  Properties: ResolvedProperties {
  //    "intField": ResolvedProperty {
  //      Value: interface{}(42),
  //    },
  //  },
  //})

	resolver.On("LocalGetClass", expectedParams).
		Return(test_helper.IdentityThunk(expected), nil).Once()

  query := "{ Root { Actions { SomeAction { intField } } } }"
  resolver.AssertJSONResponse(t, query, expectedResponse)
}
