package janusgraph

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
	"github.com/graphql-go/graphql"
)

// GetGraph returns the result based on th graphQL request
func (f *Janusgraph) GetGraph(request graphql.ResolveParams) (interface{}, error) {
	return nil, fmt.Errorf("not supported")
}

// GraphqlListThings returns a list of things, similar to
// ListThings (REST API) but takes a graphql.ResolveParams to control
func (f *Janusgraph) GraphqlListThings(request graphql.ResolveParams) (interface{}, error) {
	q := gremlin.G.V().
		HasLabel(THING_LABEL).
		HasString("atClass", request.Info.FieldName).
		// Range(offset, first).
		Values([]string{"uuid"})

	result, err := f.client.Execute(q)
	if err != nil {
		return nil, err
	}

	thingsSchemaAsMaps := []map[string]interface{}{}
	// Get the UUIDs from the first query.
	UUIDs := result.AssertStringSlice()
	for _, uuid := range UUIDs {
		var thingResponse models.ThingGetResponse
		err := f.GetThing(nil, strfmt.UUID(uuid), &thingResponse)
		if err != nil {
			// skip silently; it's probably deleted.
			continue
		}

		schemaMap, ok := thingResponse.Schema.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected schema to be a map[string]interface{}, but it was a %t", thingResponse.Schema)
		}

		thingsSchemaAsMaps = append(thingsSchemaAsMaps, schemaMap)
	}

	return thingsSchemaAsMaps, nil
}
