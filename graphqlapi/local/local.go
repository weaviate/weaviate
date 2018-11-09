package local

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	local_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	local_get_meta "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get_meta"
	"github.com/graphql-go/graphql"
)

// Build the local queries from the database schema.
func Build(dbSchema *schema.Schema) (*graphql.Field, error) {
	getField, err := local_get.Build(dbSchema)
	if err != nil {
		return nil, err
	}

	getMetaField, err := local_get_meta.Build(dbSchema)
	if err != nil {
		return nil, err
	}

	localObject := graphql.NewObject(graphql.ObjectConfig{
		Name: "WeaviateLocalObj",
		Fields: graphql.Fields{
			"Get":     getField,
			"GetMeta": getMetaField,
		},
		Description: "Type of query on the local Weaviate",
	})

	localField := graphql.Field{
		Type:        localObject,
		Description: "Query a local Weaviate instance",
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			fmt.Printf("- localGetAndMetaObjectResolver (pass on true)\n")
			// This step does nothing; all ways allow the resolver to continue
			return true, nil
		},
	}

	return &localField, nil
}

func buildGetMeta(dbSchema *schema.Schema) (*graphql.Field, error) {
	return nil, nil
}

/*

type Foo struct {
	Name string
}

var FieldFooType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Foo",
	Fields: graphql.Fields{
		"name": &graphql.Field{Type: graphql.String},
	},
})

type Bar struct {
	Name string
}

var FieldBarType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Bar",
	Fields: graphql.Fields{
		"name": &graphql.Field{Type: graphql.String},
	},
})

// QueryType fields: `concurrentFieldFoo` and `concurrentFieldBar` are resolved
// concurrently because they belong to the same field-level and their `Resolve`
// function returns a function (thunk).
var queryType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Query",
	Fields: graphql.Fields{
		"concurrentFieldFoo": &graphql.Field{
			Type: FieldFooType,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				var foo = Foo{Name: "Foo's name"}
				return func() (interface{}) {
					return &foo
				}, nil
			},
		},
		"concurrentFieldBar": &graphql.Field{
			Type: FieldBarType,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
        fmt.Printf("concurrentFieldBar\n")
				type result struct {
					data interface{}
					err  error
				}
				ch := make(chan *result, 1)
				go func() {
					defer close(ch)
					bar := &Bar{Name: "Bar's name"}
					ch <- &result{data: bar, err: nil}
				}()
				return func() (interface{}) {
					r := <-ch
          panic("baad")
					return r.data
				}, nil
			},
		},
	},
})

*/
