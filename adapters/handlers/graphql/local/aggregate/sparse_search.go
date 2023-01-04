package aggregate

import (
	"github.com/tailor-inc/graphql"
)

//func bm25Argument(className string) *graphql.ArgumentConfig {
//	prefix := fmt.Sprintf("GetObjects%s", className)
//	return &graphql.ArgumentConfig{
//		Type: graphql.NewInputObject(
//			graphql.InputObjectConfig{
//				Name:   fmt.Sprintf("%sBm25InpObj", prefix),
//				Fields: bm25Fields(prefix),
//			},
//		),
//	}
//}

func bm25Fields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"query": &graphql.InputObjectFieldConfig{
			Description: "The query to search for",
			Type:        graphql.String,
		},
		"properties": &graphql.InputObjectFieldConfig{
			Description: "The properties to search in",
			Type:        graphql.NewList(graphql.String),
		},
	}
}
