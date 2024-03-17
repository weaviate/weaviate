package get

import (
	"fmt"

	"github.com/tailor-inc/graphql"
)

func efArgument(className string) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("GetObjects%s", className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sEFInpObj", prefix),
				Fields:      efFields(prefix),
				Description: "size of the ANN list",
			},
		),
	}
}

func efFields(_ string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"dynamicFactor": &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(graphql.Int),
		},
		"dynamicMin": &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(graphql.Int),
		},
		"dynamicMax": &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(graphql.Int),
		},
	}
}
