package get

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
)

func groupArgument(kindName, className string) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("Get%ss%s", kindName, className)
	return &graphql.ArgumentConfig{
		// Description: descriptions.LocalGetGroup,
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sGroupInpObj", prefix),
				Fields:      groupFields(prefix),
				Description: descriptions.LocalGetWhereInpObj,
			},
		),
	}
}

func groupFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"type": &graphql.InputObjectFieldConfig{
			// Description: descriptions.Concepts,
			Type: graphql.NewEnum(graphql.EnumConfig{
				Name: fmt.Sprintf("%sGroupInpObjTypeEnum", prefix),
				Values: graphql.EnumValueConfigMap{
					"closest": &graphql.EnumValueConfig{},
					"merge":   &graphql.EnumValueConfig{},
				},
			}),
		},
		"force": &graphql.InputObjectFieldConfig{
			Description: descriptions.Force,
			Type:        graphql.Float,
		},
	}
}
