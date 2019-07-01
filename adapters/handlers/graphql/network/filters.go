/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

// Package network provides the network graphql endpoint for Weaviate
package network

import (
	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	network_introspect "github.com/semi-technologies/weaviate/adapters/handlers/graphql/network/introspect"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/utils"
)

func genNetworkWhereOperatorEnum() *graphql.Enum {
	enumConf := graphql.EnumConfig{
		Name: "NetworkWhereOperatorEnum",
		Values: graphql.EnumValueConfigMap{
			"And":              &graphql.EnumValueConfig{},
			"Or":               &graphql.EnumValueConfig{},
			"Equal":            &graphql.EnumValueConfig{},
			"Not":              &graphql.EnumValueConfig{},
			"NotEqual":         &graphql.EnumValueConfig{},
			"GreaterThan":      &graphql.EnumValueConfig{},
			"GreaterThanEqual": &graphql.EnumValueConfig{},
			"LessThan":         &graphql.EnumValueConfig{},
			"LessThanEqual":    &graphql.EnumValueConfig{},
		},
		Description: descriptions.WhereOperatorEnum,
	}

	return graphql.NewEnum(enumConf)
}

// This is a translation of the Prototype from JS to Go. In the prototype some filter elements are declared as global variables, this is recreated here.
func genGlobalNetworkFilterElements(filterContainer *utils.FilterContainer) {
	filterContainer.WeaviateNetworkWhereKeywordsInpObj = genWeaviateNetworkWhereNameKeywordsInpObj()
	filterContainer.WeaviateNetworkIntrospectPropertiesObjField = network_introspect.GenWeaviateNetworkIntrospectPropertiesObjField()
}

func genWeaviateNetworkWhereNameKeywordsInpObj() *graphql.InputObject {
	weaviateNetworkWhereNameKeywordsInpObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: "WeaviateNetworkWhereNameKeywordsInpObj",
			Fields: graphql.InputObjectConfigFieldMap{
				"value": &graphql.InputObjectFieldConfig{
					Type:        graphql.String,
					Description: descriptions.WhereKeywordsValue,
				},
				"weight": &graphql.InputObjectFieldConfig{
					Type:        graphql.Float,
					Description: descriptions.WhereKeywordsWeight,
				},
			},
			Description: descriptions.WhereKeywordsInpObj,
		},
	)
	return weaviateNetworkWhereNameKeywordsInpObj
}
