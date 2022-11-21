//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package get

import (
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/utils"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/sirupsen/logrus"
	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
)

type ModulesProvider interface {
	GetArguments(class *models.Class) map[string]*graphql.ArgumentConfig
	ExtractSearchParams(arguments map[string]interface{}, className string) map[string]interface{}
	GetAdditionalFields(class *models.Class) map[string]*graphql.Field
	ExtractAdditionalField(className, name string, params []*ast.Argument) interface{}
	GraphQLAdditionalFieldNames() []string
}

// Build the Local.Get part of the graphql tree
func Build(schema *schema.Schema, logger logrus.FieldLogger,
	modulesProvider ModulesProvider,
) (*graphql.Field, error) {
	if len(schema.Objects.Classes) == 0 {
		return nil, utils.ErrEmptySchema
	}

	cb := newClassBuilder(schema, logger, modulesProvider)

	var err error
	var objects *graphql.Object
	if len(schema.Objects.Classes) > 0 {
		objects, err = cb.objects()
		if err != nil {
			return nil, err
		}
	}

	return &graphql.Field{
		Name:        "Get",
		Description: descriptions.GetObjects,
		Type:        objects,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			// Does nothing; pass through the filters
			return p.Source, nil
		},
	}, nil
}
