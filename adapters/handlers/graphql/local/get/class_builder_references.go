//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package get

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func (b *classBuilder) referenceField(propertyType schema.PropertyDataType,
	property *models.Property, className string,
) *graphql.Field {
	refClasses := propertyType.Classes()
	propertyName := cases.Title(language.Und, cases.NoLower).String(property.Name)
	dataTypeClasses := []*graphql.Object{}

	for _, refClassName := range refClasses {
		// is a local ref
		refClass, ok := b.knownClasses[string(refClassName)]
		if !ok {
			panic(fmt.Sprintf("buildGetClass: unknown referenced class type for %s.%s; %s",
				className, property.Name, refClassName))
		}

		dataTypeClasses = append(dataTypeClasses, refClass)
	}

	if (len(dataTypeClasses)) == 0 {
		// this could be the case when we only have network-refs, but all network
		// refs were invalid (e.g. because the peers are gone). In this case we
		// must return (nil) early, otherwise graphql will error because it has a
		// union field with an empty list of unions.
		return nil
	}

	dataTypeClasses = append(dataTypeClasses, b.beaconClass)

	classUnion := graphql.NewUnion(graphql.UnionConfig{
		Name:        fmt.Sprintf("%s%s%s", className, propertyName, "Obj"),
		Types:       dataTypeClasses,
		ResolveType: makeResolveClassUnionType(&b.knownClasses),
		Description: property.Description,
	})

	return &graphql.Field{
		Type:        graphql.NewList(classUnion),
		Description: property.Description,
		Resolve:     makeResolveRefField(),
	}
}

func makeResolveClassUnionType(knownClasses *map[string]*graphql.Object) graphql.ResolveTypeFn {
	return func(p graphql.ResolveTypeParams) *graphql.Object {
		valueMap := p.Value.(map[string]interface{})
		refType := valueMap["__refClassType"].(string)
		switch refType {
		case "local":
			className := valueMap["__refClassName"].(string)
			classObj, ok := (*knownClasses)[className]
			if !ok {
				panic(fmt.Errorf(
					"local ref refers to class '%s', but no such kind exists in the peer network", className))
			}
			return classObj
		default:
			panic(fmt.Sprintf("unknown ref type %#v", refType))
		}
	}
}

func makeResolveRefField() graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		if p.Source.(map[string]interface{})[p.Info.FieldName] == nil {
			return nil, nil
		}

		items, ok := p.Source.(map[string]interface{})[p.Info.FieldName].([]interface{})
		if !ok {
			// could be a models.MultipleRef which would indicate that we found only
			// unresolved references, this is the case when accepts refs to types
			// ClassA and ClassB and the object only contains refs to one type (e.g.
			// ClassA). Now if the user only asks for resolving all of the other type
			// (i.e. ClassB), then all results would be returned unresolved (as
			// models.MultipleRef).

			return nil, nil
		}
		results := make([]interface{}, len(items))
		for i, item := range items {
			switch v := item.(type) {
			case search.LocalRef:
				// inject some meta data so the ResolveType can determine the type
				localRef := v.Fields
				localRef["__refClassType"] = "local"
				localRef["__refClassName"] = v.Class
				results[i] = localRef
			default:
				return nil, fmt.Errorf("unsupported type, expected search.LocalRef or NetworkRef, got %T", v)
			}
		}
		return results, nil
	}
}
