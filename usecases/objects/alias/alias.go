//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package alias

import "github.com/weaviate/weaviate/entities/models"

type schemaManager interface {
	ResolveAlias(alias string) string
}

func ResolveAlias(schemaManager schemaManager, aliasName string) (string, string) {
	if cls := schemaManager.ResolveAlias(aliasName); cls != "" {
		return cls, aliasName
	}
	return aliasName, ""
}

func ClassNameToAlias(obj *models.Object, alias string) *models.Object {
	if obj != nil {
		obj.Class = alias
	}
	return obj
}

func ClassNamesToAliases(objs []*models.Object, alias string) []*models.Object {
	for i := range objs {
		objs[i].Class = alias
	}
	return objs
}
