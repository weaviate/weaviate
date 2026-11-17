//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rank

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// rerankDataTypes are the property types a rerank reads. string is the text
// type deprecated in v1.19 that old classes still store.
var rerankDataTypes = []schema.DataType{schema.DataTypeText, schema.DataTypeString, schema.DataTypeBlob}

type Params struct {
	Property *string
	Query    *string
}

func (n Params) GetQuery() string {
	if n.Query != nil {
		return *n.Query
	}
	return ""
}

func (n Params) GetProperty() string {
	if n.Property != nil {
		return *n.Property
	}
	return ""
}

func (n Params) GetPropertiesToExtract() []string {
	property := n.GetProperty()
	if property != "" {
		return []string{property}
	}
	return []string{}
}

// Validate rejects a blank query, a property the class does not have, and a
// property that is neither text nor blob. The gRPC search calls it before the
// search, so such a request fails whether or not the search returns objects.
//
// The property is matched by its exact name: a rerank reads a first-level
// property, and schema.GetPropertyByName also accepts the path of a nested one.
func (n Params) Validate(class *models.Class) error {
	if strings.TrimSpace(n.GetQuery()) == "" {
		return errors.New("rerank: query is required")
	}
	property := n.GetProperty()
	for _, prop := range class.Properties {
		if prop.Name != property {
			continue
		}
		if len(prop.DataType) == 1 && slices.Contains(rerankDataTypes, schema.DataType(prop.DataType[0])) {
			return nil
		}
		return fmt.Errorf("rerank: property %q of collection %q is of type %s; a rerank reads a text or blob property",
			property, class.Class, strings.Join(prop.DataType, ", "))
	}
	return fmt.Errorf("rerank: "+schema.ErrorNoSuchProperty, property, class.Class)
}
