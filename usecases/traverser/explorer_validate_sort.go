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

package traverser

import (
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func (e *Explorer) validateSort(className string, sort []filters.Sort) error {
	if len(sort) == 0 {
		return nil
	}
	sch := e.schemaGetter.GetSchemaSkipAuth()
	return filters.ValidateSort(sch, schema.ClassName(className), sort)
}
