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

package traverser

import (
	"github.com/weaviate/weaviate/entities/filters"
)

func (e *Explorer) validateFilters(filter *filters.LocalFilter) error {
	if filter == nil {
		return nil
	}
	sch := e.schemaGetter.GetSchemaSkipAuth()
	return filters.ValidateFilters(sch, filter)
}
