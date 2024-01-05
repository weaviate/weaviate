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
	"fmt"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
)

func (e *Explorer) validateCursor(params dto.GetParams) error {
	if params.Cursor != nil {
		if params.Group != nil || params.HybridSearch != nil || params.KeywordRanking != nil ||
			params.NearObject != nil || params.NearVector != nil || len(params.ModuleParams) > 0 {
			return fmt.Errorf("other params cannot be set with after and limit parameters")
		}
		if err := filters.ValidateCursor(schema.ClassName(params.ClassName),
			params.Cursor, params.Pagination.Offset, params.Filters, params.Sort); err != nil {
			return err
		}
	}
	return nil
}
