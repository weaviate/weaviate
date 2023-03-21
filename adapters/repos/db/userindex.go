//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/userindex"
)

func (db *DB) UserIndexStatus(ctx context.Context,
	className string,
) ([]userindex.Index, error) {
	ind := db.GetIndex(schema.ClassName(className))
	if ind == nil {
		return nil, fmt.Errorf("class %s not found", className)
	}

	out := ind.userIndexStatus.List()

	return out, nil
}
