//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"

	"github.com/semi-technologies/weaviate/usecases/kinds"
)

func (db *DB) BatchPutThings(ctx context.Context, things kinds.BatchThings) (kinds.BatchThings, error) {
	return nil, nil
}
func (db *DB) BatchPutActions(ctx context.Context, actions kinds.BatchActions) (kinds.BatchActions, error) {
	return nil, nil
}
func (db *DB) AddBatchReferences(ctx context.Context, references kinds.BatchReferences) (kinds.BatchReferences, error) {
	return nil, nil
}
