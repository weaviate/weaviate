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

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

func (db *DB) BatchPutThings(ctx context.Context, things kinds.BatchThings) (kinds.BatchThings, error) {
	byIndex := map[string][]*storobj.Object{}
	for _, item := range things {
		for _, index := range db.indices {
			if index.Config.Kind != kind.Thing || index.Config.ClassName != schema.ClassName(item.Thing.Class) {
				continue
			}

			queue := byIndex[index.ID()]
			queue = append(queue, storobj.FromThing(item.Thing, item.Vector))
			byIndex[index.ID()] = queue
		}
	}

	for indexID, queries := range byIndex {
		err := db.indices[indexID].putObjectBatch(ctx, queries)
		if err != nil {
			return nil, errors.Wrapf(err, "index %q", indexID)
		}
	}

	return things, nil
}
func (db *DB) BatchPutActions(ctx context.Context, actions kinds.BatchActions) (kinds.BatchActions, error) {
	byIndex := map[string][]*storobj.Object{}
	for _, item := range actions {
		for _, index := range db.indices {
			if index.Config.Kind != kind.Action || index.Config.ClassName != schema.ClassName(item.Action.Class) {
				continue
			}

			queue := byIndex[index.ID()]
			queue = append(queue, storobj.FromAction(item.Action, item.Vector))
			byIndex[index.ID()] = queue
		}
	}

	for indexID, queries := range byIndex {
		err := db.indices[indexID].putObjectBatch(ctx, queries)
		if err != nil {
			return nil, errors.Wrapf(err, "index %q", indexID)
		}
	}

	return actions, nil
}

func (db *DB) AddBatchReferences(ctx context.Context, references kinds.BatchReferences) (kinds.BatchReferences, error) {
	return nil, nil
}
