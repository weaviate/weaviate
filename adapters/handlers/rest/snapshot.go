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

package rest

import (
	"github.com/semi-technologies/weaviate/adapters/repos/db"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/backup"
)

func newSnapshotterProvider(db *db.DB) backup.SnapshotterProvider {
	return &snapshotterProvider{db}
}

type snapshotterProvider struct {
	db *db.DB
}

func (sp *snapshotterProvider) Snapshotter(className string) backup.Snapshotter {
	if idx := sp.db.GetIndex(schema.ClassName(className)); idx != nil {
		return idx
	}
	return nil
}
