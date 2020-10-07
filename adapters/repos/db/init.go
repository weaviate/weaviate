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
	"os"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// On init we get the current schema and create one index object per class.
// They will in turn create shards which will either read an existing db file
// from disk or create a new one if none exists
func (d *DB) init() error {
	if err := os.MkdirAll(d.config.RootPath, 0777); err != nil {
		return errors.Wrapf(err, "create root path directory at %s", d.config.RootPath)
	}

	things := d.schemaGetter.GetSchemaSkipAuth().Things
	if things != nil {
		for _, class := range things.Classes {
			idx, err := NewIndex(IndexConfig{
				Kind:      kind.Thing,
				ClassName: schema.ClassName(class.Class),
				RootPath:  d.config.RootPath,
			}, d.schemaGetter, d.logger)

			if err != nil {
				return errors.Wrap(err, "create index")
			}

			d.indices[idx.ID()] = idx
		}
	}

	actions := d.schemaGetter.GetSchemaSkipAuth().Actions
	if actions != nil {
		for _, class := range actions.Classes {
			idx, err := NewIndex(IndexConfig{
				Kind:      kind.Action,
				ClassName: schema.ClassName(class.Class),
				RootPath:  d.config.RootPath,
			}, d.schemaGetter, d.logger)

			if err != nil {
				return errors.Wrap(err, "create index")
			}

			d.indices[idx.ID()] = idx
		}
	}
	return nil
}
