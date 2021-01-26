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
)

// On init we get the current schema and create one index object per class.
// They will in turn create shards which will either read an existing db file
// from disk or create a new one if none exists
func (d *DB) init() error {
	if err := os.MkdirAll(d.config.RootPath, 0o777); err != nil {
		return errors.Wrapf(err, "create root path directory at %s", d.config.RootPath)
	}

	objects := d.schemaGetter.GetSchemaSkipAuth().Objects
	if objects != nil {
		for _, class := range objects.Classes {

			idx, err := NewIndex(IndexConfig{
				ClassName: schema.ClassName(class.Class),
				RootPath:  d.config.RootPath,
			}, class.VectorIndexConfig.(schema.VectorIndexConfig), d.schemaGetter,
				d, d.logger)
			if err != nil {
				return errors.Wrap(err, "create index")
			}

			d.indices[idx.ID()] = idx
		}
	}

	return nil
}
