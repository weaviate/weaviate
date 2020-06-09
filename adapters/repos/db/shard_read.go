//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (s *Shard) objectByID(ctx context.Context, id strfmt.UUID, props traverser.SelectProperties, meta bool) (*storobj.Object, error) {
	var object storobj.Object

	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return nil, err
	}

	err = s.db.View(func(tx *bolt.Tx) error {
		bytes := tx.Bucket(helpers.ObjectsBucket).Get(idBytes)
		if bytes == nil {
			return nil
		}

		obj, err := storobj.FromBinary(bytes)
		if err != nil {
			return errors.Wrap(err, "unmarshal kind object")
		}
		object = *obj
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "bolt view tx")
	}

	return &object, nil
}

func (s *Shard) objectSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	meta bool) ([]*storobj.Object, error) {

	if filters == nil {
		return s.objectList(ctx, limit, meta)
	}

	return inverted.NewSearcher(s.db).Object(ctx, limit, filters, meta)
}

func (s *Shard) objectList(ctx context.Context, limit int, meta bool) ([]*storobj.Object, error) {
	out := make([]*storobj.Object, limit)
	i := 0
	err := s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(helpers.ObjectsBucket).Cursor()

		for k, v := cursor.First(); k != nil && i < limit; k, v = cursor.Next() {
			obj, err := storobj.FromBinary(v)
			if err != nil {
				return errors.Wrapf(err, "unmarhsal item %d", i)
			}

			out[i] = obj
			i++
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "bolt view tx")
	}

	return out[:i], nil
}
