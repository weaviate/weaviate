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
	"bytes"
	"context"
	"encoding/binary"

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/docid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (s *Shard) objectByID(ctx context.Context, id strfmt.UUID,
	props traverser.SelectProperties, meta bool) (*storobj.Object, error) {
	var object *storobj.Object

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
		object = obj
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "bolt view tx")
	}

	return object, nil
}

func (s *Shard) multiObjectByID(ctx context.Context,
	query []multi.Identifier) ([]*storobj.Object, error) {
	objects := make([]*storobj.Object, len(query))

	ids := make([][]byte, len(query))
	for i, q := range query {
		idBytes, err := uuid.MustParse(q.ID).MarshalBinary()
		if err != nil {
			return nil, err
		}

		ids[i] = idBytes
	}

	err := s.db.View(func(tx *bolt.Tx) error {
		for i, id := range ids {
			bytes := tx.Bucket(helpers.ObjectsBucket).Get(id)
			if bytes == nil {
				continue
			}

			obj, err := storobj.FromBinary(bytes)
			if err != nil {
				return errors.Wrap(err, "unmarshal kind object")
			}
			objects[i] = obj
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "bolt view tx")
	}

	return objects, nil
}

func (s *Shard) exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	var ok bool

	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return false, err
	}

	err = s.db.View(func(tx *bolt.Tx) error {
		bytes := tx.Bucket(helpers.ObjectsBucket).Get(idBytes)
		if bytes == nil {
			return nil
		}

		ok = true
		return nil
	})
	if err != nil {
		return false, errors.Wrap(err, "bolt view tx")
	}

	return ok, nil
}

func (s *Shard) objectByIndexID(ctx context.Context,
	indexID int32) (*storobj.Object, error) {
	keyBuf := bytes.NewBuffer(make([]byte, 4))
	binary.Write(keyBuf, binary.LittleEndian, &indexID)
	key := keyBuf.Bytes()

	var out *storobj.Object
	err := s.db.View(func(tx *bolt.Tx) error {
		uuidLookup := tx.Bucket(helpers.DocIDBucket).Get(key)
		if uuidLookup == nil {
			return storobj.NewErrNotFoundf(indexID,
				"doc id inverted resolved to a nil object, i.e. no uuid found")
		}

		lookup, err := docid.LookupFromBinary(uuidLookup)
		if err != nil {
			return errors.Wrap(err, "unmarshal docID lookup")
		}

		if lookup.Deleted {
			return storobj.NewErrNotFoundf(indexID,
				"doc id is marked as deleted at %s",
				lookup.DeletionTime.String())
		}

		bytes := tx.Bucket(helpers.ObjectsBucket).Get(lookup.PointsTo)
		if bytes == nil {
			return storobj.NewErrNotFoundf(indexID,
				"uuid found for docID, but object is nil")
		}

		obj, err := storobj.FromBinary(bytes)
		if err != nil {
			return errors.Wrap(err, "unmarshal kind object")
		}

		out = obj
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "bolt view tx")
	}

	return out, nil
}

func (s *Shard) vectorByIndexID(ctx context.Context, indexID int32) ([]float32, error) {
	obj, err := s.objectByIndexID(ctx, indexID)
	if err != nil {
		return nil, err
	}

	return obj.Vector, nil
}

func (s *Shard) objectSearch(ctx context.Context, limit int,
	filters *filters.LocalFilter, meta bool) ([]*storobj.Object, error) {
	if filters == nil {
		return s.objectList(ctx, limit, meta)
	}

	return inverted.NewSearcher(s.db, s.index.getSchema.GetSchemaSkipAuth(),
		s.invertedRowCache, s.propertyIndices).
		Object(ctx, limit, filters, meta, s.index.Config.ClassName)
}

func (s *Shard) objectVectorSearch(ctx context.Context, searchVector []float32,
	limit int, filters *filters.LocalFilter, meta bool) ([]*storobj.Object, error) {
	var allowList helpers.AllowList
	if filters != nil {
		list, err := inverted.NewSearcher(s.db, s.index.getSchema.GetSchemaSkipAuth(),
			s.invertedRowCache, s.propertyIndices).
			DocIDs(ctx, filters, meta, s.index.Config.ClassName)
		if err != nil {
			return nil, errors.Wrap(err, "build inverted filter allow list")
		}

		allowList = list
	}
	ids, err := s.vectorIndex.SearchByVector(searchVector, limit, allowList)
	if err != nil {
		return nil, errors.Wrap(err, "vector search")
	}

	if len(ids) == 0 {
		return nil, nil
	}

	var out []*storobj.Object
	// TODO: unify
	idsUint := make([]uint32, len(ids))
	for i, id := range ids {
		idsUint[i] = uint32(id)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		res, err := inverted.ObjectsFromDocIDsInTx(tx, idsUint)
		if err != nil {
			return errors.Wrap(err, "resolve doc ids to objects")
		}

		out = res
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "docID to []*storobj.Object after vector search")
	}

	return out, nil
}

func (s *Shard) objectList(ctx context.Context, limit int,
	meta bool) ([]*storobj.Object, error) {
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
