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
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (s *Shard) objectByID(ctx context.Context, id strfmt.UUID, props traverser.SelectProperties, meta bool) (*storobj.Object, error) {
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

func (s *Shard) multiObjectByID(ctx context.Context, query []multi.Identifier) ([]*storobj.Object, error) {
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

func (s *Shard) vectorByIndexID(ctx context.Context, indexID int32) ([]float32, error) {
	keyBuf := bytes.NewBuffer(make([]byte, 4))
	binary.Write(keyBuf, binary.LittleEndian, &indexID)
	key := keyBuf.Bytes()

	var vec []float32
	err := s.db.View(func(tx *bolt.Tx) error {
		uuid := tx.Bucket(helpers.IndexIDBucket).Get(key)
		if uuid == nil {
			return fmt.Errorf("index id %d resolved to a nil object-id", indexID)
		}

		bytes := tx.Bucket(helpers.ObjectsBucket).Get(uuid)
		if bytes == nil {
			return fmt.Errorf("uuid resolved to a nil object")
		}

		obj, err := storobj.FromBinary(bytes)
		if err != nil {
			return errors.Wrap(err, "unmarshal kind object")
		}

		vec = obj.Vector
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "bolt view tx")
	}

	return vec, nil
}

func (s *Shard) objectSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	meta bool) ([]*storobj.Object, error) {

	if filters == nil {
		return s.objectList(ctx, limit, meta)
	}

	return inverted.NewSearcher(s.db, s.index.getSchema.GetSchemaSkipAuth()).
		Object(ctx, limit, filters, meta, s.index.Config.ClassName)
}

func (s *Shard) objectVectorSearch(ctx context.Context, searchVector []float32, limit int,
	filters *filters.LocalFilter, meta bool) ([]*storobj.Object, error) {
	// defer func() {
	// 	err := recover()
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		debug.PrintStack()
	// 	}
	// }()

	var allowList inverted.AllowList

	// beforeAllow := time.Now()
	if filters != nil {
		list, err := inverted.NewSearcher(s.db, s.index.getSchema.GetSchemaSkipAuth()).
			DocIDs(ctx, filters, meta, s.index.Config.ClassName)
		if err != nil {
			return nil, errors.Wrap(err, "build inverted filter allow list")
		}

		allowList = list
	}
	// fmt.Printf("building allow list took %s\n", time.Since(beforeAllow))

	// beforeVector := time.Now()
	ids, err := s.vectorIndex.SearchByVector(searchVector, limit, allowList)
	if err != nil {
		return nil, errors.Wrap(err, "vector search")
	}
	// fmt.Printf("vector search took %s\n", time.Since(beforeVector))

	if ids == nil || len(ids) == 0 {
		return nil, nil
	}

	var out []*storobj.Object

	// beforeBolt := time.Now()
	if err := s.db.View(func(tx *bolt.Tx) error {
		uuidKeys := make([][]byte, len(ids))
		b := tx.Bucket(helpers.IndexIDBucket)
		if b == nil {
			return fmt.Errorf("index id bucket not found")
		}

		for i, pointer := range ids {
			keyBuf := bytes.NewBuffer(make([]byte, 4))
			pointerUint32 := uint32(pointer)
			binary.Write(keyBuf, binary.LittleEndian, &pointerUint32)
			key := keyBuf.Bytes()
			uuidKeys[i] = b.Get(key)
		}

		out = make([]*storobj.Object, len(uuidKeys))
		b = tx.Bucket(helpers.ObjectsBucket)
		if b == nil {
			return fmt.Errorf("index id bucket not found")
		}
		for i, uuid := range uuidKeys {
			elem, err := storobj.FromBinary(b.Get(uuid))
			if err != nil {
				return errors.Wrap(err, "unmarshal data object")
			}

			out[i] = elem
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "docID to []*storobj.Object after vector search")
	}
	// fmt.Printf("get object from bolt took %s\n", time.Since(beforeBolt))

	return out, nil
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
