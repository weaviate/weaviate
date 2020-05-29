package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (s *Shard) objectByID(ctx context.Context, id strfmt.UUID, props traverser.SelectProperties, meta bool) (*storobj.Object, error) {
	var object storobj.Object

	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return nil, err
	}

	err = s.db.View(func(tx *bolt.Tx) error {
		bytes := tx.Bucket(ObjectsBucket).Get(idBytes)
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

	return s.objectFilterSearch(ctx, limit, filters, meta)

}

func (s *Shard) objectList(ctx context.Context, limit int, meta bool) ([]*storobj.Object, error) {
	out := make([]*storobj.Object, limit)
	i := 0
	err := s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(ObjectsBucket).Cursor()

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

func (s *Shard) objectFilterSearch(ctx context.Context, limit int, filter *filters.LocalFilter,
	meta bool) ([]*storobj.Object, error) {

	if filter.Root.Operands != nil {
		return nil, fmt.Errorf("nested filteres not supported yet")
	}

	if filter.Root.Operator != filters.OperatorEqual {
		return nil, fmt.Errorf("filters other than equal not supported yet")
	}

	if filter.Root.Value.Type != schema.DataTypeText {
		return nil, fmt.Errorf("non text filters not supported yet")
	}

	value, ok := filter.Root.Value.Value.(string)
	if !ok {
		return nil, fmt.Errorf("expected value to be string, got %T", filter.Root.Value.Value)
	}

	props := filter.Root.On.Slice()
	if len(props) != 1 {
		return nil, fmt.Errorf("ref-filters not supported yet")
	}

	var out []*storobj.Object
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFromPropName(props[0]))
		if b == nil {
			return fmt.Errorf("bucket for prop %s not found - is it indexed?", props[0])
		}

		pointers, err := s.parseInvertedIndexRow(b.Get([]byte(value)), limit)
		if err != nil {
			return errors.Wrap(err, "parse inverted index row")
		}

		uuidKeys := make([][]byte, len(pointers.docIDs))
		b = tx.Bucket(IndexIDBucket)
		if b == nil {
			return fmt.Errorf("index id bucket not found")
		}

		for i, pointer := range pointers.docIDs {
			keyBuf := bytes.NewBuffer(make([]byte, 4))
			binary.Write(keyBuf, binary.LittleEndian, &pointer.id)
			key := keyBuf.Bytes()
			uuidKeys[i] = b.Get(key)
		}

		out = make([]*storobj.Object, len(uuidKeys))
		b = tx.Bucket(ObjectsBucket)
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
		return nil, errors.Wrap(err, "object filter search bolt view tx")
	}

	return out, nil
}

type docPointers struct {
	count  uint32
	docIDs []docPointer
}

type docPointer struct {
	id        uint32
	frequency float32
}

func (s Shard) parseInvertedIndexRow(in []byte, limit int) (docPointers, error) {
	out := docPointers{}
	if len(in) == 0 {
		return out, nil
	}

	var count uint32
	r := bytes.NewReader(in)

	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return out, errors.Wrap(err, "read doc count")
	}

	read := 0
	for {
		if read >= limit {
			// we are done because the user specified limit is reached
			break
		}

		var docID uint32
		if err := binary.Read(r, binary.LittleEndian, &docID); err != nil {
			if err == io.EOF {
				// we are done, because all entries are read
				break
			}

			return out, errors.Wrap(err, "read doc id")
		}

		var frequency float32
		if err := binary.Read(r, binary.LittleEndian, &frequency); err != nil {
			// EOF would be unexpected here, so any error including EOF is an error
			return out, errors.Wrap(err, "read doc frequency")
		}

		out.docIDs = append(out.docIDs, docPointer{id: docID, frequency: frequency})
		read++
	}

	return out, nil
}
