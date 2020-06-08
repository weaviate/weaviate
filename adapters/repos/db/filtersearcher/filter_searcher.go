package filtersearcher

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
)

type FilterSearcher struct {
	db *bolt.DB
}

func New(db *bolt.DB) *FilterSearcher {
	return &FilterSearcher{
		db: db,
	}
}

type propValuePair struct {
	prop  []byte
	value []byte
}

func (f *FilterSearcher) Object(ctx context.Context, limit int, filter *filters.LocalFilter,
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
	if err := f.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.BucketFromPropName(props[0]))
		if b == nil {
			return fmt.Errorf("bucket for prop %s not found - is it indexed?", props[0])
		}

		pointers, err := f.parseInvertedIndexRow(b.Get([]byte(value)), limit)
		if err != nil {
			return errors.Wrap(err, "parse inverted index row")
		}

		uuidKeys := make([][]byte, len(pointers.docIDs))
		b = tx.Bucket(helpers.IndexIDBucket)
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
		return nil, errors.Wrap(err, "object filter search bolt view tx")
	}

	return out, nil
}

func (fs *FilterSearcher) parseInvertedIndexRow(in []byte, limit int) (docPointers, error) {
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

type docPointers struct {
	count  uint32
	docIDs []docPointer
}

type docPointer struct {
	id        uint32
	frequency float32
}
