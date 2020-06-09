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
	prop         string
	value        []byte
	operator     filters.Operator
	hasFrequency bool
}

func (f *FilterSearcher) Object(ctx context.Context, limit int, filter *filters.LocalFilter,
	meta bool) ([]*storobj.Object, error) {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		fmt.Printf("%v at %s", r, debug.Stack())
	// 	}
	// }()

	pv, err := f.extractPropValuePairs(filter)
	if err != nil {
		return nil, err
	}

	// TODO: support more than one pv pair

	var out []*storobj.Object
	if err := f.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.BucketFromPropName(pv[0].prop))
		if b == nil {
			return fmt.Errorf("bucket for prop %s not found - is it indexed?", pv[0].prop)
		}

		pointers, err := f.docPointers(pv[0].operator, b, pv[0].value, limit, pv[0].hasFrequency)
		if err != nil {
			return err
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

func (fs *FilterSearcher) parseInvertedIndexRow(in []byte, limit int, hasFrequency bool) (docPointers, error) {
	out := docPointers{}
	if len(in) == 0 {
		return out, nil
	}

	r := bytes.NewReader(in)

	if err := binary.Read(r, binary.LittleEndian, &out.count); err != nil {
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

		if hasFrequency {
			if err := binary.Read(r, binary.LittleEndian, &frequency); err != nil {
				// EOF would be unexpected here, so any error including EOF is an error
				return out, errors.Wrap(err, "read doc frequency")
			}
		}

		out.docIDs = append(out.docIDs, docPointer{id: docID, frequency: &frequency})
		read++
	}

	return out, nil
}

func (fs *FilterSearcher) extractPropValuePairs(filter *filters.LocalFilter) ([]propValuePair, error) {
	if filter.Root.Operands != nil {
		return nil, fmt.Errorf("nested filteres not supported yet")
	}

	var extractValueFn func(in interface{}) ([]byte, error)
	var hasFrequency bool
	switch filter.Root.Value.Type {
	case schema.DataTypeText:
		extractValueFn = fs.extractTextValue
		hasFrequency = true
	case schema.DataTypeString:
		extractValueFn = fs.extractStringValue
		hasFrequency = true
	case schema.DataTypeBoolean:
		extractValueFn = fs.extractBoolValue
		hasFrequency = false
	case schema.DataTypeInt:
		extractValueFn = fs.extractIntValue
		hasFrequency = false
	case schema.DataTypeNumber:
		extractValueFn = fs.extractNumberValue
		hasFrequency = false
	default:
		return nil, fmt.Errorf("data type not supported yet")
	}

	byteValue, err := extractValueFn(filter.Root.Value.Value)
	if err != nil {
		return nil, err
	}

	props := filter.Root.On.Slice()
	if len(props) != 1 {
		return nil, fmt.Errorf("ref-filters not supported yet")
	}

	// TODO: support more than one
	return []propValuePair{
		propValuePair{
			prop:         props[0],
			value:        byteValue,
			operator:     filter.Root.Operator,
			hasFrequency: hasFrequency,
		},
	}, nil
}

type docPointers struct {
	count  uint32
	docIDs []docPointer
}

type docPointer struct {
	id        uint32
	frequency *float32
}
