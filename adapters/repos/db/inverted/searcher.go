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

package inverted

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

type Searcher struct {
	db *bolt.DB
}

func NewSearcher(db *bolt.DB) *Searcher {
	return &Searcher{
		db: db,
	}
}

func (f *Searcher) Object(ctx context.Context, limit int, filter *filters.LocalFilter,
	meta bool) ([]*storobj.Object, error) {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		fmt.Printf("%v at %s", r, debug.Stack())
	// 	}
	// }()

	pv, err := f.extractPropValuePair(filter.Root)
	if err != nil {
		return nil, err
	}

	var out []*storobj.Object
	if err := f.db.View(func(tx *bolt.Tx) error {

		if err := pv.fetchDocIDs(tx, f, limit); err != nil {
			return errors.Wrap(err, "fetch doc ids for prop/value pair")
		}

		pointers, err := pv.mergeDocIDs()
		if err != nil {
			return errors.Wrap(err, "merge doc ids by operator")
		}

		uuidKeys := make([][]byte, len(pointers.docIDs))
		b := tx.Bucket(helpers.IndexIDBucket)
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

func (fs *Searcher) parseInvertedIndexRow(in []byte, limit int, hasFrequency bool) (docPointers, error) {
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

func (fs *Searcher) extractPropValuePair(filter *filters.Clause) (*propValuePair, error) {
	var out propValuePair
	if filter.Operands != nil {
		// nested filter
		out.children = make([]*propValuePair, len(filter.Operands))

		for i, clause := range filter.Operands {
			child, err := fs.extractPropValuePair(&clause)
			if err != nil {
				return nil, errors.Wrapf(err, "nested clause at pos %d", i)
			}
			out.children[i] = child
		}
	} else {
		// we are on a value element

		var extractValueFn func(in interface{}) ([]byte, error)
		var hasFrequency bool
		switch filter.Value.Type {
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

		byteValue, err := extractValueFn(filter.Value.Value)
		if err != nil {
			return nil, err
		}

		out.value = byteValue
		out.hasFrequency = hasFrequency

		props := filter.On.Slice()
		if len(props) != 1 {
			return nil, fmt.Errorf("ref-filters not supported yet")
		}

		out.prop = props[0]
	}

	out.operator = filter.Operator

	return &out, nil
}

type docPointers struct {
	count  uint32
	docIDs []docPointer
}

type docPointer struct {
	id        uint32
	frequency *float32
}
