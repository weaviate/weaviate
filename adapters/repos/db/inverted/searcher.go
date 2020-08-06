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
	db     *bolt.DB
	schema schema.Schema
}

func NewSearcher(db *bolt.DB, schema schema.Schema) *Searcher {
	return &Searcher{
		db:     db,
		schema: schema,
	}
}

// Object returns a list of full objects
func (f *Searcher) Object(ctx context.Context, limit int, filter *filters.LocalFilter,
	meta bool, className schema.ClassName) ([]*storobj.Object, error) {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		fmt.Printf("%v at %s", r, debug.Stack())
	// 	}
	// }()

	pv, err := f.extractPropValuePair(filter.Root, className)
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

// DocIDs is similar to Objects, but does not actually resolve the docIDs to
// full objects. Instead it returns the pure object id pointers. They can then
// be used in a secondary index (e.g. vector index)
//
// DocID queries does not contain a limit by design, as we won't know if the limit
// wouldn't remove the item that is most important for the follow up query.
// Imagine the user sets the limit to 1 and the follow-up is a vector search.
// If we already limited the allowList to 1, the vector search would be
// pointless, as only the first element would be allowed, regardless of which
// had the shortest distance
func (f *Searcher) DocIDs(ctx context.Context, filter *filters.LocalFilter,
	meta bool, className schema.ClassName) (AllowList, error) {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		fmt.Printf("%v at %s", r, debug.Stack())
	// 	}
	// }()

	pv, err := f.extractPropValuePair(filter.Root, className)
	if err != nil {
		return nil, err
	}

	if err := f.db.View(func(tx *bolt.Tx) error {
		if err := pv.fetchDocIDs(tx, f, -1); err != nil {
			return errors.Wrap(err, "fetch doc ids for prop/value pair")
		}

		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "doc id filter search bolt view tx")
	}

	pointers, err := pv.mergeDocIDs()
	if err != nil {
		return nil, errors.Wrap(err, "merge doc ids by operator")
	}

	out := make(AllowList, len(pointers.docIDs))
	for _, p := range pointers.docIDs {
		out.Insert(p.id)
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
		if limit > 0 && read >= limit { // limit >0 allows us to specify -1 to mean unlimited
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

func (fs *Searcher) extractPropValuePair(filter *filters.Clause, className schema.ClassName) (*propValuePair, error) {
	var out propValuePair
	if filter.Operands != nil {
		// nested filter
		out.children = make([]*propValuePair, len(filter.Operands))

		for i, clause := range filter.Operands {
			child, err := fs.extractPropValuePair(&clause, className)
			if err != nil {
				return nil, errors.Wrapf(err, "nested clause at pos %d", i)
			}
			out.children[i] = child
		}
		out.operator = filter.Operator
		return &out, nil
	}

	// on value or non-nested filter
	props := filter.On.Slice()
	if len(props) != 1 {
		return nil, fmt.Errorf("ref-filters not supported yet")
	}

	// we are on a value element
	if fs.onRefProp(className, props[0]) && filter.Value.Type == schema.DataTypeInt {
		// ref prop and int type is a special case, the user is looking for the
		// reference count as opposed to the content
		return fs.extractReferenceCount(props[0], filter.Value.Value, filter.Operator)
	}
	return fs.extractPrimitiveProp(props[0], filter.Value.Type, filter.Value.Value, filter.Operator)
}

func (fs *Searcher) extractPrimitiveProp(propName string, dt schema.DataType, value interface{},
	operator filters.Operator) (*propValuePair, error) {
	var extractValueFn func(in interface{}) ([]byte, error)
	var hasFrequency bool
	switch dt {
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
	case "":
		return nil, fmt.Errorf("data type cannot be empty")
	default:
		return nil, fmt.Errorf("data type %q not supported yet", dt)
	}

	byteValue, err := extractValueFn(value)
	if err != nil {
		return nil, err
	}

	return &propValuePair{
		value:        byteValue,
		hasFrequency: hasFrequency,
		prop:         propName,
		operator:     operator,
	}, nil
}

func (fs *Searcher) extractReferenceCount(propName string, value interface{},
	operator filters.Operator) (*propValuePair, error) {
	byteValue, err := fs.extractIntCountValue(value)
	if err != nil {
		return nil, err
	}

	return &propValuePair{
		value:        byteValue,
		hasFrequency: false,
		prop:         helpers.MetaCountProp(propName),
		operator:     operator,
	}, nil
}

func (fs *Searcher) onRefProp(className schema.ClassName, propName string) bool {
	c := fs.schema.FindClassByName(className)
	if c == nil {
		return false
	}

	for _, prop := range c.Properties {
		if prop.Name != propName {
			continue
		}

		if schema.IsRefDataType(prop.DataType) {
			return true
		}
	}

	return false
}

type docPointers struct {
	count  uint32
	docIDs []docPointer
}

type docPointer struct {
	id        uint32
	frequency *float32
}
