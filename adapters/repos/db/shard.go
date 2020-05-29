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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/indexcounter"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// Shard is the smallest completely-contained index unit. A shard mananages
// database files for all the objects it owns. How a shard is determined for a
// target object (e.g. Murmur hash, etc.) is still open at this point
type Shard struct {
	index   *Index // a reference to the underlying index, which in turn contains schema information
	name    string
	db      *bolt.DB // one db file per shard, uses buckets for separation between data storage, index storage, etc.
	counter *indexcounter.Counter
}

var (
	ObjectsBucket []byte = []byte("objects")
	IndexIDBucket []byte = []byte("index_ids")
)

func NewShard(shardName string, index *Index) (*Shard, error) {
	s := &Shard{
		index: index,
		name:  shardName,
	}

	err := s.initDBFile()
	if err != nil {
		return nil, errors.Wrapf(err, "init shard %s", s.ID())
	}

	counter, err := indexcounter.New(s.ID(), index.Config.RootPath)
	if err != nil {
		return nil, errors.Wrapf(err, "init shard index counter %s", s.ID())
	}

	s.counter = counter
	return s, nil
}

func (s *Shard) ID() string {
	return fmt.Sprintf("%s_%s", s.index.ID(), s.name)
}

func (s *Shard) DBPath() string {
	return fmt.Sprintf("%s/%s.db", s.index.Config.RootPath, s.ID())
}

func (s *Shard) initDBFile() error {
	boltdb, err := bolt.Open(s.DBPath(), 0600, nil)
	if err != nil {
		return errors.Wrapf(err, "open bolt at %s", s.DBPath())
	}

	err = boltdb.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(ObjectsBucket); err != nil {
			return errors.Wrapf(err, "create objects bucket '%s'", string(ObjectsBucket))
		}

		if _, err := tx.CreateBucketIfNotExists(IndexIDBucket); err != nil {
			return errors.Wrapf(err, "create indexID bucket '%s'", string(IndexIDBucket))
		}

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "create bolt buckets")
	}

	s.db = boltdb
	return nil
}

func (s *Shard) addProperty(ctx context.Context, prop *models.Property) error {
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketFromPropName(prop.Name))
		return err
	}); err != nil {
		return errors.Wrap(err, "bolt update tx")
	}

	return nil
}

func bucketFromPropName(propName string) []byte {
	return []byte(fmt.Sprintf("property_%s", propName))
}

func (s *Shard) putObject(ctx context.Context, object *storobj.Object) error {
	idBytes, err := uuid.MustParse(object.ID().String()).MarshalBinary()
	if err != nil {
		return err
	}

	docID, err := s.counter.GetAndInc()
	if err != nil {
		return errors.Wrap(err, "get new doc id from counter")
	}
	object.SetIndexID(docID)

	data, err := object.MarshalBinary()
	if err != nil {
		return errors.Wrapf(err, "marshal object %s to binary", object.ID())
	}

	invertProps, err := s.analyzeObject(object)
	if err != nil {
		return err
	}

	if err := s.db.Batch(func(tx *bolt.Tx) error {
		// insert data object
		if err := tx.Bucket(ObjectsBucket).Put([]byte(idBytes), data); err != nil {
			return errors.Wrap(err, "put object data")
		}

		// build indexID->UUID lookup
		if err := s.addIndexIDLookup(tx, idBytes, docID); err != nil {
			return errors.Wrap(err, "put inverted indices props")
		}

		// insert inverted index props
		if err := s.extendInvertedIndices(tx, invertProps, docID); err != nil {
			return errors.Wrap(err, "put inverted indices props")
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "bolt batch tx")
	}

	return nil
}

func (s *Shard) analyzeObject(object *storobj.Object) ([]inverted.Property, error) {
	if object.Schema() == nil {
		return nil, nil
	}

	var schemaModel *models.Schema
	if object.Kind == kind.Thing {
		schemaModel = s.index.getSchema.GetSchemaSkipAuth().Things
	} else {
		schemaModel = s.index.getSchema.GetSchemaSkipAuth().Actions
	}

	c, err := schema.GetClassByName(schemaModel, object.Class().String())
	if err != nil {
		return nil, err
	}

	schemaMap, ok := object.Schema().(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected schema to be map, but got %T", object.Schema())
	}

	return inverted.NewAnalyzer().Object(schemaMap, c.Properties)
}

func (s *Shard) extendInvertedIndices(tx *bolt.Tx, props []inverted.Property, docID uint32) error {
	for _, prop := range props {
		b := tx.Bucket(bucketFromPropName(prop.Name))
		if b == nil {
			return fmt.Errorf("no bucket for prop '%s' found", prop.Name)
		}

		for _, item := range prop.Items {
			if err := s.extendInvertedIndexItem(b, item, docID, item.TermFrequency); err != nil {
				return errors.Wrapf(err, "extend index with item '%s'", string(item.Data))
			}
		}
	}

	return nil
}

// extendInvertedIndexItem maintains an inverted index row for one search term,
// the structure is as follows:
//
// Bytes | Meaning
// 0..4   | count of matching documents as uint32 (little endian)
// 5..7   | doc id of first matching doc as uint32 (little endian)
// 8..11   | term frequency in first doc as float32 (little endian)
// ...
// (n-7)..(n-4) | doc id of last doc
// (n-3)..n     | term frequency of last
func (s *Shard) extendInvertedIndexItem(b *bolt.Bucket, item inverted.Countable, docID uint32, freq float32) error {
	data := b.Get(item.Data)
	updated := bytes.NewBuffer(data)
	if len(data) == 0 {
		// this is the first time someones writing this row, initalize counter in
		// beginning as zero
		docCount := uint32(0)
		binary.Write(updated, binary.LittleEndian, &docCount)
	}

	// append current document
	binary.Write(updated, binary.LittleEndian, &docID)
	binary.Write(updated, binary.LittleEndian, &freq)
	extended := updated.Bytes()

	// read and increase doc count
	reader := bytes.NewReader(extended)
	var docCount uint32
	binary.Read(reader, binary.LittleEndian, &docCount)
	docCount++
	countBytes := bytes.NewBuffer(make([]byte, 0, 4))
	binary.Write(countBytes, binary.LittleEndian, &docCount)

	// combine back together and save
	combined := append(countBytes.Bytes(), extended[4:]...)
	err := b.Put(item.Data, combined)
	if err != nil {
		return err
	}

	return nil
}

func (s *Shard) addIndexIDLookup(tx *bolt.Tx, id []byte, docID uint32) error {
	keyBuf := bytes.NewBuffer(make([]byte, 4))
	binary.Write(keyBuf, binary.LittleEndian, &docID)
	key := keyBuf.Bytes()

	b := tx.Bucket(IndexIDBucket)
	if b == nil {
		return fmt.Errorf("no index id bucket found")
	}

	if err := b.Put(key, id); err != nil {
		return errors.Wrap(err, "store uuid for index id")
	}

	return nil
}

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

// TODO: stop reading if limit is reached
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

	for {
		var docID uint32
		if err := binary.Read(r, binary.LittleEndian, &docID); err != nil {
			if err == io.EOF {
				// we are done
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
	}

	return out, nil
}
