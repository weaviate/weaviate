package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
)

type docIDWithFrequency struct {
	DocID     uint32
	Frequency float32
}

type batchProp struct {
	Items        map[string]batchPropItem
	HasFrequency bool
	Name         string
}

type batchPropItem struct {
	Data []byte
	Docs []docIDWithFrequency
}

func (s *Shard) spikeBatchedInverted(objects []*storobj.Object,
	duplicates map[int]struct{}, statuses map[strfmt.UUID]objectInsertStatus,
	errs map[int]error) error {

	combined := map[string]batchProp{}

	for i, object := range objects {
		if _, ok := duplicates[i]; ok {
			continue
		}

		if err := errs[i]; err != nil {
			continue
		}

		nextInvertProps, err := s.analyzeObject(object)
		if err != nil {
			return errors.Wrap(err, "analyze next object")
		}

		for _, prop := range nextInvertProps {
			combinedProp, ok := combined[prop.Name]
			if !ok {
				combinedProp = batchProp{
					Name:         prop.Name,
					HasFrequency: prop.HasFrequency,
					Items:        map[string]batchPropItem{},
				}
			}

			for _, item := range prop.Items {
				combinedPropItem, ok := combinedProp.Items[string(item.Data)]
				if !ok {
					combinedPropItem = batchPropItem{
						Data: item.Data,
					}
				}

				combinedPropItem.Docs = append(combinedPropItem.Docs, docIDWithFrequency{
					DocID:     statuses[object.ID()].docID,
					Frequency: item.TermFrequency,
				})

				combinedProp.Items[string(item.Data)] = combinedPropItem
			}

			combined[prop.Name] = combinedProp
		}
	}

	return s.batchExtendInvertedIndices(combined)
}

func (s *Shard) batchExtendInvertedIndices(props map[string]batchProp) error {
	for _, prop := range props {

		// TODO: try concurrent
		before := time.Now()
		if err := s.db.Batch(func(tx *bolt.Tx) error {
			b := tx.Bucket(helpers.BucketFromPropName(prop.Name))
			if b == nil {
				return fmt.Errorf("no bucket for prop '%s' found", prop.Name)
			}

			if prop.HasFrequency {
				for _, item := range prop.Items {
					if err := s.batchExtendInvertedIndexItemWithFrequency(b, item,
						item.Docs); err != nil {
						return errors.Wrapf(err, "extend index with item '%s'",
							string(item.Data))
					}
				}
			} else {
				for _, item := range prop.Items {
					if err := s.batchExtendInvertedIndexItemWithoutFrequency(b, item,
						item.Docs); err != nil {
						return errors.Wrapf(err, "extend index with item '%s'",
							string(item.Data))
					}

				}
			}

			return nil
		}); err != nil {
			return errors.Wrap(err, "batch invert props")
		}
		fmt.Printf("prop %s took %s\n", prop.Name, time.Since(before))
	}

	return nil
}

func (s *Shard) batchExtendInvertedIndexItemWithoutFrequency(b *bolt.Bucket, item batchPropItem,
	docs []docIDWithFrequency) error {

	return s.batchExtendInvertedIndexItemWithOptionalFrequency(b, item, docs, false)
}

func (s *Shard) batchExtendInvertedIndexItemWithFrequency(b *bolt.Bucket,
	item batchPropItem, docs []docIDWithFrequency) error {

	return s.batchExtendInvertedIndexItemWithOptionalFrequency(b, item, docs, true)
}

func (s *Shard) batchExtendInvertedIndexItemWithOptionalFrequency(b *bolt.Bucket,
	item batchPropItem, docs []docIDWithFrequency, hasFrequency bool) error {
	data := b.Get(item.Data)

	updated := bytes.NewBuffer(data)
	if len(data) == 0 {
		// this is the first time someones writing this row, initalize counter in
		// beginning as zero, and a dummy checksum
		updated.Write([]uint8{0, 0, 0, 0}) // dummy checksum
		docCount := uint32(0)
		binary.Write(updated, binary.LittleEndian, &docCount)
	}

	for _, doc := range docs {
		if err := binary.Write(updated, binary.LittleEndian, &doc.DocID); err != nil {
			return errors.Wrap(err, "write doc id")
		}
		if hasFrequency {
			if err := binary.Write(updated, binary.LittleEndian, &doc.Frequency); err != nil {
				return errors.Wrap(err, "write doc frequency")
			}
		}
	}

	extended := updated.Bytes()

	// read and increase doc count
	reader := bytes.NewReader(extended[4:])
	var docCount uint32
	binary.Read(reader, binary.LittleEndian, &docCount)
	docCount++
	countBuf := bytes.NewBuffer(make([]byte, 0, 4))
	binary.Write(countBuf, binary.LittleEndian, &docCount)

	// overwrite old doc count

	startPos := 4 // first 4 bytes are checksum, so 4-7 is count
	countBytes := countBuf.Bytes()
	for i := 0; i < 4; i++ {
		extended[startPos+i] = countBytes[i]
	}

	// finally calculate the checksum and prepend one more time.
	chksum, err := s.checksum(extended[4:])
	if err != nil {
		return err
	}

	// overwrite first four bytes with checksum
	startPos = 0 // first 4 bytes are checksum
	for i := 0; i < 4; i++ {
		extended[startPos+i] = chksum[i]
	}

	lengthOfOneEntry := 4
	if hasFrequency {
		lengthOfOneEntry = 8
	}
	if len(extended) != 0 && len(extended) > 8 && (len(extended)-8)%lengthOfOneEntry != 0 {
		// -8 to remove the checksum and doc count
		// module 8 for 4 bytes of docID + frequency or alternatively 4 without the
		// frequency
		return fmt.Errorf("sanity check: invert row has invalid updated length %d"+
			"with original length %d", len(extended), len(data))
	}

	err = b.Put(item.Data, extended)
	if err != nil {
		return err
	}

	return nil
}
