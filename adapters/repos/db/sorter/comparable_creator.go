//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package sorter

import "github.com/weaviate/weaviate/entities/storobj"

type comparable struct {
	docID uint64
	// all property values that will be used for comparison
	// most important 1st, least important last
	values []interface{}
	// additional payload to hold data related to object, not used in sorting process
	payload interface{}
}

type comparableCreator struct {
	extractor *comparableValueExtractor
	propNames []string
}

func newComparableCreator(extractor *comparableValueExtractor, propNames []string) *comparableCreator {
	return &comparableCreator{extractor, propNames}
}

func (c *comparableCreator) createFromBytes(docID uint64, objData []byte) *comparable {
	return c.createFromBytesWithPayload(docID, objData, nil)
}

func (c *comparableCreator) createFromBytesWithPayload(docID uint64, objData []byte, payload interface{}) *comparable {
	values := make([]interface{}, len(c.propNames))
	for level, propName := range c.propNames {
		values[level] = c.extractor.extractFromBytes(objData, propName)
	}
	return &comparable{docID, values, payload}
}

// func (c *comparableCreator) createFromObject(object *storobj.Object) *comparable {
// 	return c.createFromObjectWithPayload(object, nil)
// }

func (c *comparableCreator) createFromObjectWithPayload(object *storobj.Object, payload interface{}) *comparable {
	values := make([]interface{}, len(c.propNames))
	for level, propName := range c.propNames {
		values[level] = c.extractor.extractFromObject(object, propName)
	}
	return &comparable{object.DocID(), values, payload}
}

func (c *comparableCreator) extractDocIDs(comparables []*comparable) []uint64 {
	docIDs := make([]uint64, len(comparables))
	for i, comparable := range comparables {
		docIDs[i] = comparable.docID
	}
	return docIDs
}

func (c *comparableCreator) extractPayloads(comparables []*comparable,
	consume func(i int, docID uint64, payload interface{}) (stop bool),
) {
	for i, comparable := range comparables {
		if consume(i, comparable.docID, comparable.payload) {
			break
		}
	}
}
