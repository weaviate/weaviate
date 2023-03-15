//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) groupResults(ctx context.Context, ids []uint64,
	dists []float32, params *searchparams.GroupBy,
	additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	objsBucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	objs, err := newGrouper(ids, dists, params, objsBucket, additional).Do(ctx)

	return objs, dists, err
}

type grouper struct {
	ids        []uint64
	dists      []float32
	params     *searchparams.GroupBy
	additional additional.Properties
	objBucket  *lsmkv.Bucket
}

func newGrouper(ids []uint64, dists []float32,
	params *searchparams.GroupBy, objBucket *lsmkv.Bucket,
	additional additional.Properties,
) *grouper {
	return &grouper{
		ids:        ids,
		dists:      dists,
		params:     params,
		objBucket:  objBucket,
		additional: additional,
	}
}

func (g *grouper) Do(ctx context.Context) ([]*storobj.Object, error) {
	docIDBytes := make([]byte, 8)

	groupsOrdered := []string{}
	groups := map[string][]uint64{}
	distances := map[string][]float32{}
	unmarshalledObjects := map[string]*storobj.Object{}
	docIDObject := map[uint64]*storobj.Object{}

DOCS_LOOP:
	for i, docID := range g.ids {
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		objData, err := g.objBucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return nil, fmt.Errorf("%w: could not get obj by doc id %d", err, docID)
		}
		if objData == nil {
			continue
		}
		value, ok, _ := storobj.ParseAndExtractProperty(objData, g.params.Property)
		if !ok {
			continue
		}

		// whole object, might be that we only need value and ID to be extracted
		unmarshalled, err := storobj.FromBinaryOptional(objData, g.additional)
		if err != nil {
			return nil, fmt.Errorf("%w: unmarshal data object at position %d", err, i)
		}
		docIDObject[docID] = unmarshalled

		for _, val := range g.getValues(value) {
			current, ok := groups[val]
			if len(current) >= g.params.ObjectsPerGroup {
				continue DOCS_LOOP
			}

			if !ok && len(groups) >= g.params.Groups {
				continue DOCS_LOOP
			}

			groups[val] = append(current, docID)
			distances[val] = append(distances[val], g.dists[i])

			if _, ok := unmarshalledObjects[val]; !ok {
				unmarshalledObjects[val] = unmarshalled
				// this group doesn't exist add it to the ordered list
				groupsOrdered = append(groupsOrdered, val)
			}
		}
	}

	objs := make([]*storobj.Object, len(groups))
	for i, val := range groupsOrdered {
		docIDs := groups[val]
		unmarshalled := unmarshalledObjects[val]
		hits := []map[string]interface{}{}
		for j, docID := range docIDs {
			props := map[string]interface{}{}
			for k, v := range docIDObject[docID].Properties().(map[string]interface{}) {
				props[k] = v
			}
			props["_additional"] = &additional.GroupHitAdditional{
				ID:       docIDObject[docID].ID().String(),
				Distance: distances[val][j],
			}
			hits = append(hits, props)
		}
		group := additional.Group{
			ID:          i,
			GroupValue:  val,
			Count:       len(hits),
			Hits:        hits,
			MaxDistance: distances[val][0],
			MinDistance: distances[val][len(distances[val])-1],
		}

		// add group
		if unmarshalled.AdditionalProperties() == nil {
			unmarshalled.Object.Additional = models.AdditionalProperties{}
		}
		unmarshalled.AdditionalProperties()["group"] = group
		objs[i] = unmarshalled
	}

	return objs, nil
}

func (g *grouper) getValues(values []string) []string {
	if len(values) == 0 {
		return []string{""}
	}
	return values
}
