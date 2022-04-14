//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/sirupsen/logrus"
)

func (s *Shard) objectByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties,
	additional additional.Properties) (*storobj.Object, error) {
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return nil, err
	}

	bytes, err := s.store.Bucket(helpers.ObjectsBucketLSM).Get(idBytes)
	if err != nil {
		return nil, err
	}

	if bytes == nil {
		return nil, nil
	}

	obj, err := storobj.FromBinary(bytes)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal object")
	}

	return obj, nil
}

func (s *Shard) multiObjectByID(ctx context.Context,
	query []multi.Identifier) ([]*storobj.Object, error) {
	objects := make([]*storobj.Object, len(query))

	ids := make([][]byte, len(query))
	for i, q := range query {
		idBytes, err := uuid.MustParse(q.ID).MarshalBinary()
		if err != nil {
			return nil, err
		}

		ids[i] = idBytes
	}

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	for i, id := range ids {
		bytes, err := bucket.Get(id)
		if err != nil {
			return nil, err
		}

		if bytes == nil {
			continue
		}

		obj, err := storobj.FromBinary(bytes)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshal kind object")
		}
		objects[i] = obj
	}

	return objects, nil
}

// TODO: This does an actual read which is not really needed, if we see this
// come up in profiling, we could optimize this by adding an explicit Exists()
// on the LSMKV which only checks the bloom filters, which at least in the case
// of a true negative would be considerably faster. For a (false) positive,
// we'd still need to check, though.
func (s *Shard) exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return false, err
	}

	bytes, err := s.store.Bucket(helpers.ObjectsBucketLSM).Get(idBytes)
	if err != nil {
		return false, errors.Wrap(err, "read request")
	}

	if bytes == nil {
		return false, nil
	}

	return true, nil
}

func (s *Shard) objectByIndexID(ctx context.Context,
	indexID uint64, acceptDeleted bool) (*storobj.Object, error) {
	keyBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyBuf, indexID)

	bytes, err := s.store.Bucket(helpers.ObjectsBucketLSM).
		GetBySecondary(0, keyBuf)
	if err != nil {
		return nil, err
	}

	if bytes == nil {
		return nil, storobj.NewErrNotFoundf(indexID,
			"uuid found for docID, but object is nil")
	}

	obj, err := storobj.FromBinary(bytes)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal kind object")
	}

	return obj, nil
}

func (s *Shard) vectorByIndexID(ctx context.Context, indexID uint64) ([]float32, error) {
	keyBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyBuf, indexID)

	bytes, err := s.store.Bucket(helpers.ObjectsBucketLSM).
		GetBySecondary(0, keyBuf)
	if err != nil {
		return nil, err
	}

	if bytes == nil {
		return nil, storobj.NewErrNotFoundf(indexID,
			"uuid found for docID, but object is nil")
	}

	return storobj.VectorFromBinary(bytes)
}

func (s *Shard) objectSearch(ctx context.Context, limit int,
	filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking,
	sort []filters.Sort, additional additional.Properties) ([]*storobj.Object, []float32, error) {
	if keywordRanking != nil {
		if v := s.versioner.Version(); v < 2 {
			return nil, nil, errors.Errorf("shard was built with an older version of " +
				"Weaviate which does not yet support BM25 search")
		}

		bm25Config := s.index.getInvertedIndexConfig().BM25

		return inverted.NewBM25Searcher(bm25Config, s.store,
			s.index.getSchema.GetSchemaSkipAuth(), s.invertedRowCache,
			s.propertyIndices, s.index.classSearcher, s.deletedDocIDs, s.propLengths,
			s.index.logger, s.versioner.Version()).
			Object(ctx, limit, keywordRanking, filters, sort, additional, s.index.Config.ClassName)
	}

	if filters == nil {
		objs, err := s.objectList(ctx, limit, sort, additional)
		return objs, nil, err
	}
	objs, err := inverted.NewSearcher(s.store, s.index.getSchema.GetSchemaSkipAuth(),
		s.invertedRowCache, s.propertyIndices, s.index.classSearcher,
		s.deletedDocIDs, s.index.stopwords, s.versioner.Version()).
		Object(ctx, limit, filters, sort, additional, s.index.Config.ClassName)
	return objs, nil, err
}

func (s *Shard) objectVectorSearch(ctx context.Context,
	searchVector []float32, targetDist float32, limit int, filters *filters.LocalFilter,
	additional additional.Properties) ([]*storobj.Object, []float32, error) {
	var (
		ids       []uint64
		dists     []float32
		err       error
		allowList helpers.AllowList
	)

	beforeAll := time.Now()

	if filters != nil {
		list, err := s.buildAllowList(ctx, filters, additional)
		if err != nil {
			return nil, nil, err
		}
		allowList = list
	}

	if limit < 0 {
		ids, dists, err = s.vectorIndex.SearchByVectorDistance(
			searchVector, targetDist, s.index.Config.QueryMaximumResults, allowList)
		if err != nil {
			return nil, nil, errors.Wrap(err, "vector search by distance")
		}
	} else {
		ids, dists, err = s.vectorIndex.SearchByVector(searchVector, limit, allowList)
		if err != nil {
			return nil, nil, errors.Wrap(err, "vector search")
		}
	}

	invertedTook := time.Since(beforeAll)
	beforeVector := time.Now()

	if len(ids) == 0 {
		return nil, nil, nil
	}

	hnswTook := time.Since(beforeVector)
	beforeObjects := time.Now()

	objs, err := s.objectsByDocID(ids, additional)
	if err != nil {
		return nil, nil, err
	}
	objectsTook := time.Since(beforeObjects)

	s.index.logger.WithField("action", "filtered_vector_search").
		WithFields(logrus.Fields{
			"inverted_took":         uint64(invertedTook),
			"hnsw_took":             uint64(hnswTook),
			"retrieve_objects_took": uint64(objectsTook),
		}).Trace("completed filtered vector search")

	return objs, dists, nil
}

func (s *Shard) objectsByDocID(ids []uint64,
	additional additional.Properties) ([]*storobj.Object, error) {
	out := make([]*storobj.Object, len(ids))

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, errors.Errorf("objects bucket not found")
	}

	i := 0

	for _, id := range ids {
		keyBuf := bytes.NewBuffer(nil)
		binary.Write(keyBuf, binary.LittleEndian, &id)
		docIDBytes := keyBuf.Bytes()
		res, err := bucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return nil, err
		}

		if res == nil {
			continue
		}

		unmarshalled, err := storobj.FromBinaryOptional(res, additional)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal data object at position %d", i)
		}

		out[i] = unmarshalled
		i++
	}

	return out[:i], nil
}

func (s *Shard) objectList(ctx context.Context, limit int,
	sort []filters.Sort, additional additional.Properties) ([]*storobj.Object, error) {
	out := make([]*storobj.Object, limit)

	if len(sort) > 0 {
		return s.sortedObjectList(ctx, limit, sort, additional)
	}

	i := 0
	cursor := s.store.Bucket(helpers.ObjectsBucketLSM).Cursor()
	defer cursor.Close()

	for k, v := cursor.First(); k != nil && i < limit; k, v = cursor.Next() {
		obj, err := storobj.FromBinary(v)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarhsal item %d", i)
		}

		out[i] = obj
		i++
	}

	// sort herer objects

	return out[:i], nil
}

type docIDAndStringValue struct {
	docID uint64
	value string
}

func (s *Shard) sortedObjectList(ctx context.Context, limit int, sort []filters.Sort,
	additional additional.Properties) ([]*storobj.Object, error) {
	prop := sort[0].Path[0]
	order := sort[0].Order

	// assumption text prop

	i := 0
	cursor := s.store.Bucket(helpers.ObjectsBucketLSM).Cursor()

	candidates := make([]docIDAndStringValue, 0, limit)
	before := time.Now()

	for k, v := cursor.First(); k != nil && i < limit; k, v = cursor.Next() {
		docID, err := storobj.DocIDFromBinary(v)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarhsal doc id of item %d", i)
		}

		propValue, success, _ := storobj.ParseAndExtractTextProp(v, prop)
		if !success {
			continue
		}

		value := propValue[0]

		if currentIsBetterThanWorstElement(candidates, limit, value, order) {
			candidates = addToCandidates(candidates, limit, docIDAndStringValue{docID: docID, value: value}, order)
		}
	}
	cursor.Close()

	fmt.Printf("entire search took %s\n for %d elements\n", time.Since(before), limit)

	docIDs := make([]uint64, len(candidates))
	for i, cand := range candidates {
		docIDs[i] = cand.docID
	}

	return s.objectsByDocID(docIDs, additional)
}

func currentIsBetterThanWorstElement(candidates []docIDAndStringValue, limit int,
	curr string, order string) bool {
	if len(candidates) < limit {
		return true
	}

	target := candidates[len(candidates)-1].value
	if order == "asc" {
		if curr < target {
			return true
		}
	} else {
		if curr > target {
			return true
		}
	}

	return false
}

func addToCandidates(candidates []docIDAndStringValue, limit int,
	newEntry docIDAndStringValue, order string) []docIDAndStringValue {
	if len(candidates) == 0 {
		return []docIDAndStringValue{newEntry}
	}

	pos := -1
	for i, cand := range candidates {
		pos = i
		if order == "asc" {
			if newEntry.value <= cand.value {
				break
			}
		} else {
			if newEntry.value >= cand.value {
				break
			}
		}
	}

	// fmt.Printf("target pos is %d for %v\n", pos, newEntry.value[:20])

	if len(candidates) < limit {
		candidates = append(candidates[:pos+1], candidates[pos:len(candidates)]...)
	} else {
		candidates = append(candidates[:pos+1], candidates[pos:len(candidates)-1]...)
	}
	candidates[pos] = newEntry

	return candidates
}

func (s *Shard) buildAllowList(ctx context.Context, filters *filters.LocalFilter,
	addl additional.Properties) (helpers.AllowList, error) {
	list, err := inverted.NewSearcher(s.store, s.index.getSchema.GetSchemaSkipAuth(),
		s.invertedRowCache, s.propertyIndices, s.index.classSearcher,
		s.deletedDocIDs, s.index.stopwords, s.versioner.Version()).
		DocIDs(ctx, filters, addl, s.index.Config.ClassName)
	if err != nil {
		return nil, errors.Wrap(err, "build inverted filter allow list")
	}

	return list, nil
}
