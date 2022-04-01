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
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/semi-technologies/weaviate/usecases/vectorizer"
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
	filters *filters.LocalFilter, keywordRanking *traverser.KeywordRankingParams,
	additional additional.Properties) ([]*storobj.Object, error) {
	if keywordRanking != nil {
		if v := s.versioner.Version(); v < 2 {
			return nil, errors.Errorf("shard was built with an older version of " +
				"Weaviate which does not yet support BM25 search")
		}

		bm25Config := s.index.getInvertedIndexConfig().BM25

		return inverted.NewBM25Searcher(bm25Config, s.store,
			s.index.getSchema.GetSchemaSkipAuth(), s.invertedRowCache,
			s.propertyIndices, s.index.classSearcher, s.deletedDocIDs, s.propLengths,
			s.index.logger, s.versioner.Version()).
			Object(ctx, limit, keywordRanking, filters, additional, s.index.Config.ClassName)
	}

	if filters == nil {
		return s.objectList(ctx, limit, additional)
	}
	return inverted.NewSearcher(s.store, s.index.getSchema.GetSchemaSkipAuth(),
		s.invertedRowCache, s.propertyIndices, s.index.classSearcher,
		s.deletedDocIDs, s.versioner.Version()).
		Object(ctx, limit, filters, additional, s.index.Config.ClassName)
}

func (s *Shard) localObjectVectorSearch(ctx context.Context,
	searchVector []float32, certainty float64, limit int, filters *filters.LocalFilter,
	additional additional.Properties, isInternalCaller bool) ([]*storobj.Object, []float32, error) {
	if limit < 0 {
		additional.Vector = true
		return s.objectVectorSearchByDistance(
			ctx, searchVector, filters, additional, certainty, isInternalCaller)
	}

	return s.objectVectorSearch(ctx, searchVector, limit, filters, additional)
}

func (s *Shard) objectVectorSearchByDistance(ctx context.Context, searchVector []float32,
	filters *filters.LocalFilter, additional additional.Properties,
	certainty float64, isInternalCaller bool) ([]*storobj.Object, []float32, error) {
	var (
		searchParams = s.newSearchByDistParams(isInternalCaller)

		allowList  helpers.AllowList
		resultObjs []*storobj.Object
		resultDist []float32
	)

	if filters != nil {
		list, err := s.buildAllowList(ctx, filters, additional)
		if err != nil {
			return nil, nil, err
		}
		allowList = list
	}

	recursiveSearch := func() (bool, error) {
		shouldContinue := false

		ids, dist, err := s.vectorIndex.SearchByVector(searchVector, searchParams.totalLimit, allowList)
		if err != nil {
			return shouldContinue, errors.Wrap(err, "vector search")
		}

		// ensures the indexers aren't out of range
		offsetCap := searchParams.offsetCapacity(ids)
		totalLimitCap := searchParams.totalLimitCapacity(ids)

		ids, dist = ids[offsetCap:totalLimitCap], dist[offsetCap:totalLimitCap]

		if len(ids) == 0 {
			return shouldContinue, nil
		}

		objs, err := s.objectsByDocID(ids, additional)
		if err != nil {
			return shouldContinue, err
		}

		// we can determine before checking all results whether
		// another search will be needed, by seeing if the last
		// result's certainty is still above the desired
		// threshold. this is possible because the index sorts
		// results by distance descending prior to returning
		lastObj := objs[len(objs)-1]
		shouldContinue, err = isAboveCertaintyThreshold(lastObj, searchVector, certainty)
		if err != nil {
			return false, err
		}

		for i := range objs {
			aboveThresh, err := isAboveCertaintyThreshold(objs[i], searchVector, certainty)
			if err != nil {
				return false, err
			}

			if aboveThresh {
				resultObjs = append(resultObjs, objs[i])
				resultDist = append(resultDist, dist[i])
			} else {
				// as soon as we encounter a certainty which
				// is below threshold, we can stop searching
				break
			}
		}

		return shouldContinue, nil
	}

	shouldContinue, err := recursiveSearch()
	if err != nil {
		return nil, nil, err
	}

	for shouldContinue {
		searchParams.iterate()
		if searchParams.maxLimitReached() {
			s.index.logger.
				WithField("action", "unlimited_vector_search").
				WithField("path", s.index.Config.RootPath).
				Warnf("maximum search limit of %d results has been reached",
					searchParams.maximumSearchLimit)
			break
		}

		shouldContinue, err = recursiveSearch()
		if err != nil {
			return nil, nil, err
		}
	}

	return resultObjs, resultDist, nil
}

func (s *Shard) objectVectorSearch(ctx context.Context, searchVector []float32,
	limit int, filters *filters.LocalFilter, additional additional.Properties) ([]*storobj.Object, []float32, error) {
	var allowList helpers.AllowList
	beforeAll := time.Now()
	if filters != nil {
		list, err := s.buildAllowList(ctx, filters, additional)
		if err != nil {
			return nil, nil, err
		}
		allowList = list
	}
	invertedTook := time.Since(beforeAll)
	beforeVector := time.Now()
	ids, dists, err := s.vectorIndex.SearchByVector(searchVector, limit, allowList)
	if err != nil {
		return nil, nil, errors.Wrap(err, "vector search")
	}

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
	additional additional.Properties) ([]*storobj.Object, error) {
	out := make([]*storobj.Object, limit)
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

	return out[:i], nil
}

func (s *Shard) buildAllowList(ctx context.Context, filters *filters.LocalFilter,
	addl additional.Properties) (helpers.AllowList, error) {
	list, err := inverted.NewSearcher(s.store, s.index.getSchema.GetSchemaSkipAuth(),
		s.invertedRowCache, s.propertyIndices, s.index.classSearcher,
		s.deletedDocIDs, s.versioner.Version()).
		DocIDs(ctx, filters, addl, s.index.Config.ClassName)
	if err != nil {
		return nil, errors.Wrap(err, "build inverted filter allow list")
	}

	return list, nil
}

func (s *Shard) newSearchByDistParams(internalCaller bool) *searchByDistParams {
	initialOffset := 0
	initialLimit := defaultSearchByDistInitialLimit

	var maxLimit int64

	// search by distance will be used both internally (within weaviate)
	// and externally (initiated by client), however the usecase is a
	// bit different.
	//
	// if internal, we do not want to set a max limit on the results,
	// because likely the caller is an aggregation process. if this
	// search is initiated directly by the caller, for instance with
	// a Get query, the limit will be set to the host's maximum query
	// limit.
	if internalCaller {
		maxLimit = -1
	} else {
		maxLimit = s.index.Config.QueryMaximumResults
	}

	return &searchByDistParams{
		offset:             initialOffset,
		limit:              initialLimit,
		totalLimit:         initialOffset + initialLimit,
		maximumSearchLimit: maxLimit,
	}
}

const (
	// the initial limit of 100 here is an
	// arbitrary decision, and can be tuned
	// as needed
	defaultSearchByDistInitialLimit = 100
	// the decision to increase the limit in
	// multiples of 10 here is an arbitrary
	// decision, and can be tuned as needed
	defaultSearchByDistLimitMultiplier = 10
)

type searchByDistParams struct {
	offset             int
	limit              int
	totalLimit         int
	maximumSearchLimit int64
}

func (params *searchByDistParams) offsetCapacity(ids []uint64) int {
	var offsetCap int
	if params.offset < len(ids) {
		offsetCap = params.offset
	} else {
		offsetCap = len(ids)
	}

	return offsetCap
}

func (params *searchByDistParams) totalLimitCapacity(ids []uint64) int {
	var totalLimitCap int
	if params.totalLimit < len(ids) {
		totalLimitCap = params.totalLimit
	} else {
		totalLimitCap = len(ids)
	}

	return totalLimitCap
}

func (params *searchByDistParams) iterate() {
	params.offset = params.totalLimit
	params.limit *= defaultSearchByDistLimitMultiplier
	params.totalLimit = params.offset + params.limit
}

func (params *searchByDistParams) maxLimitReached() bool {
	if params.maximumSearchLimit < 0 {
		return true
	}

	return int64(params.totalLimit) > params.maximumSearchLimit
}

func isAboveCertaintyThreshold(obj *storobj.Object, searchVector []float32, certainty float64) (bool, error) {
	res := obj.SearchResult(additional.Properties{Vector: true})
	nd, err := vectorizer.NormalizedDistance(searchVector, res.Vector)
	if err != nil {
		return false, err
	}

	objCertainty := 1 - nd

	return float64(objCertainty) >= certainty, nil
}
