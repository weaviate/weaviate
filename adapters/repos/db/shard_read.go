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
	"github.com/semi-technologies/weaviate/adapters/repos/db/sorter"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/sirupsen/logrus"
)

func (s *Shard) objectByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
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
	query []multi.Identifier,
) ([]*storobj.Object, error) {
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
	indexID uint64, acceptDeleted bool,
) (*storobj.Object, error) {
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
	sort []filters.Sort, additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
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
		objs, err := s.objectList(ctx, limit, sort, additional, s.index.Config.ClassName)
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
	sort []filters.Sort, additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
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

	var sortTook uint64
	if len(sort) > 0 {
		beforeSort := time.Now()
		ids, dists, err = s.sortDocIDsAndDists(ctx, limit, sort, additional,
			s.index.Config.ClassName, ids, dists)
		if err != nil {
			return nil, nil, errors.Wrap(err, "vector search sort")
		}
		sortTook = uint64(time.Since(beforeSort))
	}

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
			"sort_took":             uint64(sortTook),
		}).Trace("completed filtered vector search")

	return objs, dists, nil
}

func (s *Shard) objectsByDocID(ids []uint64,
	additional additional.Properties,
) ([]*storobj.Object, error) {
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
	sort []filters.Sort, additional additional.Properties,
	className schema.ClassName,
) ([]*storobj.Object, error) {
	if len(sort) > 0 {
		docIDs, err := s.sortedObjectList(ctx, limit, sort, additional, className)
		if err != nil {
			return nil, err
		}
		return s.objectsByDocID(docIDs, additional)
	}

	return s.allObjectList(ctx, limit, additional, className)
}

func (s *Shard) allObjectList(ctx context.Context, limit int,
	additional additional.Properties,
	className schema.ClassName,
) ([]*storobj.Object, error) {
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

func (s *Shard) sortedObjectList(ctx context.Context, limit int, sort []filters.Sort,
	additional additional.Properties, className schema.ClassName,
) ([]uint64, error) {
	lsmSorter := sorter.NewLSMSorter(s.store, s.index.getSchema.GetSchemaSkipAuth(), className)
	docIDs, err := lsmSorter.Sort(ctx, limit, sort, additional)
	if err != nil {
		return nil, errors.Wrap(err, "sort object list")
	}
	return docIDs, nil
}

func (s *Shard) sortDocIDsAndDists(ctx context.Context, limit int, sort []filters.Sort,
	additional additional.Properties, className schema.ClassName,
	docIDs []uint64, dists []float32,
) ([]uint64, []float32, error) {
	lsmSorter := sorter.NewLSMSorter(s.store, s.index.getSchema.GetSchemaSkipAuth(), className)
	sortedDocIDs, sortedDists, err := lsmSorter.SortDocIDsAndDists(ctx, limit, sort, docIDs, dists, additional)
	if err != nil {
		return nil, nil, errors.Wrap(err, "sort objects with distances")
	}
	return sortedDocIDs, sortedDists, nil
}

func (s *Shard) buildAllowList(ctx context.Context, filters *filters.LocalFilter,
	addl additional.Properties,
) (helpers.AllowList, error) {
	list, err := inverted.NewSearcher(s.store, s.index.getSchema.GetSchemaSkipAuth(),
		s.invertedRowCache, s.propertyIndices, s.index.classSearcher,
		s.deletedDocIDs, s.index.stopwords, s.versioner.Version()).
		DocIDs(ctx, filters, addl, s.index.Config.ClassName)
	if err != nil {
		return nil, errors.Wrap(err, "build inverted filter allow list")
	}

	return list, nil
}

func (s *Shard) uuidFromDocID(docID uint64) (strfmt.UUID, error) {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return "", errors.Errorf("objects bucket not found")
	}

	keyBuf := bytes.NewBuffer(nil)
	binary.Write(keyBuf, binary.LittleEndian, &docID)
	docIDBytes := keyBuf.Bytes()
	res, err := bucket.GetBySecondary(0, docIDBytes)
	if err != nil {
		return "", err
	}

	prop, _, err := storobj.ParseAndExtractProperty(res, "id")
	if err != nil {
		return "", err
	}

	return strfmt.UUID(prop[0]), nil
}

func (s *Shard) batchDeleteObject(ctx context.Context, id strfmt.UUID) error {
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return err
	}

	var docID uint64
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	existing, err := bucket.Get([]byte(idBytes))
	if err != nil {
		return errors.Wrap(err, "unexpected error on previous lookup")
	}

	if existing == nil {
		// nothing to do
		return nil
	}

	// we need the doc ID so we can clean up inverted indices currently
	// pointing to this object
	docID, err = storobj.DocIDFromBinary(existing)
	if err != nil {
		return errors.Wrap(err, "get existing doc id from object binary")
	}

	err = bucket.Delete(idBytes)
	if err != nil {
		return errors.Wrap(err, "delete object from bucket")
	}

	err = s.cleanupInvertedIndexOnDelete(existing, docID)
	if err != nil {
		return errors.Wrap(err, "delete object from bucket")
	}

	// in-mem
	// TODO: do we still need this?
	s.deletedDocIDs.Add(docID)

	if err := s.vectorIndex.Delete(docID); err != nil {
		return errors.Wrap(err, "delete from vector index")
	}

	return nil
}
