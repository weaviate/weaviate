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

package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/sorter"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties) (*storobj.Object, error) {
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

func (s *Shard) MultiObjectByID(ctx context.Context, query []multi.Identifier) ([]*storobj.Object, error) {
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
func (s *Shard) Exists(ctx context.Context, id strfmt.UUID) (bool, error) {
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

func (s *Shard) objectByIndexID(ctx context.Context, indexID uint64, acceptDeleted bool) (*storobj.Object, error) {
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
	return s.readVectorByIndexIDIntoSlice(ctx, indexID, &common.VectorSlice{Buff8: keyBuf})
}

func (s *Shard) readVectorByIndexIDIntoSlice(ctx context.Context, indexID uint64, container *common.VectorSlice) ([]float32, error) {
	binary.LittleEndian.PutUint64(container.Buff8, indexID)

	bytes, newBuff, err := s.store.Bucket(helpers.ObjectsBucketLSM).
		GetBySecondaryIntoMemory(0, container.Buff8, container.Buff)
	if err != nil {
		return nil, err
	}

	if bytes == nil {
		return nil, storobj.NewErrNotFoundf(indexID,
			"no object for doc id, it could have been deleted")
	}

	container.Buff = newBuff
	return storobj.VectorFromBinary(bytes, container.Slice)
}

func (s *Shard) ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor, additional additional.Properties) ([]*storobj.Object, []float32, error) {
	if keywordRanking != nil {
		if v := s.versioner.Version(); v < 2 {
			return nil, nil, errors.Errorf(
				"shard was built with an older version of " +
					"Weaviate which does not yet support BM25 search")
		}

		var bm25objs []*storobj.Object
		var bm25count []float32
		var err error
		var objs helpers.AllowList
		var filterDocIds helpers.AllowList

		if filters != nil {
			objs, err = inverted.NewSearcher(s.index.logger, s.store,
				s.index.getSchema.GetSchemaSkipAuth(), s.propertyIndices,
				s.index.classSearcher, s.index.stopwords, s.versioner.Version(),
				s.isFallbackToSearchable, s.tenant(), s.index.Config.QueryNestedRefLimit).
				DocIDs(ctx, filters, additional, s.index.Config.ClassName)
			if err != nil {
				return nil, nil, err
			}

			filterDocIds = objs
		}

		className := s.index.Config.ClassName
		bm25Config := s.index.getInvertedIndexConfig().BM25
		bm25searcher := inverted.NewBM25Searcher(bm25Config, s.store,
			s.index.getSchema.GetSchemaSkipAuth(), s.propertyIndices, s.index.classSearcher,
			s.GetPropertyLengthTracker(), s.index.logger, s.versioner.Version())
		bm25objs, bm25count, err = bm25searcher.BM25F(ctx, filterDocIds, className, limit, *keywordRanking)
		if err != nil {
			return nil, nil, err
		}

		return bm25objs, bm25count, nil
	}

	if filters == nil {
		objs, err := s.ObjectList(ctx, limit, sort,
			cursor, additional, s.index.Config.ClassName)
		return objs, nil, err
	}
	objs, err := inverted.NewSearcher(s.index.logger, s.store, s.index.getSchema.GetSchemaSkipAuth(),
		s.propertyIndices, s.index.classSearcher, s.index.stopwords, s.versioner.Version(),
		s.isFallbackToSearchable, s.tenant(), s.index.Config.QueryNestedRefLimit).
		Objects(ctx, limit, filters, sort, additional, s.index.Config.ClassName)
	return objs, nil, err
}

func (s *Shard) ObjectVectorSearch(ctx context.Context, searchVector []float32, targetDist float32, limit int, filters *filters.LocalFilter, sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties) ([]*storobj.Object, []float32, error) {
	var (
		ids       []uint64
		dists     []float32
		err       error
		allowList helpers.AllowList
	)

	if filters != nil {
		beforeFilter := time.Now()
		list, err := s.buildAllowList(ctx, filters, additional)
		if err != nil {
			return nil, nil, err
		}
		allowList = list
		s.metrics.FilteredVectorFilter(time.Since(beforeFilter))
	}

	beforeVector := time.Now()
	if limit < 0 {
		ids, dists, err = s.queue.SearchByVectorDistance(
			searchVector, targetDist, s.index.Config.QueryMaximumResults, allowList)
		if err != nil {
			return nil, nil, errors.Wrap(err, "vector search by distance")
		}
	} else {
		ids, dists, err = s.queue.SearchByVector(searchVector, limit, allowList)
		if err != nil {
			return nil, nil, errors.Wrap(err, "vector search")
		}
	}
	if len(ids) == 0 {
		return nil, nil, nil
	}

	if filters != nil {
		s.metrics.FilteredVectorVector(time.Since(beforeVector))
	}

	if groupBy != nil {
		return s.groupResults(ctx, ids, dists, groupBy, additional)
	}

	if len(sort) > 0 {
		beforeSort := time.Now()
		ids, dists, err = s.sortDocIDsAndDists(ctx, limit, sort,
			s.index.Config.ClassName, ids, dists)
		if err != nil {
			return nil, nil, errors.Wrap(err, "vector search sort")
		}
		if filters != nil {
			s.metrics.FilteredVectorSort(time.Since(beforeSort))
		}
	}

	beforeObjects := time.Now()

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	objs, err := storobj.ObjectsByDocID(bucket, ids, additional)
	if err != nil {
		return nil, nil, err
	}

	if filters != nil {
		s.metrics.FilteredVectorObjects(time.Since(beforeObjects))
	}

	return objs, dists, nil
}

func (s *Shard) ObjectList(ctx context.Context, limit int, sort []filters.Sort, cursor *filters.Cursor, additional additional.Properties, className schema.ClassName) ([]*storobj.Object, error) {
	if len(sort) > 0 {
		docIDs, err := s.sortedObjectList(ctx, limit, sort, className)
		if err != nil {
			return nil, err
		}
		bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
		return storobj.ObjectsByDocID(bucket, docIDs, additional)
	}

	if cursor == nil {
		cursor = &filters.Cursor{After: "", Limit: limit}
	}
	return s.cursorObjectList(ctx, cursor, additional, className)
}

func (s *Shard) cursorObjectList(ctx context.Context, c *filters.Cursor,
	additional additional.Properties,
	className schema.ClassName,
) ([]*storobj.Object, error) {
	cursor := s.store.Bucket(helpers.ObjectsBucketLSM).Cursor()
	defer cursor.Close()

	var key, val []byte
	if c.After == "" {
		key, val = cursor.First()
	} else {
		uuidBytes, err := uuid.MustParse(c.After).MarshalBinary()
		if err != nil {
			return nil, errors.Wrap(err, "after argument is not a valid uuid")
		}
		key, val = cursor.Seek(uuidBytes)
		if bytes.Equal(key, uuidBytes) {
			// move cursor by one if it's the same ID
			key, val = cursor.Next()
		}
	}

	i := 0
	out := make([]*storobj.Object, c.Limit)

	for ; key != nil && i < c.Limit; key, val = cursor.Next() {
		obj, err := storobj.FromBinary(val)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarhsal item %d", i)
		}

		out[i] = obj
		i++
	}

	return out[:i], nil
}

func (s *Shard) sortedObjectList(ctx context.Context, limit int, sort []filters.Sort, className schema.ClassName) ([]uint64, error) {
	lsmSorter, err := sorter.NewLSMSorter(s.store, s.index.getSchema.GetSchemaSkipAuth(), className)
	if err != nil {
		return nil, errors.Wrap(err, "sort object list")
	}
	docIDs, err := lsmSorter.Sort(ctx, limit, sort)
	if err != nil {
		return nil, errors.Wrap(err, "sort object list")
	}
	return docIDs, nil
}

func (s *Shard) sortDocIDsAndDists(ctx context.Context, limit int, sort []filters.Sort, className schema.ClassName, docIDs []uint64, dists []float32) ([]uint64, []float32, error) {
	lsmSorter, err := sorter.NewLSMSorter(s.store, s.index.getSchema.GetSchemaSkipAuth(), className)
	if err != nil {
		return nil, nil, errors.Wrap(err, "sort objects with distances")
	}
	sortedDocIDs, sortedDists, err := lsmSorter.SortDocIDsAndDists(ctx, limit, sort, docIDs, dists)
	if err != nil {
		return nil, nil, errors.Wrap(err, "sort objects with distances")
	}
	return sortedDocIDs, sortedDists, nil
}

func (s *Shard) buildAllowList(ctx context.Context, filters *filters.LocalFilter, addl additional.Properties) (helpers.AllowList, error) {
	list, err := inverted.NewSearcher(s.index.logger, s.store, s.index.getSchema.GetSchemaSkipAuth(),
		s.propertyIndices, s.index.classSearcher, s.index.stopwords, s.versioner.Version(),
		s.isFallbackToSearchable, s.tenant(), s.index.Config.QueryNestedRefLimit).
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
	existing, err := bucket.Get(idBytes)
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

	if err := s.queue.Delete(docID); err != nil {
		return errors.Wrap(err, "delete from vector index")
	}

	return nil
}

func (s *Shard) WasDeleted(ctx context.Context, id strfmt.UUID) (bool, error) {
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return false, err
	}

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	return bucket.WasDeleted(idBytes)
}
