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
	"fmt"
	"time"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) ObjectByIDErrDeleted(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties) (*storobj.Object, error) {
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return nil, err
	}

	bytes, err := s.store.Bucket(helpers.ObjectsBucketLSM).GetErrDeleted(idBytes)
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

func (s *Shard) ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties) (*storobj.Object, error) {
	s.activityTrackerRead.Add(1)
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
	s.activityTrackerRead.Add(1)
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

func (s *Shard) ObjectDigestsInRange(ctx context.Context,
	initialUUID, finalUUID strfmt.UUID, limit int) (
	objs []types.RepairResponse, err error,
) {
	initialUUIDBytes, err := uuid.MustParse(initialUUID.String()).MarshalBinary()
	if err != nil {
		return nil, err
	}

	finalUUIDBytes, err := uuid.MustParse(finalUUID.String()).MarshalBinary()
	if err != nil {
		return nil, err
	}

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)

	// note: it's important to first create the on disk cursor so to avoid potential double scanning over flushing memtable
	cursor := bucket.CursorOnDisk()
	defer cursor.Close()

	n := 0

	// note: read-write access to active and flushing memtable will be blocked only during the scope of this inner function
	err = func() error {
		inMemCursor := bucket.CursorInMem()
		defer inMemCursor.Close()

		for k, v := inMemCursor.Seek(initialUUIDBytes); n < limit && k != nil && bytes.Compare(k, finalUUIDBytes) < 1; k, v = inMemCursor.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				obj, err := storobj.FromBinaryUUIDOnly(v)
				if err != nil {
					return fmt.Errorf("cannot unmarshal object: %w", err)
				}

				replicaObj := types.RepairResponse{
					ID:         obj.ID().String(),
					UpdateTime: obj.LastUpdateTimeUnix(),
					// TODO: use version when supported
					Version: 0,
				}

				objs = append(objs, replicaObj)

				n++
			}
		}

		return nil
	}()
	if err != nil {
		return nil, err
	}

	for k, v := cursor.Seek(initialUUIDBytes); n < limit && k != nil && bytes.Compare(k, finalUUIDBytes) < 1; k, v = cursor.Next() {
		select {
		case <-ctx.Done():
			return objs, ctx.Err()
		default:
			obj, err := storobj.FromBinaryUUIDOnly(v)
			if err != nil {
				return objs, fmt.Errorf("cannot unmarshal object: %w", err)
			}

			replicaObj := types.RepairResponse{
				ID:         obj.ID().String(),
				UpdateTime: obj.LastUpdateTimeUnix(),
				// TODO: use version when supported
				Version: 0,
			}

			objs = append(objs, replicaObj)

			n++
		}
	}

	return objs, nil
}

// TODO: This does an actual read which is not really needed, if we see this
// come up in profiling, we could optimize this by adding an explicit Exists()
// on the LSMKV which only checks the bloom filters, which at least in the case
// of a true negative would be considerably faster. For a (false) positive,
// we'd still need to check, though.
func (s *Shard) Exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	s.activityTrackerRead.Add(1)
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

func (s *Shard) vectorByIndexID(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
	keyBuf := make([]byte, 8)
	return s.readVectorByIndexIDIntoSlice(ctx, indexID, &common.VectorSlice{Buff8: keyBuf}, targetVector)
}

func (s *Shard) readVectorByIndexIDIntoSlice(ctx context.Context, indexID uint64, container *common.VectorSlice, targetVector string) ([]float32, error) {
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
	return storobj.VectorFromBinary(bytes, container.Slice, targetVector)
}

func (s *Shard) multiVectorByIndexID(ctx context.Context, indexID uint64, targetVector string) ([][]float32, error) {
	keyBuf := make([]byte, 8)
	return s.readMultiVectorByIndexIDIntoSlice(ctx, indexID, &common.VectorSlice{Buff8: keyBuf}, targetVector)
}

func (s *Shard) readMultiVectorByIndexIDIntoSlice(ctx context.Context, indexID uint64, container *common.VectorSlice, targetVector string) ([][]float32, error) {
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
	return storobj.MultiVectorFromBinary(bytes, container.Slice, targetVector)
}

func (s *Shard) ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	additional additional.Properties, properties []string,
) ([]*storobj.Object, []float32, error) {
	var err error

	// Report slow queries if this method takes longer than expected
	startTime := time.Now()
	defer func() {
		s.slowQueryReporter.LogIfSlow(ctx, startTime, map[string]any{
			"collection":      s.index.Config.ClassName,
			"shard":           s.ID(),
			"tenant":          s.tenant(),
			"query":           "ObjectSearch",
			"filters":         filters,
			"limit":           limit,
			"sort":            sort,
			"cursor":          cursor,
			"keyword_ranking": keywordRanking,
			"version":         s.versioner.Version(),
			"additional":      additional,
			// in addition the slowQueryReporter will extract any slow query details
			// that may or may not have been written into the ctx
		})
	}()

	s.activityTrackerRead.Add(1)
	if keywordRanking != nil {
		if v := s.versioner.Version(); v < 2 {
			return nil, nil, errors.Errorf(
				"shard was built with an older version of " +
					"Weaviate which does not yet support BM25 search")
		}

		var bm25objs []*storobj.Object
		var bm25count []float32
		var objs helpers.AllowList
		var filterDocIds helpers.AllowList

		if filters != nil {
			objs, err = inverted.NewSearcher(s.index.logger, s.store,
				s.index.getSchema.ReadOnlyClass, s.propertyIndices,
				s.index.classSearcher, s.index.stopwords, s.versioner.Version(),
				s.isFallbackToSearchable, s.tenant(), s.index.Config.QueryNestedRefLimit,
				s.bitmapFactory).
				DocIDs(ctx, filters, additional, s.index.Config.ClassName)
			if err != nil {
				return nil, nil, err
			}

			filterDocIds = objs
			defer objs.Close()
		}

		className := s.index.Config.ClassName
		bm25Config := s.index.GetInvertedIndexConfig().BM25
		logger := s.index.logger.WithFields(logrus.Fields{"class": s.index.Config.ClassName, "shard": s.name})
		bm25searcher := inverted.NewBM25Searcher(bm25Config, s.store,
			s.index.getSchema.ReadOnlyClass, s.propertyIndices, s.index.classSearcher,
			s.GetPropertyLengthTracker(), logger, s.versioner.Version())
		bm25objs, bm25count, err = bm25searcher.BM25F(ctx, filterDocIds, className, limit, *keywordRanking, additional)
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
	objs, err := inverted.NewSearcher(s.index.logger, s.store, s.index.getSchema.ReadOnlyClass,
		s.propertyIndices, s.index.classSearcher, s.index.stopwords, s.versioner.Version(),
		s.isFallbackToSearchable, s.tenant(), s.index.Config.QueryNestedRefLimit, s.bitmapFactory).
		Objects(ctx, limit, filters, sort, additional, s.index.Config.ClassName, properties,
			s.index.Config.InvertedSorterDisabled)
	return objs, nil, err
}

func (s *Shard) VectorDistanceForQuery(ctx context.Context, docId uint64, searchVectors []models.Vector, targetVectors []string) ([]float32, error) {
	if len(targetVectors) != len(searchVectors) || len(targetVectors) == 0 {
		return nil, fmt.Errorf("target vectors and search vectors must have the same non-zero length")
	}

	distances := make([]float32, len(targetVectors))
	for j, target := range targetVectors {
		index, ok := s.GetVectorIndex(target)
		if !ok {
			return nil, fmt.Errorf("index %s not found", target)
		}
		var distancer common.QueryVectorDistancer
		switch v := searchVectors[j].(type) {
		case []float32:
			distancer = index.QueryVectorDistancer(v)
		case [][]float32:
			distancer = index.QueryMultiVectorDistancer(v)
		default:
			return nil, fmt.Errorf("unsupported vector type: %T", v)
		}
		dist, err := distancer.DistanceToNode(docId)
		if err != nil {
			return nil, err
		}
		distances[j] = dist
	}
	return distances, nil
}

func (s *Shard) ObjectVectorSearch(ctx context.Context, searchVectors []models.Vector, targetVectors []string, targetDist float32, limit int, filters *filters.LocalFilter, sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties, targetCombination *dto.TargetCombination, properties []string) ([]*storobj.Object, []float32, error) {
	startTime := time.Now()

	defer func() {
		s.slowQueryReporter.LogIfSlow(ctx, startTime, map[string]any{
			"collection": s.index.Config.ClassName,
			"shard":      s.ID(),
			"tenant":     s.tenant(),
			"query":      "ObjectVectorSearch",
			"filters":    filters,
			"limit":      limit,
			"sort":       sort,
			"version":    s.versioner.Version(),
			"additional": additional,
			"group_by":   groupBy,
			// in addition the slowQueryReporter will extract any slow query details
			// that may or may not have been written into the ctx
		})
	}()

	s.activityTrackerRead.Add(1)

	var allowList helpers.AllowList
	if filters != nil {
		beforeFilter := time.Now()
		list, err := s.buildAllowList(ctx, filters, additional)
		if err != nil {
			return nil, nil, err
		}
		allowList = list
		took := time.Since(beforeFilter)
		s.metrics.FilteredVectorFilter(took)
		helpers.AnnotateSlowQueryLog(ctx, "filters_build_allow_list_took", took)
		helpers.AnnotateSlowQueryLog(ctx, "filters_ids_matched", allowList.Len())
	}

	eg := enterrors.NewErrorGroupWrapper(s.index.logger)
	eg.SetLimit(_NUMCPU)
	idss := make([][]uint64, len(targetVectors))
	distss := make([][]float32, len(targetVectors))
	beforeVector := time.Now()

	for i, targetVector := range targetVectors {
		i := i
		targetVector := targetVector
		eg.Go(func() error {
			var (
				ids   []uint64
				dists []float32
				err   error
			)

			vidx, ok := s.GetVectorIndex(targetVector)
			if !ok {
				return fmt.Errorf("index for target vector %q not found", targetVector)
			}

			if limit < 0 {
				switch searchVector := searchVectors[i].(type) {
				case []float32:
					ids, dists, err = vidx.SearchByVectorDistance(
						ctx, searchVector, targetDist, s.index.Config.QueryMaximumResults, allowList)
					if err != nil {
						// This should normally not fail. A failure here could indicate that more
						// attention is required, for example because data is corrupted. That's
						// why this error is explicitly pushed to sentry.
						err = fmt.Errorf("vector search by distance: %w", err)
						entsentry.CaptureException(err)
						return err
					}
				case [][]float32:
					ids, dists, err = vidx.SearchByMultiVectorDistance(
						ctx, searchVector, targetDist, s.index.Config.QueryMaximumResults, allowList)
					if err != nil {
						// This should normally not fail. A failure here could indicate that more
						// attention is required, for example because data is corrupted. That's
						// why this error is explicitly pushed to sentry.
						err = fmt.Errorf("multi vector search by distance: %w", err)
						entsentry.CaptureException(err)
						return err
					}
				default:
					return fmt.Errorf("vector search by distance: unsupported type: %T", searchVectors[i])
				}
			} else {
				switch searchVector := searchVectors[i].(type) {
				case []float32:
					ids, dists, err = vidx.SearchByVector(ctx, searchVector, limit, allowList)
					if err != nil {
						// This should normally not fail. A failure here could indicate that more
						// attention is required, for example because data is corrupted. That's
						// why this error is explicitly pushed to sentry.
						err = fmt.Errorf("vector search: %w", err)
						// annotate for sentry so we know which collection/shard this happened on
						entsentry.CaptureException(fmt.Errorf("collection %q shard %q: %w",
							s.index.Config.ClassName, s.name, err))
						return err
					}
				case [][]float32:
					ids, dists, err = vidx.SearchByMultiVector(ctx, searchVector, limit, allowList)
					if err != nil {
						// This should normally not fail. A failure here could indicate that more
						// attention is required, for example because data is corrupted. That's
						// why this error is explicitly pushed to sentry.
						err = fmt.Errorf("multi vector search: %w", err)
						// annotate for sentry so we know which collection/shard this happened on
						entsentry.CaptureException(fmt.Errorf("collection %q shard %q: %w",
							s.index.Config.ClassName, s.name, err))
						return err
					}
				default:
					return fmt.Errorf("vector search: unsupported type: %T", searchVectors[i])
				}
			}
			if len(ids) == 0 {
				return nil
			}

			idss[i] = ids
			distss[i] = dists
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	if allowList != nil {
		defer allowList.Close()
	}

	idsCombined, distCombined, err := CombineMultiTargetResults(ctx, s, s.index.logger, idss, distss, targetVectors, searchVectors, targetCombination, limit, targetDist)
	if err != nil {
		return nil, nil, err
	}

	if filters != nil {
		s.metrics.FilteredVectorVector(time.Since(beforeVector))
	}
	helpers.AnnotateSlowQueryLog(ctx, "vector_search_took", time.Since(beforeVector))

	if groupBy != nil {
		objs, dists, err := s.groupResults(ctx, idsCombined, distCombined, groupBy, additional, properties)
		if err != nil {
			return nil, nil, err
		}
		return objs, dists, nil
	}

	if len(sort) > 0 {
		beforeSort := time.Now()
		idsCombined, distCombined, err = s.sortDocIDsAndDists(ctx, limit, sort,
			s.index.Config.ClassName, idsCombined, distCombined)
		if err != nil {
			return nil, nil, errors.Wrap(err, "vector search sort")
		}
		took := time.Since(beforeSort)
		if filters != nil {
			s.metrics.FilteredVectorSort(took)
		}
		helpers.AnnotateSlowQueryLog(ctx, "sort_took", took)
	}

	beforeObjects := time.Now()

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	objs, err := storobj.ObjectsByDocID(bucket, idsCombined, additional, properties, s.index.logger)
	if err != nil {
		return nil, nil, err
	}

	took := time.Since(beforeObjects)
	if filters != nil {
		s.metrics.FilteredVectorObjects(took)
	}

	helpers.AnnotateSlowQueryLog(ctx, "objects_took", took)
	return objs, distCombined, nil
}

func (s *Shard) ObjectList(ctx context.Context, limit int, sort []filters.Sort, cursor *filters.Cursor, additional additional.Properties, className schema.ClassName) ([]*storobj.Object, error) {
	s.activityTrackerRead.Add(1)
	if len(sort) > 0 {
		beforeSort := time.Now()
		docIDs, err := s.sortedObjectList(ctx, limit, sort, className)
		if err != nil {
			return nil, err
		}
		helpers.AnnotateSlowQueryLog(ctx, "sort_took", time.Since(beforeSort))
		bucket := s.store.Bucket(helpers.ObjectsBucketLSM)

		beforeObjects := time.Now()
		defer func() {
			took := time.Since(beforeObjects)
			helpers.AnnotateSlowQueryLog(ctx, "objects_took", took)
		}()
		return storobj.ObjectsByDocID(bucket, docIDs, additional, nil, s.index.logger)
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

func (s *Shard) sortedObjectList(ctx context.Context, limit int, sort []filters.Sort,
	className schema.ClassName,
) ([]uint64, error) {
	lsmSorter, err := sorter.NewLSMSorter(s.store, s.index.getSchema.ReadOnlyClass,
		className, s.index.Config.InvertedSorterDisabled)
	if err != nil {
		return nil, errors.Wrap(err, "sort object list")
	}
	docIDs, err := lsmSorter.Sort(ctx, limit, sort)
	if err != nil {
		return nil, errors.Wrap(err, "sort object list")
	}
	return docIDs, nil
}

func (s *Shard) sortDocIDsAndDists(ctx context.Context, limit int, sort []filters.Sort,
	className schema.ClassName, docIDs []uint64, dists []float32,
) ([]uint64, []float32, error) {
	lsmSorter, err := sorter.NewLSMSorter(s.store, s.index.getSchema.ReadOnlyClass,
		className, s.index.Config.InvertedSorterDisabled)
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
	list, err := inverted.NewSearcher(s.index.logger, s.store, s.index.getSchema.ReadOnlyClass,
		s.propertyIndices, s.index.classSearcher, s.index.stopwords, s.versioner.Version(),
		s.isFallbackToSearchable, s.tenant(), s.index.Config.QueryNestedRefLimit, s.bitmapFactory).
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
	err := binary.Write(keyBuf, binary.LittleEndian, &docID)
	if err != nil {
		return "", fmt.Errorf("write doc id to buffer: %w", err)
	}
	docIDBytes := keyBuf.Bytes()
	res, err := bucket.GetBySecondary(0, docIDBytes)
	if err != nil {
		return "", fmt.Errorf("get object by doc id: %w", err)
	}

	prop, _, err := storobj.ParseAndExtractProperty(res, "id")
	if err != nil {
		return "", fmt.Errorf("parse and extract property: %w", err)
	}

	return strfmt.UUID(prop[0]), nil
}

func (s *Shard) batchDeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error {
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return err
	}

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
	docID, updateTime, err := storobj.DocIDAndTimeFromBinary(existing)
	if err != nil {
		return errors.Wrap(err, "get existing doc id from object binary")
	}

	if deletionTime.IsZero() {
		err = bucket.Delete(idBytes)
	} else {
		err = bucket.DeleteWith(idBytes, deletionTime)
	}
	if err != nil {
		return errors.Wrap(err, "delete object from bucket")
	}

	err = s.cleanupInvertedIndexOnDelete(existing, docID)
	if err != nil {
		return errors.Wrap(err, "delete object from bucket")
	}

	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Delete(docID); err != nil {
			return fmt.Errorf("delete from vector index queue of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if err = s.mayDeleteObjectHashTree(idBytes, updateTime); err != nil {
		return errors.Wrap(err, "object deletion in hashtree")
	}

	return nil
}

func (s *Shard) WasDeleted(ctx context.Context, id strfmt.UUID) (bool, time.Time, error) {
	s.activityTrackerRead.Add(1)
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return false, time.Time{}, err
	}

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	return bucket.WasDeleted(idBytes)
}
