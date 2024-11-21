package db

import (
	"context"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/sorter"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

// ReadIndex is read-only index that usually loaded from
// disk or object storage.
// Currently we implement only for `flat` index.
// A single `ReadIndex` is a index for whole collection.
// Unlike `db.Index` and `db.Shard`, ReadIndex doesn't need to be aware of shards, because
// any readindex can serve query for any shard (just to make sure `/collectin/tenant` path is correct)
// We can have something like ReadShard. But later.
type ReadIndex struct {
	collection string
	basePath   string

	log logrus.FieldLogger

	// map[tenant]store
	// need RWLock for protection.
	stores map[string]*lsmkv.Store

	// if it's first time handling for a tenant, we use fetcher to download tenant's data.
	// same if tenant version is updated.
	fetcher *LSMFetcher

	// assume each collection has single static index which is of flat index type.
	index VectorIndex

	stopwords *stopwords.Detector

	fallbackSearchable func() bool

	schema ClassGetter
}

type ClassGetter interface {
	Collection(ctx context.Context, name string) (*models.Class, error)
}

func NewReadIndex(collection, basePath string, fetcher *LSMFetcher, schema ClassGetter, log logrus.FieldLogger) *ReadIndex {
	detectStopwords, _ := stopwords.NewDetectorFromPreset(stopwords.EnglishPreset)
	fallbackToSearchable := func() bool {
		return false
	}

	return &ReadIndex{
		collection:         collection,
		basePath:           basePath,
		log:                log,
		fetcher:            fetcher,
		stores:             make(map[string]*lsmkv.Store),
		stopwords:          detectStopwords,
		fallbackSearchable: fallbackToSearchable,
		schema:             schema,
	}
}

// creates lsmkv store for given tenant.
// this is usually called when ReadIndex is accessed for first time for
// a tenant
func (r *ReadIndex) initStore(ctx context.Context, tenant string) (*lsmkv.Store, error) {
	// currently just fetch from upstream. no cache for now. Just to keep it simple
	store, _, err := r.fetcher.Fetch(ctx, r.collection, tenant, 0)
	if err != nil {
		return nil, err
	}
	r.stores[tenant] = store
	return store, nil
}

func (r *ReadIndex) getStoreFor(ctx context.Context, tenant string) (*lsmkv.Store, error) {
	var err error

	s, ok := r.stores[tenant]
	if !ok {
		s, err = r.initStore(ctx, tenant)
	}
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (r *ReadIndex) ID() string {
	return strings.ToLower(r.collection)
}

func (r *ReadIndex) MultiObjectByID(ctx context.Context, query []multi.Identifier, tenant string) ([]*storobj.Object, error) {
	objects := make([]*storobj.Object, len(query))

	s, err := r.getStoreFor(ctx, tenant)
	if err != nil {
		return nil, err
	}

	ids := make([][]byte, len(query))
	for i, q := range query {
		idBytes, err := uuid.MustParse(q.ID).MarshalBinary()
		if err != nil {
			return nil, err
		}

		ids[i] = idBytes
	}

	bucket := s.Bucket(helpers.ObjectsBucketLSM)
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

func (r *ReadIndex) ObjectVectorSearch(
	ctx context.Context,
	search [][]float32,
	target []string,
	distance float32,
	limit int,
	filters *filters.LocalFilter,
	sort []filters.Sort,
	groupBy *searchparams.GroupBy,
	additional additional.Properties,
	_ *additional.ReplicationProperties, // we don't need replication
	tenant string,
	targetCombination *dto.TargetCombination,
	properties []string,
) ([]*storobj.Object, []float32, error) {
	var (
		ids  []uint64
		dist []float32
		err  error
	)

	s, err := r.getStoreFor(ctx, tenant)
	if err != nil {
		return nil, nil, err
	}

	if len(search) > 1 {
		panic("assert failure. got more than on search vector")
	}

	if limit < 0 {
		ids, dist, err = r.index.SearchByVectorDistance(ctx, search[0], distance, 1000, nil)
	} else {
		ids, dist, err = r.index.SearchByVector(ctx, search[0], limit, nil)
	}

	if err != nil {
		return nil, nil, err
	}

	bucket := s.Bucket(helpers.ObjectsBucketLSM)
	objs, err := storobj.ObjectsByDocID(bucket, ids, additional, properties, r.log)
	if err != nil {
		return nil, nil, err
	}

	return objs, dist, nil
}

func (r *ReadIndex) ObjectByID(
	ctx context.Context,
	id strfmt.UUID,
	props search.SelectProperties,
	addl additional.Properties,
	replProps *additional.ReplicationProperties,
	tenant string,
) (*storobj.Object, error) {
	s, err := r.getStoreFor(ctx, tenant)
	if err != nil {
		return nil, err
	}
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return nil, err
	}

	bytes, err := s.Bucket(helpers.ObjectsBucketLSM).Get(idBytes)
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

func (r *ReadIndex) ObjectSearch(
	ctx context.Context,
	limit int,
	filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking,
	sort []filters.Sort,
	cursor *filters.Cursor,
	addl additional.Properties,
	repl *additional.ReplicationProperties,
	tenant string,
	autoCut int,
	properties []string,
) ([]*storobj.Object, []float32, error) {
	s, err := r.getStoreFor(ctx, tenant)
	if err != nil {
		return nil, nil, err
	}

	getClass := func(name string) *models.Class {
		v, err := r.schema.Collection(ctx, name)
		if err != nil {
			panic("error getting class")
		}
		return v
	}

	if keywordRanking != nil {
		var (
			bm25objs     []*storobj.Object
			bm25count    []float32
			objs         helpers.AllowList
			filterDocIds helpers.AllowList
		)

		if filters != nil {
			objs, err = inverted.NewSearcher(r.log, s,
				getClass, nil,
				nil, r.stopwords, 0,
				r.fallbackSearchable, tenant, 1000,
				nil).
				DocIDs(ctx, filters, addl, schema.ClassName(r.collection))
			if err != nil {
				return nil, nil, err
			}

			filterDocIds = objs
		}

		bconf := schema.BM25Config{
			B:  0.75,
			K1: 1.2,
		}
		bsearch := inverted.NewBM25Searcher(bconf, s, getClass, nil, nil, nil, r.log, 0)
		bm25objs, bm25count, err = bsearch.BM25F(ctx, filterDocIds, schema.ClassName(r.collection), limit, *keywordRanking, addl)
		if err != nil {
			return nil, nil, err
		}

		return bm25objs, bm25count, nil
	}
	if filters == nil {
		objs, err := r.ObjectList(ctx, s, getClass, limit, sort, cursor, addl, schema.ClassName(r.collection))
		return objs, nil, err
	}

	objs, err := inverted.NewSearcher(r.log, s, getClass, nil, nil, r.stopwords, 0, nil, tenant, 1000, nil).Objects(ctx, limit, filters, sort, addl, schema.ClassName(r.collection), properties)

	return objs, nil, err
}

func (r *ReadIndex) ObjectList(ctx context.Context, store *lsmkv.Store, getClass func(name string) *models.Class, limit int, sort []filters.Sort, cursor *filters.Cursor, addl additional.Properties, classname schema.ClassName) ([]*storobj.Object, error) {
	// assums len(sort) > 0
	docIDs, err := r.sortedObjectList(ctx, limit, sort, store, getClass, classname)
	if err != nil {
		return nil, err
	}
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	return storobj.ObjectsByDocID(bucket, docIDs, addl, nil, r.log)
}

func (r *ReadIndex) sortedObjectList(ctx context.Context, limit int, sort []filters.Sort, store *lsmkv.Store, getClass func(name string) *models.Class, className schema.ClassName) ([]uint64, error) {
	lsmSorter, err := sorter.NewLSMSorter(store, getClass, className)
	if err != nil {
		return nil, errors.Wrap(err, "sort object list")
	}
	docIDs, err := lsmSorter.Sort(ctx, limit, sort)
	if err != nil {
		return nil, errors.Wrap(err, "sort object list")
	}
	return docIDs, nil
}
