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

package aggregator

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/docid"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/traverser"
	"github.com/weaviate/weaviate/usecases/traverser/hybrid"
)

// grouper is the component which identifies the top-n groups for a specific
// group-by parameter. It is used as part of the grouped aggregator, which then
// additionally performs an aggregation for each group.
type grouper struct {
	*Aggregator
	values    map[interface{}]map[uint64]struct{} // map[value][docID]struct, to keep docIds unique
	topGroups []group
	limit     int
}

func newGrouper(a *Aggregator, limit int) *grouper {
	return &grouper{
		Aggregator: a,
		values:     map[interface{}]map[uint64]struct{}{},
		limit:      limit,
	}
}

func (g *grouper) Do(ctx context.Context) ([]group, error) {
	if len(g.params.GroupBy.Slice()) > 1 {
		return nil, fmt.Errorf("grouping by cross-refs not supported")
	}
	isVectorEmpty, err := dto.IsVectorEmpty(g.params.SearchVector)
	if err != nil {
		return nil, fmt.Errorf("grouper: %w", err)
	}

	if g.params.Filters == nil && isVectorEmpty && g.params.Hybrid == nil {
		return g.groupAll(ctx)
	} else {
		return g.groupFiltered(ctx)
	}
}

func (g *grouper) groupAll(ctx context.Context) ([]group, error) {
	err := ScanAllLSM(ctx, g.store, func(prop *models.PropertySchema, docID uint64) (bool, error) {
		return true, g.addElementById(prop, docID)
	}, &storobj.PropertyExtraction{
		PropertyPaths: [][]string{{g.params.GroupBy.Property.String()}},
	})
	if err != nil {
		return nil, errors.Wrap(err, "group all (unfiltered)")
	}

	return g.aggregateAndSelect()
}

func (g *grouper) groupFiltered(ctx context.Context) ([]group, error) {
	ids, err := g.fetchDocIDs(ctx)
	if err != nil {
		return nil, err
	}

	if err := docid.ScanObjectsLSM(g.store, ids,
		func(prop *models.PropertySchema, docID uint64) (bool, error) {
			return true, g.addElementById(prop, docID)
		}, []string{g.params.GroupBy.Property.String()}); err != nil {
		return nil, err
	}

	return g.aggregateAndSelect()
}

func (g *grouper) fetchDocIDs(ctx context.Context) (ids []uint64, err error) {
	allowList, err := g.buildAllowList(ctx)
	if err != nil {
		return nil, err
	}
	if allowList != nil {
		defer allowList.Close()
	}

	isVectorEmpty, err := dto.IsVectorEmpty(g.params.SearchVector)
	if err != nil {
		return nil, fmt.Errorf("grouper: fetch doc ids: %w", err)
	}

	if !isVectorEmpty {
		ids, _, err = g.vectorSearch(ctx, allowList, g.params.SearchVector)
		if err != nil {
			return nil, fmt.Errorf("failed to perform vector search: %w", err)
		}
	} else if g.params.Hybrid != nil {
		ids, err = g.hybrid(ctx, allowList, g.modules)
		if err != nil {
			return nil, fmt.Errorf("hybrid search: %w", err)
		}
	} else {
		ids = allowList.Slice()
	}

	return
}

func (g *grouper) hybrid(ctx context.Context, allowList helpers.AllowList, modules *modules.Provider) ([]uint64, error) {
	sparseSearch := func() ([]*storobj.Object, []float32, error) {
		kw, err := g.buildHybridKeywordRanking()
		if err != nil {
			return nil, nil, fmt.Errorf("build hybrid keyword ranking: %w", err)
		}

		if g.params.ObjectLimit == nil {
			limit := hybrid.DefaultLimit
			g.params.ObjectLimit = &limit
		}

		sparse, dists, err := g.bm25Objects(ctx, kw)
		if err != nil {
			return nil, nil, fmt.Errorf("aggregate sparse search: %w", err)
		}

		return sparse, dists, nil
	}

	denseSearch := func(vec models.Vector) ([]*storobj.Object, []float32, error) {
		res, dists, err := g.objectVectorSearch(ctx, vec, allowList)
		if err != nil {
			return nil, nil, fmt.Errorf("aggregate grouped dense search: %w", err)
		}

		return res, dists, nil
	}

	res, err := hybrid.Search(ctx, &hybrid.Params{
		HybridSearch: g.params.Hybrid,
		Keyword:      nil,
		Class:        g.params.ClassName.String(),
	}, g.logger, sparseSearch, denseSearch, nil, modules, g.getSchema, traverser.NewTargetParamHelper())
	if err != nil {
		return nil, err
	}

	ids := make([]uint64, len(res))
	for i, r := range res {
		ids[i] = *r.DocID
	}

	return ids, nil
}

func (g *grouper) addElementById(s *models.PropertySchema, docID uint64) error {
	if s == nil {
		return nil
	}

	item, ok := (*s).(map[string]interface{})[g.params.GroupBy.Property.String()]
	if !ok {
		return nil
	}

	switch val := item.(type) {
	case []string:
		for i := range val {
			g.addItem(val[i], docID)
		}
	case []float64:
		for i := range val {
			g.addItem(val[i], docID)
		}
	case []bool:
		for i := range val {
			g.addItem(val[i], docID)
		}
	case []interface{}:
		for i := range val {
			g.addItem(val[i], docID)
		}
	case models.MultipleRef:
		for i := range val {
			g.addItem(val[i].Beacon, docID)
		}
	default:
		g.addItem(val, docID)
	}

	return nil
}

func (g *grouper) addItem(item interface{}, docID uint64) {
	idsMap, ok := g.values[item]
	if !ok {
		idsMap = map[uint64]struct{}{}
	}
	idsMap[docID] = struct{}{}
	g.values[item] = idsMap
}

func (g *grouper) aggregateAndSelect() ([]group, error) {
	for value, idsMap := range g.values {
		count := len(idsMap)
		ids := make([]uint64, count)

		i := 0
		for id := range idsMap {
			ids[i] = id
			i++
		}

		g.insertOrdered(group{
			res: aggregation.Group{
				GroupedBy: &aggregation.GroupedBy{
					Path:  g.params.GroupBy.Slice(),
					Value: value,
				},
				Count: count,
			},
			docIDs: ids,
		})
	}

	return g.topGroups, nil
}

func (g *grouper) insertOrdered(elem group) {
	if len(g.topGroups) == 0 {
		g.topGroups = []group{elem}
		return
	}

	added := false
	for i, existing := range g.topGroups {
		if existing.res.Count > elem.res.Count {
			continue
		}

		// we have found the first one that's smaller so we must insert before i
		g.topGroups = append(
			g.topGroups[:i], append(
				[]group{elem},
				g.topGroups[i:]...,
			)...,
		)

		added = true
		break
	}

	if len(g.topGroups) > g.limit {
		g.topGroups = g.topGroups[:len(g.topGroups)-1]
	}

	if !added && len(g.topGroups) < g.limit {
		g.topGroups = append(g.topGroups, elem)
	}
}

// ScanAllLSM iterates over every row in the object buckets.
// Caller can specify which properties it is interested in to make the scanning more performant, or pass nil to
// decode everything.
func ScanAllLSM(ctx context.Context, store *lsmkv.Store, scan docid.ObjectScanFn, properties *storobj.PropertyExtraction) error {
	b := store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return fmt.Errorf("objects bucket not found")
	}

	c := b.Cursor()
	defer c.Close()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			elem, err := storobj.FromBinaryOptional(v, additional.Properties{}, properties)
			if err != nil {
				return errors.Wrapf(err, "unmarshal data object")
			}

			// scanAll has no abort, so we can ignore the first arg
			properties := elem.Properties()
			_, err = scan(&properties, elem.DocID)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
