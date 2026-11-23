//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package postingedge

import (
	"context"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// The filtered search of the index a shard opens (integration-plan.md,
// section 8, amendment 1). A filter is the doc ids a search may return. With
// fewer allowed than the cutoff the search is flat: the routing code of every
// allowed vector ranks it, and the best are rescored. With more, the index is
// searched as without a filter, and a member the filter leaves out routes and
// is never a candidate (postingSearch.allow). No path returns a doc id the
// filter leaves out.

// DefaultFlatSearchCutoff is the allowed vectors under which a filtered
// search is flat: the flatSearchCutoff of HNSW, compared the same way.
const DefaultFlatSearchCutoff = 40_000

// flatCutoff is the cutoff of the index: the default, the one the config
// gives, or none at all.
func (p *Persistent) flatCutoff() int {
	switch {
	case p.cfg.flatCutoff == 0:
		return DefaultFlatSearchCutoff
	case p.cfg.flatCutoff < 0:
		return 0
	}
	return p.cfg.flatCutoff
}

// searchIndex is the search of an index with a base: the flat search of a
// filter that allows fewer vectors than the cutoff, and otherwise the search
// of the index, with the filter if there is one. The caller holds mu for
// reading.
func (p *Persistent) searchIndex(ctx context.Context, vector []float32, k int, allow helpers.AllowList,
	stats *diskSearchStats,
) ([]uint64, []float32, error) {
	if allow != nil && allow.Len() < p.flatCutoff() {
		return p.g.flatSearch(ctx, vector, k, p.flatRescore(k, routingBitsOf(&p.g.meta)), allow, stats)
	}
	search := p.g.searcher()
	search.allow = allow
	return search.run(ctx, vector, k, nil, stats)
}

// flatRescore is how many of the allowed vectors a flat search rescores, by
// the width in bits of the code that ranks them there: with the automatic
// limit, the rule of the rescore applied to that width, and at least four
// times k; with a limit of its own, that limit and at least k. ef raises it,
// so that the flat search too finds more as ef grows (integration-plan.md,
// section 8, amendment 3). The code that ranks is the routing code, 512 bits
// at 1 536 dimensions, where the rule gives 507: with the 64 it gives for the
// full code, the routing code has a ceiling of recall of 0.9255 on dbpedia
// (integration-plan.md, section 4).
func (p *Persistent) flatRescore(k, bits int) int {
	limit, perK := p.uc.RQ.RescoreLimit, 0
	if limit == ent.RescoreLimitAuto {
		limit, perK = autoRescoreLimit(bits), autoRescorePerK
	}
	return max(limit, k, perK*k, int(p.g.ix.ef.Load()))
}

// routingBitsOf is the width of the code the postings are ranked by: the
// routing code, or the full code where the routing code is no narrower.
func routingBitsOf(meta *growMeta) int {
	if meta.RoutingBits > 0 {
		return meta.RoutingBits
	}
	return scanBitsOf(meta)
}

// flatSearch is the flat search of a filter: every allowed doc id the index
// holds and has not marked deleted is ranked by its routing code, which is in
// memory; the best rescore of them are rescored with their exact vectors, all
// of them where there are no more than that; and the best k of those are the
// answer. It loads no posting. An index with no codes in memory has no flat
// search yet: it refuses, and reads no code from anywhere else
// (integration-plan.md, section 8, amendment 3).
func (g *growIndex) flatSearch(ctx context.Context, vector []float32, k, rescore int, allow helpers.AllowList,
	stats *diskSearchStats,
) ([]uint64, []float32, error) {
	if k < 1 {
		return nil, nil, errors.Errorf("k must be positive, got %d", k)
	}
	if len(vector) != g.meta.Dims {
		return nil, nil, errors.Wrapf(common.ErrWrongDimensions,
			"query has length %d, the index holds vectors of length %d", len(vector), g.meta.Dims)
	}
	if err := checkValues(vector); err != nil {
		return nil, nil, errors.Wrap(err, "query")
	}
	if g.noCodes {
		return nil, nil, errors.Wrapf(ErrNotSupported,
			"a filter that allows %d vectors, under the cutoff of the flat search, with no codes in memory (rq.cacheMode %q)",
			allow.Len(), ent.CacheModeNone)
	}
	if stats == nil {
		stats = &diskSearchStats{}
	}
	stats.Flat = true
	query := g.ix.normalized(vector)

	it := allow.Iterator()
	defer it.Stop()
	ids := make([]uint32, 0, min(allow.Len(), len(g.live)))
	for id, ok := it.Next(); ok; id, ok = it.Next() {
		if id >= uint64(len(g.live)) || !g.live[id] || g.deleted != nil && g.deleted[id] {
			continue
		}
		ids = append(ids, uint32(id))
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	var candidates []scored
	if len(ids) <= rescore {
		candidates = make([]scored, len(ids))
		for i, id := range ids {
			candidates[i] = scored{id: uint64(id)}
		}
	} else {
		estimator := g.st.quantizer.NewEstimator(query)
		var best topK
		best.reset(rescore)
		dists := make([]float32, len(ids))
		if err := g.st.side.score(estimator, ids, dists); err != nil {
			return nil, nil, err
		}
		for i, id := range ids {
			best.push(uint64(id), dists[i])
		}
		stats.Scored = len(ids)
		candidates = best.sorted()
	}

	asked := len(candidates)
	candidates, err := g.ix.rescore(ctx, query, candidates)
	if err != nil {
		return nil, nil, err
	}
	stats.Rescored = asked
	var result topK
	result.reset(k)
	for _, cand := range candidates {
		result.push(cand.id, cand.dist)
	}
	return topResults(result.sorted(), k)
}
