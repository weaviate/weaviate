//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package aggregate

import (
	"context"
	"fmt"

	analytics "github.com/SeMI-network/janus-spark-analytics/clients/go"
	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (p *Processor) getResultFromAnalyticsEngine(ctx context.Context, query *gremlin.Query,
	params *traverser.AggregateParams) ([]interface{}, error) {
	hash, err := params.AnalyticsHash()
	if params.Analytics.ForceRecalculate {
		// no need to even check the cache, the user wants to start a new job
		// regardless of the cache state
		return p.triggerNewAnalyticsJob(ctx, query, hash)
	}

	cachedResult, err := p.cache.Get(ctx, keyFromHash(hash))
	if err != nil {
		return nil, fmt.Errorf("could not check whether cached query result is present: %v", err)
	}

	if len(cachedResult.Kvs) > 0 {
		return p.cachedAnalyticsResult(cachedResult.Kvs[0].Value)
	}

	return p.triggerNewAnalyticsJob(ctx, query, hash)
}

func (p *Processor) cachedAnalyticsResult(res []byte) ([]interface{}, error) {
	parsed, err := analytics.ParseResult(res)
	if err != nil {
		return nil, fmt.Errorf("found a cached result, but couldn't parse it: %v", err)
	}

	switch parsed.Status {
	case analytics.StatusSucceeded:
		return parsed.Result.([]interface{}), nil

	case analytics.StatusInProgress:
		return nil, fmt.Errorf("an analysis job matching your query is already running with id '%s'. "+
			"However, it hasn't finished yet. Please check back later.", parsed.ID)

	case analytics.StatusFailed:
		return nil, fmt.Errorf("the previous analyis job matching your query with id '%s' failed. "+
			"To try again, set 'forceRecalculate' to 'true', the error message "+
			"from the previous failure was: %v", parsed.ID, parsed.Result)

	default:
		return nil, fmt.Errorf("unknown status '%s'", parsed.Status)
	}
}

func (p *Processor) triggerNewAnalyticsJob(ctx context.Context, query *gremlin.Query, hash string) ([]interface{}, error) {
	err := p.analytics.Schedule(ctx, analytics.QueryParams{Query: query.String(), ID: hash})
	if err != nil {
		return nil, fmt.Errorf("could not trigger new analytics job against the analytics api: %v", err)
	}

	// The following error is the happy path once we got here. It really is just
	// a mesasge to the user that a new job has been triggered and that they can
	// check back later to check whether it's completed. This is because from the
	// perspective of the graphql api only serving a result is success.
	// Everything else - even though in this case totally expected - must be an
	// error so we can return this information back to the user
	return nil, fmt.Errorf("new job started - check back later: "+
		"the requested analytics request could not be served from cache, so a new analytics job "+
		"was triggered. This job runs in the background and can take considerable time depending "+
		"on the size of your graph. Please check back later. The id of your analysis job is '%s'.",
		hash)
}
