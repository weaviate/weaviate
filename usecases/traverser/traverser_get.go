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

package traverser

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/tracing"
)

func (t *Traverser) GetClass(ctx context.Context, principal *models.Principal,
	params dto.GetParams,
) ([]interface{}, error) {
	// Seed trace flags here — the only layer with config access; everything below reads them off ctx.
	ctx = tracing.WithFlags(ctx, buildTraceFlags(t.config.Config, params.AdditionalProperties.TraceSpans))
	ctx, span := tracing.StartRootSpan(ctx, "weaviate.traverser.GetClass",
		trace.WithAttributes(attribute.String("weaviate.collection", params.ClassName)))
	defer span.End()

	before := time.Now()

	ok := t.ratelimiter.TryInc()
	if !ok {
		// we currently have no concept of error status code or typed errors in
		// GraphQL, so there is no other way then to send a message containing what
		// we want to convey
		return nil, enterrors.NewErrRateLimit()
	}

	defer t.ratelimiter.Dec()

	t.metrics.QueriesGetInc(params.ClassName)
	defer t.metrics.QueriesGetDec(params.ClassName)
	defer t.metrics.QueriesObserveDuration(params.ClassName, before.UnixMilli())

	if err := t.probeForRefDepthLimit(params.Properties); err != nil {
		return nil, err
	}

	// validate here, because filters can contain references that need to be authorized
	if err := t.validateFilters(ctx, principal, params.Filters); err != nil {
		return nil, errors.Wrap(err, "invalid 'where' filter")
	}

	certainty := ExtractCertaintyFromParams(params)
	if certainty != 0 || params.AdditionalProperties.Certainty {
		// if certainty is provided as input, we must ensure
		// that the vector index is configured to use cosine
		// distance
		if err := t.validateGetDistanceParams(params); err != nil {
			return nil, err
		}
	}

	return t.explorer.GetClass(ctx, params)
}

// buildTraceFlags maps the live per-area runtime toggles into request trace
// flags. traceSpans is the per-request override: when set it forces every area
// on for this request, ORed over the runtime toggles (so a request can be traced
// even while all runtime toggles are off). DynamicValue.Get is nil-safe.
func buildTraceFlags(cfg config.Config, traceSpans bool) tracing.Flags {
	flags := tracing.NewFlags(map[tracing.Area]bool{
		tracing.AreaVector: cfg.TraceVectorSearch.Get(),
		tracing.AreaBM25:   cfg.TraceBM25Search.Get(),
		tracing.AreaHybrid: cfg.TraceHybridSearch.Get(),
	})
	if traceSpans {
		flags = tracing.WithForceAll(flags)
	}
	return flags
}

// probeForRefDepthLimit checks to ensure reference nesting depth doesn't exceed the limit
// provided by QUERY_CROSS_REFERENCE_DEPTH_LIMIT
func (t *Traverser) probeForRefDepthLimit(props search.SelectProperties) error {
	var (
		determineDepth func(props search.SelectProperties, currDepth int) int
		depthLimit     = t.config.Config.QueryCrossReferenceDepthLimit
	)

	determineDepth = func(props search.SelectProperties, currDepth int) int {
		if len(props) == 0 || currDepth > depthLimit {
			return 0
		}

		currDepth++
		maxDepth := 0
		for _, prop := range props {
			for _, refTarget := range prop.Refs {
				maxDepth = max(maxDepth, determineDepth(refTarget.RefProperties, currDepth))
			}
		}

		return maxDepth + 1
	}

	depth := determineDepth(props, 0)
	if depth > depthLimit {
		return fmt.Errorf("nested references depth exceeds QUERY_CROSS_REFERENCE_DEPTH_LIMIT (%d)", depthLimit)
	}
	return nil
}

func (t *Traverser) validateFilters(ctx context.Context, principal *models.Principal, filter *filters.LocalFilter) error {
	if filter == nil {
		return nil
	}

	f := func(name string) (*models.Class, error) {
		err := t.authorizer.Authorize(ctx, principal, authorization.READ, authorization.CollectionsMetadata(name)...)
		if err != nil {
			return nil, err
		}

		return t.schemaGetter.ReadOnlyClass(name), nil
	}

	return filters.ValidateFilters(f, filter)
}
