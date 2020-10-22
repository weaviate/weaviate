//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Aggregate resolves meta queries
func (t *Traverser) Aggregate(ctx context.Context, principal *models.Principal,
	params *AggregateParams) (interface{}, error) {
	err := t.authorizer.Authorize(principal, "get", "traversal/*")
	if err != nil {
		return nil, err
	}

	unlock, err := t.locks.LockConnector()
	if err != nil {
		return nil, fmt.Errorf("could not acquire lock: %v", err)
	}
	defer unlock()

	inspector := newTypeInspector(t.schemaGetter)

	res, err := t.vectorSearcher.Aggregate(ctx, *params)
	if err != nil {
		return nil, err
	}

	return inspector.WithTypes(res, *params)
}

// AggregateParams to describe the Local->Meta->Kind->Class query. Will be passed to
// the individual connector methods responsible for resolving the Meta
// query.
type AggregateParams struct {
	Kind             kind.Kind
	Filters          *filters.LocalFilter
	Analytics        filters.AnalyticsProps
	ClassName        schema.ClassName
	Properties       []AggregateProperty
	GroupBy          *filters.Path
	IncludeMetaCount bool
	Limit            *int
}

// Aggregator is the desired computation that the database connector
// should perform on this property
type Aggregator struct {
	Type  string
	Limit *int // used on TopOccurrence Agg
}

func (a Aggregator) String() string {
	return a.Type
}

// Aggreators used in every prop
var (
	CountAggregator = Aggregator{Type: "count"}
	TypeAggregator  = Aggregator{Type: "type"}
)

// Aggregators used in numerical props
var (
	SumAggregator     = Aggregator{Type: "sum"}
	MeanAggregator    = Aggregator{Type: "mean"}
	ModeAggregator    = Aggregator{Type: "mode"}
	MedianAggregator  = Aggregator{Type: "median"}
	MaximumAggregator = Aggregator{Type: "maximum"}
	MinimumAggregator = Aggregator{Type: "minimum"}
)

// Aggregators used in boolean props
var (
	TotalTrueAggregator       = Aggregator{Type: "totalTrue"}
	PercentageTrueAggregator  = Aggregator{Type: "percentageTrue"}
	TotalFalseAggregator      = Aggregator{Type: "totalFalse"}
	PercentageFalseAggregator = Aggregator{Type: "percentageFalse"}
)

const TopOccurrencesType = "topOccurrences"

// NewTopOccurrencesAggregator creates a TopOccurrencesAggregator, we cannot
// use a singleton for this as the desired limit can be different each time
func NewTopOccurrencesAggregator(limit *int) Aggregator {
	return Aggregator{Type: TopOccurrencesType, Limit: limit}
}

// Aggregators used in ref props
var (
	PointingToAggregator = Aggregator{Type: "pointingTo"}
)

// AggregateProperty is any property of a class that we want to retrieve meta
// information about
type AggregateProperty struct {
	Name        schema.PropertyName
	Aggregators []Aggregator
}

// AnalyticsHash is a special hash for use with an external analytics engine
// which has caching capabilities. Anything that would produce a different
// result, such as new or different properties or different analytics props
// will create a different hash. Chaning analytics-meta information, such as
// 'forceRecalculate' however, will not change the hash. Doing so would prevent
// us from ever retrieving a cached result that was generated with the
// 'forceRecalculate' option on.
func (p AggregateParams) AnalyticsHash() (string, error) {
	// make sure to copy the params, so that we don't accidentally mutate the
	// original
	params := p
	// always override analytical props to make sure they don't influence the
	// hash
	params.Analytics = filters.AnalyticsProps{}

	return params.md5()
}

func (p AggregateParams) md5() (string, error) {
	paramBytes, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("couldnt convert params to json before hashing: %s", err)
	}

	hash := md5.New()
	fmt.Fprintf(hash, "%s", paramBytes)
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func ParseAggregatorProp(name string) (Aggregator, error) {
	switch name {
	// common
	case CountAggregator.String():
		return CountAggregator, nil
	case TypeAggregator.String():
		return TypeAggregator, nil

	// numerical
	case MeanAggregator.String():
		return MeanAggregator, nil
	case MedianAggregator.String():
		return MedianAggregator, nil
	case ModeAggregator.String():
		return ModeAggregator, nil
	case MaximumAggregator.String():
		return MaximumAggregator, nil
	case MinimumAggregator.String():
		return MinimumAggregator, nil
	case SumAggregator.String():
		return SumAggregator, nil

	// boolean
	case TotalTrueAggregator.String():
		return TotalTrueAggregator, nil
	case TotalFalseAggregator.String():
		return TotalFalseAggregator, nil
	case PercentageTrueAggregator.String():
		return PercentageTrueAggregator, nil
	case PercentageFalseAggregator.String():
		return PercentageFalseAggregator, nil

	// string/text
	case TopOccurrencesType:
		return NewTopOccurrencesAggregator(ptInt(5)), nil // default to limit 5, can be overwritten

	// ref
	case PointingToAggregator.String():
		return PointingToAggregator, nil

	default:
		return Aggregator{}, fmt.Errorf("unrecognized aggregator prop '%s'", name)
	}
}

func ptInt(in int) *int {
	return &in
}
