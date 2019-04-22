/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package kinds

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/entities/filters"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
)

// LocalAggregate resolves meta queries
func (t *Traverser) LocalAggregate(ctx context.Context, params *AggregateParams) (interface{}, error) {
	unlock, err := t.locks.LockConnector()
	if err != nil {
		return nil, fmt.Errorf("could not acquire lock: %v", err)
	}
	defer unlock()

	return t.repo.LocalAggregate(ctx, params)
}

// AggregateParams to describe the Local->GetMeta->Kind->Class query. Will be passed to
// the individual connector methods responsible for resolving the GetMeta
// query.
type AggregateParams struct {
	Kind       kind.Kind
	Filters    *filters.LocalFilter
	Analytics  filters.AnalyticsProps
	ClassName  schema.ClassName
	Properties []AggregateProperty
	GroupBy    *filters.Path
}

// Aggregator is the desired computation that the database connector
// should perform on this property
type Aggregator string

const (
	// CountAggregator the occurence of this property
	CountAggregator Aggregator = "count"

	// SumAggregator of all the values of the prop (i.e. sum of all Ints or Numbers)
	SumAggregator Aggregator = "sum"

	// MeanAggregator calculates the mean of an Int or Number
	MeanAggregator Aggregator = "mean"

	// ModeAggregator calculates the mode (most occurring value) of an Int or Number
	ModeAggregator Aggregator = "mode"

	// MedianAggregator calculates the median (most occurring value) of an Int or Number
	MedianAggregator Aggregator = "median"

	// MaximumAggregator selects the maximum value of an Int or Number
	MaximumAggregator Aggregator = "maximum"

	// MinimumAggregator selects the maximum value of an Int or Number
	MinimumAggregator Aggregator = "minimum"
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
// will create a different hash. Chaning anayltics-meta information, such as
// 'forceRecalculate' however, will not change the hash. Doing so would prevent
// us from ever retrieving a cached result that wass generated with the
// 'forceRecalculate' option on.
func (p AggregateParams) AnalyticsHash() (string, error) {

	// make sure to copy the params, so that we don't accidentaly mutate the
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
	case string(MeanAggregator):
		return MeanAggregator, nil
	case string(MedianAggregator):
		return MedianAggregator, nil
	case string(ModeAggregator):
		return ModeAggregator, nil
	case string(MaximumAggregator):
		return MaximumAggregator, nil
	case string(MinimumAggregator):
		return MinimumAggregator, nil
	case string(CountAggregator):
		return CountAggregator, nil
	case string(SumAggregator):
		return SumAggregator, nil
	default:
		return "", fmt.Errorf("unrecognized aggregator prop '%s'", name)
	}
}
