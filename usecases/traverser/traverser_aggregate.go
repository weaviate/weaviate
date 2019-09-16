//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
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

// LocalAggregate resolves meta queries
func (t *Traverser) LocalAggregate(ctx context.Context, principal *models.Principal,
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

// Aggreators used in every prop
const (
	CountAggregator Aggregator = "count"
	TypeAggregator  Aggregator = "type"
)

// Aggregators used in numerical props
const (
	SumAggregator     Aggregator = "sum"
	MeanAggregator    Aggregator = "mean"
	ModeAggregator    Aggregator = "mode"
	MedianAggregator  Aggregator = "median"
	MaximumAggregator Aggregator = "maximum"
	MinimumAggregator Aggregator = "minimum"
)

// Aggregators used in boolean props
const (
	TotalTrueAggregator       Aggregator = "totalTrue"
	PercentageTrueAggregator  Aggregator = "percentageTrue"
	TotalFalseAggregator      Aggregator = "totalFalse"
	PercentageFalseAggregator Aggregator = "percentageFalse"
)

// Aggregators used in string props
const (
	TopOccurrencesAggregator Aggregator = "topOccurrences"
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

	// common
	case string(CountAggregator):
		return CountAggregator, nil

	// numerical
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
	case string(SumAggregator):
		return SumAggregator, nil

	// boolean
	case string(TotalTrueAggregator):
		return TotalTrueAggregator, nil
	case string(TotalFalseAggregator):
		return TotalFalseAggregator, nil
	case string(PercentageTrueAggregator):
		return PercentageTrueAggregator, nil
	case string(PercentageFalseAggregator):
		return PercentageFalseAggregator, nil

	// string/text
	case string(TopOccurrencesAggregator):
		return TopOccurrencesAggregator, nil

	default:
		return "", fmt.Errorf("unrecognized aggregator prop '%s'", name)
	}
}
