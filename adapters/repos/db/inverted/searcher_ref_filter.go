//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
)

// a helper tool to extract the uuid beacon for any matching reference
type refFilterExtractor struct {
	logger        logrus.FieldLogger
	filter        *filters.Clause
	className     schema.ClassName
	classSearcher ClassSearcher
	schema        schema.Schema
}

// ClassSearcher is anything that allows a root-level ClassSearch
type ClassSearcher interface {
	ClassSearch(ctx context.Context,
		params dto.GetParams) ([]search.Result, error)
	GetQueryMaximumResults() int
}

func newRefFilterExtractor(logger logrus.FieldLogger,
	classSearcher ClassSearcher, filter *filters.Clause,
	className schema.ClassName, schema schema.Schema,
) *refFilterExtractor {
	return &refFilterExtractor{
		logger:        logger,
		filter:        filter,
		className:     className,
		classSearcher: classSearcher,
		schema:        schema,
	}
}

func (r *refFilterExtractor) Do(ctx context.Context) (*propValuePair, error) {
	if err := r.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid usage")
	}

	ids, err := r.fetchIDs(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "nested request to fetch matching IDs")
	}

	if len(ids) > r.classSearcher.GetQueryMaximumResults() {
		r.logger.
			WithField("nested_reference_results", len(ids)).
			WithField("query_maximum_results", r.classSearcher.GetQueryMaximumResults()).
			Warnf("Number of found nested reference results exceeds configured QUERY_MAXIMUM_RESULTS. " +
				"This may result in search performance degradation or even out of memory errors.")
	}

	return r.resultsToPropValuePairs(ids)
}

func (r *refFilterExtractor) paramsForNestedRequest() (dto.GetParams, error) {
	return dto.GetParams{
		Filters:   r.innerFilter(),
		ClassName: r.filter.On.Child.Class.String(),
		Pagination: &filters.Pagination{
			// The limit is chosen arbitrarily, it used to be 1e4 in the ES-based
			// implementation, so using a 10x as high value should be safe. However,
			// we might come back to reduce this number in case this leads to
			// unexpected performance issues
			// Limit: int(config.DefaultQueryMaximumResults),

			// due to reported issue https://github.com/weaviate/weaviate/issues/2537
			// ref search is temporarily (until better solution) effectively unlimited
			Offset: 0,
			Limit:  math.MaxInt,
		},
		// set this to indicate that this is a sub-query, so we do not need
		// to perform the same search limits cutoff check that we do with
		// the root query
		AdditionalProperties: additional.Properties{ReferenceQuery: true},
	}, nil
}

func (r *refFilterExtractor) innerFilter() *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: r.filter.Operator,
			On:       r.filter.On.Child,
			Value:    r.filter.Value,
		},
	}
}

type classUUIDPair struct {
	class string
	id    strfmt.UUID
}

func (r *refFilterExtractor) fetchIDs(ctx context.Context) ([]classUUIDPair, error) {
	params, err := r.paramsForNestedRequest()
	if err != nil {
		return nil, err
	}

	res, err := r.classSearcher.ClassSearch(ctx, params)
	if err != nil {
		return nil, err
	}

	out := make([]classUUIDPair, len(res))
	for i, elem := range res {
		out[i] = classUUIDPair{class: elem.ClassName, id: elem.ID}
	}

	return out, nil
}

func (r *refFilterExtractor) resultsToPropValuePairs(ids []classUUIDPair,
) (*propValuePair, error) {
	switch len(ids) {
	case 0:
		return r.emptyPropValuePair(), nil
	case 1:
		return r.backwardCompatibleIDToPropValuePair(ids[0])
	default:
		return r.chainedIDsToPropValuePair(ids)
	}
}

func (r *refFilterExtractor) emptyPropValuePair() *propValuePair {
	return &propValuePair{
		prop:         lowercaseFirstLetter(r.filter.On.Property.String()),
		hasFrequency: false,
		value:        nil,
		operator:     filters.OperatorEqual,
	}
}

// Because we still support the old beacon format that did not include the
// class yet, we cannot be sure about which format we will find in the
// database. Typically we would now build a filter, such as value==beacon.
// However, this beacon would either have the old format or the new format.
// Depending on which format was used during importing, one would match and the
// other wouldn't.
//
// As a workaround we can use an OR filter to allow both, such as
// ( value==beacon_old_format OR value==beacon_new_format )
func (r *refFilterExtractor) backwardCompatibleIDToPropValuePair(p classUUIDPair) (*propValuePair, error) {
	// this workaround is already implemented in the chained ID case, so we can
	// simply pass it through:
	return r.chainedIDsToPropValuePair([]classUUIDPair{p})
}

func (r *refFilterExtractor) idToPropValuePairWithValue(v []byte) (*propValuePair, error) {
	return &propValuePair{
		prop:         lowercaseFirstLetter(r.filter.On.Property.String()),
		hasFrequency: false,
		value:        v,
		operator:     filters.OperatorEqual,
	}, nil
}

// chain multiple alternatives using an OR operator
func (r *refFilterExtractor) chainedIDsToPropValuePair(ids []classUUIDPair) (*propValuePair, error) {
	children, err := r.idsToPropValuePairs(ids)
	if err != nil {
		return nil, err
	}

	return &propValuePair{
		prop:         lowercaseFirstLetter(r.filter.On.Property.String()),
		hasFrequency: false,
		operator:     filters.OperatorOr,
		children:     children,
	}, nil
}

// Use both new format with class name in the beacon, as well as the old
// format. Since the results will be OR'ed anyway, this is safe todo.
//
// The additional lookups and OR-merge operations have a cost, therefore this
// backward-compatible logic should be removed, as soon as we can be sure that
// no more class-less beacons exist. Most likely this will be the case with the
// next breaking change, such as v2.0.0.
func (r *refFilterExtractor) idsToPropValuePairs(ids []classUUIDPair) ([]*propValuePair, error) {
	// This makes it safe to access the first element later on without further
	// checks
	if len(ids) == 0 {
		return nil, nil
	}

	out := make([]*propValuePair, len(ids)*2)
	bb := crossref.NewBulkBuilderWithEstimates(len(ids)*2, ids[0].class, 1.25)
	for i, id := range ids {
		// future-proof way
		pv, err := r.idToPropValuePairWithValue(bb.ClassAndID(id.class, id.id))
		if err != nil {
			return nil, err
		}

		out[i*2] = pv

		// backward-compatible way
		pv, err = r.idToPropValuePairWithValue(bb.LegacyIDOnly(id.id))
		if err != nil {
			return nil, err
		}

		out[(i*2)+1] = pv
	}

	return out, nil
}

func (r *refFilterExtractor) validate() error {
	if len(r.filter.On.Slice())%2 != 1 {
		return fmt.Errorf("path must have an odd number of segments")
	}

	return nil
}

func lowercaseFirstLetter(in string) string {
	switch len(in) {
	case 0:
		return in
	case 1:
		return strings.ToLower(in)
	default:
		return strings.ToLower(in[:1]) + in[1:]
	}
}
