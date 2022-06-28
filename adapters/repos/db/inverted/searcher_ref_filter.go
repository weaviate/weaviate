//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// a helper tool to extract the uuid beacon for any matching reference
type refFilterExtractor struct {
	filter        *filters.Clause
	className     schema.ClassName
	classSearcher ClassSearcher
	schema        schema.Schema
}

// ClassSearcher is anything that allows a root-level ClassSearch
type ClassSearcher interface {
	ClassSearch(ctx context.Context,
		params traverser.GetParams) ([]search.Result, error)
	GetQueryMaximumResults() int
}

func newRefFilterExtractor(classSearcher ClassSearcher,
	filter *filters.Clause, className schema.ClassName,
	schema schema.Schema) *refFilterExtractor {
	return &refFilterExtractor{
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

	return r.resultsToPropValuePairs(ids)
}

func (r *refFilterExtractor) paramsForNestedRequest() (traverser.GetParams, error) {
	return traverser.GetParams{
		Filters:   r.innerFilter(),
		ClassName: r.filter.On.Child.Class.String(),
		Pagination: &filters.Pagination{
			// The limit is chosen arbitrarily, it used to be 1e4 in the ES-based
			// implementation, so using a 10x as high value should be safe. However,
			// we might come back to reduce this number in case this leads to
			// unexpected performance issues
			Limit: int(config.DefaultQueryMaximumResults),
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
		return r.idToPropValuePair(ids[0])
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

func (r *refFilterExtractor) idToPropValuePair(p classUUIDPair) (*propValuePair, error) {
	return &propValuePair{
		prop:         lowercaseFirstLetter(r.filter.On.Property.String()),
		hasFrequency: false,
		value:        []byte(crossref.New("localhost", p.class, p.id).String()),
		operator:     filters.OperatorEqual,
	}, nil
}

// func (r *refFilterExtractor) beacon(id strfmt.UUID) (strfmt.URI, error) {
// 	return strfmt.URI(crossref.New("localhost", id).String()), nil
// }

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

func (r *refFilterExtractor) idsToPropValuePairs(ids []classUUIDPair) ([]*propValuePair, error) {
	out := make([]*propValuePair, len(ids))
	for i, id := range ids {
		pv, err := r.idToPropValuePair(id)
		if err != nil {
			return nil, err
		}

		out[i] = pv
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
