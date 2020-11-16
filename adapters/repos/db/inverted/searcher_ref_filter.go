package inverted

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// a helper tool to extract the uuid beacon for any matching reference
type refFilterExtractor struct {
	filter        *filters.Clause
	className     schema.ClassName
	classSearcher ClassSearcher
}

// ClassSearcher is anything that allows a root-level ClassSearch
type ClassSearcher interface {
	ClassSearch(ctx context.Context,
		params traverser.GetParams) ([]search.Result, error)
}

func newRefFilterExtractor(classSearcher ClassSearcher,
	filter *filters.Clause, className schema.ClassName) *refFilterExtractor {
	return &refFilterExtractor{
		filter:        filter,
		className:     className,
		classSearcher: classSearcher,
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

func (r *refFilterExtractor) paramsForNestedRequest() traverser.GetParams {
	return traverser.GetParams{
		Filters:   r.innerFilter(),
		ClassName: r.filter.On.Class.String(),
		Pagination: &filters.Pagination{
			// The limit is chosen arbitrarily, it used to be 1e4 in the ES-based
			// implementation, so using a 10x as high value should be safe. However,
			// we might come back to reduce this number in case this leads to
			// unexpected performance issues
			Limit: 1e5,
		},
		Kind: kind.Thing, // TODO: discovery dynamically
	}
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

func (r *refFilterExtractor) fetchIDs(ctx context.Context) ([]strfmt.UUID, error) {
	res, err := r.classSearcher.ClassSearch(ctx, r.paramsForNestedRequest())
	if err != nil {
		return nil, err
	}

	out := make([]strfmt.UUID, len(res))
	for i, elem := range res {
		out[i] = elem.ID
	}

	return out, nil
}

func (r *refFilterExtractor) resultsToPropValuePairs(ids []strfmt.UUID,
) (*propValuePair, error) {
	switch len(ids) {
	case 0:
		return r.emptyPropValuePair(), nil
	case 1:
		return r.idToPropValuePair(ids[0]), nil
	default:
		return r.chainedIDsToPropValuePair(ids), nil
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

func (r *refFilterExtractor) idToPropValuePair(id strfmt.UUID) *propValuePair {
	return &propValuePair{
		prop:         lowercaseFirstLetter(r.filter.On.Property.String()),
		hasFrequency: false,
		value:        []byte(id),
		operator:     filters.OperatorEqual,
	}
}

// chain multiple alternatives using an OR operator
func (r *refFilterExtractor) chainedIDsToPropValuePair(ids []strfmt.UUID) *propValuePair {
	return &propValuePair{
		prop:         lowercaseFirstLetter(r.filter.On.Property.String()),
		hasFrequency: false,
		operator:     filters.OperatorOr,
		children:     r.idsToPropValuePairs(ids),
	}
}

func (r *refFilterExtractor) idsToPropValuePairs(ids []strfmt.UUID) []*propValuePair {
	out := make([]*propValuePair, len(ids))
	for i, id := range ids {
		out[i] = r.idToPropValuePair(id)
	}

	return out
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
