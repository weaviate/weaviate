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

package inverted

import (
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/tokenizer"
)

// extractNestedProp builds a propValuePair for a dot-notation nested property
// filter such as "addresses.city = 'Berlin'". It walks the schema to find the
// leaf NestedProperty, encodes the filter value, and sets isNested=true so
// the execution path uses the nested bucket and strips positions to docIDs.
func (s *Searcher) extractNestedProp(filter *filters.Clause, path string,
	prop *models.Property, class *models.Class,
) (*propValuePair, error) {
	if filter.Operator == filters.OperatorIsNull {
		return nil, fmt.Errorf("IsNull is not yet supported for nested properties")
	}

	segments := strings.Split(path, ".")
	// segments[0] is the top-level prop already fetched; walk segments[1:] to the leaf.
	leaf, err := findNestedLeaf(segments[1:], prop.NestedProperties)
	if err != nil {
		return nil, fmt.Errorf("nested path %q: %w", path, err)
	}

	if !isNestedFilterable(leaf) {
		return nil, fmt.Errorf("nested property %q is not filterable", path)
	}

	return s.buildNestedFilterPair(filter, path, segments[0], leaf, class)
}

// findNestedLeaf walks segments through nestedProps to locate the leaf
// NestedProperty. Returns an error if any segment is not found.
func findNestedLeaf(segments []string, props []*models.NestedProperty) (*models.NestedProperty, error) {
	for i, seg := range segments {
		var found *models.NestedProperty
		for _, np := range props {
			if np.Name == seg {
				found = np
				break
			}
		}
		if found == nil {
			return nil, fmt.Errorf("sub-property %q not found", seg)
		}
		if i == len(segments)-1 {
			return found, nil
		}
		props = found.NestedProperties
	}
	return nil, fmt.Errorf("empty path")
}

// buildNestedFilterPair encodes the filter value for the given leaf type and
// returns the corresponding propValuePair(s).
func (s *Searcher) buildNestedFilterPair(filter *filters.Clause, path, propName string,
	leaf *models.NestedProperty, class *models.Class,
) (*propValuePair, error) {
	// Keys in the nested bucket use only the path relative to the top-level
	// property (e.g. "city", "owner.firstname"), not the full filter path
	// (e.g. "event.city"). Strip the top-level property name.
	relativePath := strings.TrimPrefix(path, propName+".")
	keyPrefix := nested.PathPrefix(relativePath)
	dt := schema.DataType(leaf.DataType[0])
	switch dt {
	case schema.DataTypeText, schema.DataTypeTextArray:
		return s.buildNestedTextFilterPair(filter, path, propName, leaf.Tokenization, keyPrefix, class)
	default:
		return s.buildNestedPrimitiveFilterPair(filter, path, propName, dt, keyPrefix, class)
	}
}

// buildNestedTextFilterPair handles tokenizable text properties. Multiple
// tokens produce multiple propValuePairs combined with AND, mirroring
// extractTokenizableProp for flat properties.
func (s *Searcher) buildNestedTextFilterPair(filter *filters.Clause, path, propName, tokenization string,
	keyPrefix []byte, class *models.Class,
) (*propValuePair, error) {
	valueStr, ok := filter.Value.Value.(string)
	if !ok {
		return nil, fmt.Errorf("nested path %q: expected string value, got %T", path, filter.Value.Value)
	}

	var terms []string
	if filter.Operator == filters.OperatorLike {
		terms = tokenizer.TokenizeWithWildcardsForClass(tokenization, valueStr, class.Class)
	} else {
		terms = tokenizer.TokenizeForClass(tokenization, valueStr, class.Class)
	}

	pvps := make([]*propValuePair, 0, len(terms))
	for _, term := range terms {
		if s.stopwords.IsStopword(term) {
			continue
		}
		pvps = append(pvps, &propValuePair{
			prop:               propName,
			value:              []byte(term),
			nestedKeyPrefix:    keyPrefix,
			operator:           filter.Operator,
			hasFilterableIndex: true,
			isNested:           true,
			Class:              class,
		})
	}

	if len(pvps) == 0 {
		return nil, ErrOnlyStopwords
	}
	if len(pvps) == 1 {
		return pvps[0], nil
	}
	return &propValuePair{operator: filters.OperatorAnd, children: pvps, Class: class}, nil
}

// buildNestedPrimitiveFilterPair handles non-text primitive types (int,
// number, bool, date and their array variants).
func (s *Searcher) buildNestedPrimitiveFilterPair(filter *filters.Clause, path, propName string,
	dt schema.DataType, keyPrefix []byte, class *models.Class,
) (*propValuePair, error) {
	// Map array types to their scalar equivalent — each array element is stored
	// individually in the nested bucket, so filtering works the same way.
	scalar := dt
	if base, ok := schema.IsArrayType(dt); ok {
		scalar = base
	}

	var (
		encodedValue []byte
		err          error
	)
	switch scalar {
	case schema.DataTypeInt:
		encodedValue, err = s.extractIntValue(filter.Value.Value)
	case schema.DataTypeNumber:
		encodedValue, err = s.extractNumberValue(filter.Value.Value)
	case schema.DataTypeBoolean:
		encodedValue, err = s.extractBoolValue(filter.Value.Value)
	case schema.DataTypeDate:
		encodedValue, err = s.extractDateValue(filter.Value.Value)
	default:
		return nil, fmt.Errorf("nested path %q: unsupported leaf type %q", path, dt)
	}
	if err != nil {
		return nil, fmt.Errorf("nested path %q: encode value: %w", path, err)
	}

	return &propValuePair{
		prop:               propName,
		value:              encodedValue,
		nestedKeyPrefix:    keyPrefix,
		operator:           filter.Operator,
		hasFilterableIndex: true,
		isNested:           true,
		Class:              class,
	}, nil
}
