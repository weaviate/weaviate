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

	"github.com/weaviate/weaviate/entities/filters"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/tokenizer"
)

// extractNestedProp builds a propValuePair for a dot-notation nested property
// filter such as "addresses.city = 'Berlin'" or "addresses[1].city = 'Berlin'".
// It walks the schema to find the leaf NestedProperty, encodes the filter value,
// and sets isNested=true so the execution path uses the nested bucket and strips
// positions to docIDs. Any arr[N] index markers are parsed into arrayIndices.
func (s *Searcher) extractNestedProp(filter *filters.Clause, path string,
	prop *models.Property, class *models.Class,
) (*propValuePair, error) {
	cleanRelPath, cleanRelSegs, arrayIndices := filnested.ParseIndexedPath(path)

	if filter.Operator == filters.OperatorIsNull {
		return s.buildNestedIsNullPair(filter, prop.Name, cleanRelPath, arrayIndices, class)
	}

	leaf, err := findNestedLeaf(cleanRelSegs, prop.NestedProperties)
	if err != nil {
		return nil, fmt.Errorf("nested path %q: %w", path, err)
	}

	// TODO aliszka:nested_filtering when rangeable / searchable nested
	// filtering support is added, add corresponding schema.IsNestedRangeable
	// and schema.IsNestedSearchable checks alongside the filterable check
	// below. The validator currently rejects non-filterable leaves at parse
	// time; the check below stays as a defensive safeguard for any code path
	// that bypasses validation.
	if !schema.IsNestedFilterable(leaf) {
		return nil, fmt.Errorf("nested property %q is not filterable", path)
	}

	return s.buildNestedFilterPair(filter, prop.Name, path, cleanRelPath, arrayIndices, leaf, class)
}

// findNestedLeaf walks segments through nestedProps to locate the leaf
// NestedProperty. Returns an error if any segment is not found.
func findNestedLeaf(segments []string, props []*models.NestedProperty) (*models.NestedProperty, error) {
	for i, seg := range segments {
		found := filnested.FindNestedProp(props, seg)
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
func (s *Searcher) buildNestedFilterPair(filter *filters.Clause, propName, fullPath, relPath string,
	arrayIndices []filnested.ArrayIndex, leaf *models.NestedProperty, class *models.Class,
) (*propValuePair, error) {
	dt := schema.DataType(leaf.DataType[0])
	switch dt {
	case schema.DataTypeText, schema.DataTypeTextArray:
		return s.buildNestedTextFilterPair(filter, propName, fullPath, relPath, leaf, arrayIndices, class)
	default:
		return s.buildNestedPrimitiveFilterPair(filter, propName, fullPath, relPath, dt, arrayIndices, class)
	}
}

// buildNestedTextFilterPair handles tokenizable text properties. Multiple
// tokens produce multiple propValuePairs combined with AND, mirroring
// extractTokenizableProp for flat properties.
//
// TODO aliszka:nested_filtering the tokenization pipeline here (stopwords,
// ASCII folding, wildcard handling) was aligned with extractTokenizableProp
// manually. Consider extracting a shared helper to avoid future divergence.
func (s *Searcher) buildNestedTextFilterPair(filter *filters.Clause, propName, fullPath, relPath string,
	leaf *models.NestedProperty, arrayIndices []filnested.ArrayIndex, class *models.Class,
) (*propValuePair, error) {
	valueStr, ok := filter.Value.Value.(string)
	if !ok {
		return nil, fmt.Errorf("nested path %q: expected string value, got %T", fullPath, filter.Value.Value)
	}

	var terms []string
	if filter.Operator == filters.OperatorLike {
		text := valueStr
		if leaf.TextAnalyzer != nil && leaf.TextAnalyzer.ASCIIFold {
			ignore := tokenizer.BuildIgnoreSet(leaf.TextAnalyzer.ASCIIFoldIgnore)
			text = tokenizer.FoldASCII(text, ignore)
		}
		terms = tokenizer.TokenizeWithWildcardsForClass(leaf.Tokenization, text, class.Class)
	} else {
		var sw tokenizer.StopwordDetector
		if leaf.Tokenization == models.PropertyTokenizationWord {
			d, err := s.stopwordProvider.GetForNested(leaf)
			if err != nil {
				return nil, fmt.Errorf("nested path %q: get stopwords: %w", fullPath, err)
			}
			sw = d
		}
		prepared := tokenizer.NewPreparedAnalyzer(leaf.TextAnalyzer)
		result := tokenizer.Analyze(valueStr, leaf.Tokenization, class.Class, prepared, sw)
		terms = result.Query
	}

	pvps := make([]*propValuePair, 0, len(terms))
	for _, term := range terms {
		pvps = append(pvps, &propValuePair{
			prop:               propName,
			value:              []byte(term),
			operator:           filter.Operator,
			hasFilterableIndex: true,
			nested:             nestedInfo{isNested: true, relPath: relPath, arrayIndices: arrayIndices},
			Class:              class,
		})
	}

	if len(pvps) == 0 {
		return nil, ErrOnlyStopwords
	}
	if len(pvps) == 1 {
		return pvps[0], nil
	}
	return &propValuePair{operator: filters.OperatorAnd, children: pvps, nested: nestedInfo{isCorrelated: true, childrenFromTokenization: true}, prop: propName, Class: class}, nil
}

// buildNestedPrimitiveFilterPair handles non-text primitive types (int,
// number, bool, date and their array variants).
func (s *Searcher) buildNestedPrimitiveFilterPair(filter *filters.Clause, propName, fullPath, relPath string,
	dt schema.DataType, arrayIndices []filnested.ArrayIndex, class *models.Class,
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
		return nil, fmt.Errorf("nested path %q: unsupported leaf type %q", fullPath, dt)
	}
	if err != nil {
		return nil, fmt.Errorf("nested path %q: encode value: %w", fullPath, err)
	}

	return &propValuePair{
		prop:               propName,
		value:              encodedValue,
		operator:           filter.Operator,
		hasFilterableIndex: true,
		nested:             nestedInfo{isNested: true, relPath: relPath, arrayIndices: arrayIndices},
		Class:              class,
	}, nil
}

// buildNestedIsNullPair builds a propValuePair for an IsNull filter on a
// nested property. relPath is "" for root-level existence (e.g. "addresses IsNull")
// or the dot-notation sub-path (e.g. "city" for "addresses.city IsNull").
// arrayIndices carries any arr[N] constraints from the filter path.
func (s *Searcher) buildNestedIsNullPair(filter *filters.Clause, propName, relPath string, arrayIndices []filnested.ArrayIndex, class *models.Class) (*propValuePair, error) {
	value, err := s.extractBoolValue(filter.Value.Value)
	if err != nil {
		return nil, fmt.Errorf("nested IsNull %q: encode bool: %w", propName, err)
	}
	return &propValuePair{
		prop:     propName,
		value:    value,
		operator: filters.OperatorIsNull,
		nested:   nestedInfo{isNested: true, relPath: relPath, arrayIndices: arrayIndices},
		Class:    class,
	}, nil
}
