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

	switch len(pvps) {
	case 0:
		return nil, ErrOnlyStopwords
	case 1:
		return pvps[0], nil
	default:
		// Propagate relPath and arrayIndices onto the wrapper so callers that
		// inspect the wrapper directly (e.g. extractContains' first-class
		// ContainsNone path) see the same operand metadata as the inner
		// per-token leaves.
		return &propValuePair{
			operator: filters.OperatorAnd,
			children: pvps,
			nested: nestedInfo{
				isWithinRootSubtree:      true,
				childrenFromTokenization: true,
				relPath:                  relPath,
				arrayIndices:             arrayIndices,
			},
			prop:  propName,
			Class: class,
		}, nil
	}
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
	case schema.DataTypeUUID:
		encodedValue, err = s.extractUUIDValue(filter.Value.Value)
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

// nestedRootProp returns the root property name if child's entire subtree
// consists of nested conditions on a single root nested prop, or "" if it
// contains any flat property, mixed-root nested leaves, or has no nested
// content. Used by groupNestedByProp to decide which children fold into a
// same-root subtree wrapper.
//
// Terminating cases:
//   - direct nested leaf (nested.isNested): returns child.prop.
//   - already-marked subtree (nested.isWithinRootSubtree): returns child.prop.
//
// Recursive cases — for AND/OR/NOT operator children, all descendants must
// agree on a single root for the subtree to be eligible:
//   - AND/OR with N≥1 children: all children must return the same non-empty root.
//   - NOT with 1 child: returns whatever the operand returns.
//
// A flat leaf, mixed-root subtree, or empty operator returns "".
func nestedRootProp(child *propValuePair) string {
	if child.nested.isNested || child.nested.isWithinRootSubtree {
		return child.prop
	}
	switch child.operator {
	case filters.OperatorAnd, filters.OperatorOr, filters.OperatorNot,
		filters.ContainsAll, filters.ContainsAny, filters.ContainsNone:
		// ContainsAll / ContainsAny / ContainsNone are AND / OR / NOT(OR) aliases
		// on a nested path (first-class-operator approach — operator identity
		// preserved by extractContains). Including ContainsNone here lets
		// groupNestedByProp same-root-wrap `AND(name=…, ContainsNone(tags,…))`
		// so the correlated-AND path enforces same-element semantics across
		// the two predicates.
		if len(child.children) == 0 {
			return ""
		}
		first := nestedRootProp(child.children[0])
		if first == "" {
			return ""
		}
		for _, gc := range child.children[1:] {
			if nestedRootProp(gc) != first {
				return ""
			}
		}
		return first
	default:
		return ""
	}
}

// groupNestedByProp rewrites a flat slice of AND or OR children so that
// conditions targeting the same root nested property are grouped into a
// single isWithinRootSubtree wrapper node (with the caller's parentOperator,
// AND or OR) that the resolver will handle with position-aware semantics.
// Flat (non-nested) conditions and nested conditions from different props
// are returned unchanged in their original order.
//
// A child is eligible for grouping when it is:
//   - a direct nested leaf (nested.isNested == true), or
//   - a tokenization-produced compound AND (nested.isWithinRootSubtree == true)
//
// If no nested children are found, the original slice is returned as-is.
// Single-child groups are kept as plain children (no wrapper node created).
//
// Example — given an AND filter with parentOperator=AND:
//
//	addresses.city = "Berlin"     (nested, root=addresses)
//	age > 30                      (flat)
//	addresses.postcode = "10115"  (nested, root=addresses)
//	cars.make = "BMW"             (nested, root=cars)
//
// the result is:
//
//	isWithinRootSubtree(addresses, AND) [city="Berlin", postcode="10115"]
//	age > 30
//	cars.make = "BMW"
//
// The same call with parentOperator=OR produces the analogous OR wrapper.
func groupNestedByProp(children []*propValuePair, class *models.Class, parentOperator filters.Operator) []*propValuePair {
	// First pass: build groups per prop (preserving first-seen order).
	groups := make(map[string][]*propValuePair)
	var propOrder []string
	for _, child := range children {
		prop := nestedRootProp(child)
		if prop == "" {
			continue
		}
		if _, seen := groups[prop]; !seen {
			propOrder = append(propOrder, prop)
		}
		groups[prop] = append(groups[prop], child)
	}
	if len(groups) == 0 {
		return children
	}

	// Second pass: reconstruct the children slice, replacing multi-condition
	// nested groups with a single isWithinRootSubtree wrapper node carrying the
	// caller's parentOperator. Single-condition groups are kept as plain
	// children unless the singleton is a nested NOT/OR operator subtree —
	// wrapping it ensures the planner evaluates scope-aware NOT/OR at the
	// operand's natural LCA even when the parent AND has cross-root or scalar
	// siblings (singleton-NOT/OR wrapping). Flat conditions retain their position.
	result := make([]*propValuePair, 0, len(children))
	emitted := make(map[string]bool, len(propOrder))
	for _, child := range children {
		prop := nestedRootProp(child)
		if prop == "" {
			result = append(result, child)
			continue
		}
		if emitted[prop] {
			continue
		}
		emitted[prop] = true
		group := groups[prop]
		if len(group) == 1 && !shouldWrapNestedSingleton(group[0]) {
			result = append(result, group[0])
			continue
		}
		result = append(result, &propValuePair{
			operator: parentOperator,
			nested:   nestedInfo{isWithinRootSubtree: true},
			prop:     prop,
			children: group,
			Class:    class,
		})
	}
	return result
}

// shouldWrapNestedSingleton reports whether a singleton nested group child
// needs an isWithinRootSubtree wrapper. Direct leaves (isNested=true) share
// docID-level semantics with their wrapped form, so wrapping adds overhead
// without changing results. Already-wrapped children (tokenization wrappers,
// inner AND-isWRS) carry their own scope-aware processing and don't need
// another layer. Nested NOT/OR operator children DO need wrapping so the
// scope-aware planner inverts NOT or unions OR at the operand's LCA per
// element — without this, a NOT (or OR) singleton with a scalar/cross-root
// sibling falls back to docID-level resolution.
func shouldWrapNestedSingleton(child *propValuePair) bool {
	if child.nested.isWithinRootSubtree {
		return false
	}
	if child.nested.isNested {
		return false
	}
	switch child.operator {
	case filters.OperatorNot, filters.OperatorOr:
		return true
	default:
		return false
	}
}
