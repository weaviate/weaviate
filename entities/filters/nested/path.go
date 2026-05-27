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

package nested

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// pathSep is the separator character for nested property paths.
// All path construction and parsing goes through SplitPath/JoinPath so
// this is the single place to change if the separator ever changes.
const pathSep = "."

// indexOpen / indexClose delimit an arr[N] index suffix inside a path
// segment (e.g. "cars[0]"). All bracket parsing in this package uses
// these constants so callers don't hard-code the characters.
const (
	indexOpen  = "["
	indexClose = "]"
)

// SplitPath splits a dot-notation nested path into its property name segments.
// The inverse of JoinPath.
//
//	SplitPath("cars.tires.width") → ["cars", "tires", "width"]
func SplitPath(path string) []string {
	return strings.Split(path, pathSep)
}

// JoinPath joins property name segments into a dot-notation nested path.
// The inverse of SplitPath.
//
//	JoinPath(["cars", "tires", "width"]) → "cars.tires.width"
func JoinPath(segs []string) string {
	return strings.Join(segs, pathSep)
}

// PathSegment represents one component of a parsed nested filter path.
// Name is the clean property name without any [N] suffix.
type PathSegment struct {
	Name     string
	HasIndex bool
	Index    int
}

// ParseSegments splits a dot-notation nested filter path into PathSegments,
// each with the clean property name and any [N] index extracted.
//
//	ParseSegments("addresses[1].city") → [{Name:"addresses", HasIndex:true, Index:1}, {Name:"city"}]
func ParseSegments(path string) []PathSegment {
	raw := SplitPath(path)
	segs := make([]PathSegment, len(raw))
	for i, s := range raw {
		clean, idx, hasIdx := parseSegmentIndex(s)
		segs[i] = PathSegment{Name: clean, HasIndex: hasIdx, Index: idx}
	}
	return segs
}

// ArrayIndex records a single positional constraint from an arr[N] filter.
// RelPath is the path to the indexed array relative to the root property
// ("" for root elements, "tags" for a sub-array). Index is 0-based.
type ArrayIndex struct {
	RelPath string
	Index   int
}

// ParseIndexedPath parses a full nested filter path (e.g. "cars[1].tags[2]"),
// strips all [N] index markers, and returns:
//   - cleanRelPath: the clean relative path without the root property name or indices
//     (e.g. "tags" for "cars[1].tags[2]", "city" for "addresses[1].city")
//   - cleanRelSegs: the same path as a pre-split slice for direct use with schema walking
//   - arrayIndices: one entry per [N] occurrence with its relative path and 0-based index
func ParseIndexedPath(path string) (cleanRelPath string, cleanRelSegs []string, arrayIndices []ArrayIndex) {
	pathSegs := ParseSegments(path)
	cleanNames := make([]string, len(pathSegs))
	for i, seg := range pathSegs {
		cleanNames[i] = seg.Name
		if seg.HasIndex {
			var relPath string
			if i > 0 {
				relPath = JoinPath(cleanNames[1 : i+1])
			}
			arrayIndices = append(arrayIndices, ArrayIndex{RelPath: relPath, Index: seg.Index})
		}
	}
	if len(cleanNames) > 1 {
		cleanRelSegs = cleanNames[1:]
		cleanRelPath = JoinPath(cleanRelSegs)
	}
	return cleanRelPath, cleanRelSegs, arrayIndices
}

// RootPropName returns the root property name from a nested filter path,
// stripping any [N] index. "addresses[1].city" → "addresses".
func RootPropName(path string) string {
	first, _, _ := strings.Cut(path, pathSep)
	clean, _, _ := parseSegmentIndex(first)
	return clean
}

// parseSegmentIndex extracts an optional [N] suffix from a path segment.
// The closing bracket must be the very last character of the segment.
// "tags[2]" → ("tags", 2, true); "tags" → ("tags", 0, false).
func parseSegmentIndex(seg string) (clean string, index int, hasIndex bool) {
	// The closing bracket must be the very last character.
	if !strings.HasSuffix(seg, indexClose) {
		return seg, 0, false
	}
	start := strings.Index(seg, indexOpen)
	if start < 0 {
		return seg, 0, false
	}
	end := len(seg) - 1
	if end <= start {
		return seg, 0, false
	}
	idx, err := strconv.Atoi(seg[start+1 : end])
	if err != nil || idx < 0 {
		return seg, 0, false
	}
	return seg[:start], idx, true
}

// FindNestedProp returns the NestedProperty with the given name, or nil.
func FindNestedProp(props []*models.NestedProperty, name string) *models.NestedProperty {
	for _, np := range props {
		if np.Name == name {
			return np
		}
	}
	return nil
}

// FindLeaf walks property-name segments through a NestedProperties tree to
// locate the leaf. Segments must be clean names without [N] indices — use
// ParseIndexedPath/ParseSegments to strip indices first.
//
//	_, cleanRelSegs, _ := nested.ParseIndexedPath("cars[0].tires[1].brand")
//	leaf, err := nested.FindLeaf(cleanRelSegs, rootProp.NestedProperties)
func FindLeaf(segments []string, props []*models.NestedProperty) (*models.NestedProperty, error) {
	if len(segments) == 0 {
		return nil, fmt.Errorf("empty nested path")
	}
	var np *models.NestedProperty
	for _, seg := range segments {
		np = FindNestedProp(props, seg)
		if np == nil {
			return nil, fmt.Errorf("sub-property %q not found", seg)
		}
		props = np.NestedProperties
	}
	return np, nil
}

// HasNestedSyntax reports whether path uses nested-property syntax — a
// dotted name like "cars.make" or an indexed name like "cars[0]" or
// "cars[1].tags". Purely syntactic; performs no schema lookup. Use
// IsNestedPath for the full schema-aware check.
//
// Useful at ingress layers (REST/GraphQL/gRPC) that need to route a
// path string to the nested filter pipeline before a schema is in scope.
// Keeps the syntactic conventions (the separator character, the index
// bracket character) localized in this package.
func HasNestedSyntax(path string) bool {
	return strings.ContainsAny(path, pathSep+indexOpen)
}

// IsNestedPath reports whether path is a nested-property filter path on
// the given class — i.e. it has nested-path syntax AND its root segment
// is an object/object[] property. The separator character is internal to
// this package; callers should treat the check as opaque.
//
// A path that contains the separator but whose root is a flat property
// (or doesn't exist on the class) is NOT a nested path — those should
// fall through to the flat-property code path so they surface the right
// "no such property" or "not a nested property" error instead of an
// off-target "sub-property not found".
func IsNestedPath(class *models.Class, path string) bool {
	if !strings.Contains(path, pathSep) {
		return false
	}
	if class == nil {
		return false
	}
	rootProp, err := schema.GetPropertyByName(class, RootPropName(path))
	if err != nil || rootProp == nil || len(rootProp.DataType) == 0 {
		return false
	}
	rootDT := schema.DataType(rootProp.DataType[0])
	return rootDT == schema.DataTypeObject || rootDT == schema.DataTypeObjectArray
}

// ResolveLeaf walks a full nested filter path (dotted, optionally with [N]
// indices) on the given class and returns the leaf NestedProperty.
//
// Performs the same path-shape checks the filter validator does:
//
//   - root property [N] index requires an array type
//   - sub-property must exist in the schema
//   - intermediate sub-property must be object or object[]
//   - sub-property [N] index requires an array type
//
// Does NOT reject filters that *terminate* at an object-typed leaf — that
// rule depends on the filter operator (IsNull tolerates it; everything
// else rejects it), so it stays with the caller.
//
//	leaf, err := nested.ResolveLeaf(class, "cars[0].tires[1].brand")
func ResolveLeaf(class *models.Class, path string) (*models.NestedProperty, error) {
	rootProp, err := schema.GetPropertyByName(class, RootPropName(path))
	if err != nil {
		return nil, err
	}
	return ResolveLeafFromRoot(rootProp, path)
}

// ResolveLeafFromRoot is like ResolveLeaf but takes the already-resolved
// root property. Used by callers that have looked the root property up
// themselves (e.g. the filter validator).
func ResolveLeafFromRoot(rootProp *models.Property, path string) (*models.NestedProperty, error) {
	if rootProp == nil || len(rootProp.DataType) == 0 {
		return nil, fmt.Errorf("ResolveLeafFromRoot: root property is nil or has no datatype (path %q)", path)
	}
	pathSegs := ParseSegments(path)
	if pathSegs[0].HasIndex {
		rootDT := schema.DataType(rootProp.DataType[0])
		if _, ok := schema.IsArrayType(rootDT); !ok {
			return nil, fmt.Errorf(
				"property %q is of type %q — [N] indexing requires an array type",
				pathSegs[0].Name, rootDT)
		}
	}
	if len(pathSegs) <= 1 {
		return nil, fmt.Errorf("property %q has no sub-property segments", pathSegs[0].Name)
	}
	return walkPath(rootProp.NestedProperties, pathSegs[1:], path)
}

// walkPath is the per-segment walk shared by ResolveLeaf/ResolveLeafFromRoot.
// It applies "sub-property exists", "intermediate must be nested", and
// "[N] requires array" checks; the leaf may be any type.
func walkPath(props []*models.NestedProperty, segs []PathSegment, fullPath string) (*models.NestedProperty, error) {
	for i, seg := range segs {
		np := FindNestedProp(props, seg.Name)
		if np == nil {
			return nil, fmt.Errorf("nested path %q: sub-property %q not found", fullPath, seg.Name)
		}
		dt := schema.DataType(np.DataType[0])
		if seg.HasIndex {
			if _, ok := schema.IsArrayType(dt); !ok {
				return nil, fmt.Errorf(
					"nested path %q: sub-property %q is of type %q — [N] indexing requires an array type",
					fullPath, seg.Name, dt)
			}
		}
		if i == len(segs)-1 {
			return np, nil
		}
		if !schema.IsNested(dt) {
			return nil, fmt.Errorf(
				"nested path %q: sub-property %q must be object or object[], got %q",
				fullPath, seg.Name, dt)
		}
		props = np.NestedProperties
	}
	return nil, fmt.Errorf("nested path %q: empty sub-property path", fullPath)
}
