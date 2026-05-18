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
	"strconv"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
)

// pathSep is the separator character for nested property paths.
// All path construction and parsing goes through SplitPath/JoinPath so
// this is the single place to change if the separator ever changes.
const pathSep = "."

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
	if len(seg) == 0 || seg[len(seg)-1] != ']' {
		return seg, 0, false
	}
	start := strings.Index(seg, "[")
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
