//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright (c) 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package traverser

import (
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
)

func (e *Explorer) extractHighlights(schema models.PropertySchema, params dto.GetParams) []additional.Highlight {
	highlight := params.AdditionalProperties.Highlight
	if highlight == nil {
		return nil
	}

	query := highlightQuery(params)
	terms := highlightTerms(query)
	if len(terms) == 0 {
		return []additional.Highlight{}
	}

	matcher := highlightMatcher(terms)
	if matcher == nil {
		return []additional.Highlight{}
	}

	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return []additional.Highlight{}
	}

	properties := highlightPropertyNames(params)
	if len(properties) == 0 {
		properties = stringProperties(schemaMap)
	}

	out := make([]additional.Highlight, 0, len(properties))
	for _, property := range properties {
		values := highlightPropertyValues(schemaMap, property)
		if len(values) == 0 {
			continue
		}

		fragments := highlightFragments(values, matcher, highlight)
		if len(fragments) == 0 {
			continue
		}

		out = append(out, additional.Highlight{
			Property:  property,
			Fragments: fragments,
		})
	}

	return out
}

func highlightQuery(params dto.GetParams) string {
	if params.KeywordRanking != nil {
		return params.KeywordRanking.Query
	}
	if params.HybridSearch != nil {
		return params.HybridSearch.Query
	}
	return ""
}

func highlightPropertyNames(params dto.GetParams) []string {
	if params.AdditionalProperties.Highlight != nil && len(params.AdditionalProperties.Highlight.Properties) > 0 {
		return uniquePropertyNames(params.AdditionalProperties.Highlight.Properties)
	}
	if params.KeywordRanking != nil && len(params.KeywordRanking.Properties) > 0 {
		return uniquePropertyNames(params.KeywordRanking.Properties)
	}
	if params.HybridSearch != nil && len(params.HybridSearch.Properties) > 0 {
		return uniquePropertyNames(params.HybridSearch.Properties)
	}
	return uniquePropertyNames(params.Properties.GetPropertyNames())
}

func uniquePropertyNames(properties []string) []string {
	out := make([]string, 0, len(properties))
	seen := map[string]struct{}{}
	for _, property := range properties {
		property = strings.TrimSpace(strings.Split(property, "^")[0])
		if property == "" {
			continue
		}
		if _, ok := seen[property]; ok {
			continue
		}
		seen[property] = struct{}{}
		out = append(out, property)
	}
	return out
}

func stringProperties(schema map[string]interface{}) []string {
	out := make([]string, 0, len(schema))
	for property, value := range schema {
		if property == "id" {
			continue
		}
		if values := highlightValues(value); len(values) > 0 {
			out = append(out, property)
		}
	}
	sort.Strings(out)
	return out
}

func highlightPropertyValues(schema map[string]interface{}, property string) []string {
	path := strings.Split(property, ".")
	var value interface{} = schema
	for _, segment := range path {
		asMap, ok := value.(map[string]interface{})
		if !ok {
			return nil
		}
		value = asMap[segment]
	}
	return highlightValues(value)
}

func highlightValues(value interface{}) []string {
	switch value := value.(type) {
	case string:
		if value == "" {
			return nil
		}
		return []string{value}
	case []string:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if item != "" {
				out = append(out, item)
			}
		}
		return out
	case []interface{}:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if text, ok := item.(string); ok && text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func highlightTerms(query string) []string {
	seen := map[string]struct{}{}
	var terms []string
	for _, term := range strings.FieldsFunc(query, func(r rune) bool {
		return !(unicode.IsLetter(r) || unicode.IsNumber(r))
	}) {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		key := strings.ToLower(term)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		terms = append(terms, term)
	}
	sort.SliceStable(terms, func(i, j int) bool {
		return len([]rune(terms[i])) > len([]rune(terms[j]))
	})
	return terms
}

func highlightMatcher(terms []string) *regexp.Regexp {
	if len(terms) == 0 {
		return nil
	}

	quoted := make([]string, 0, len(terms))
	for _, term := range terms {
		if term == "" {
			continue
		}
		quoted = append(quoted, regexp.QuoteMeta(term))
	}
	if len(quoted) == 0 {
		return nil
	}

	return regexp.MustCompile(`(?i)` + strings.Join(quoted, "|"))
}

func highlightFragments(values []string, matcher *regexp.Regexp, params *additional.HighlightProperties) []string {
	fragmentCount := params.FragmentCount
	if fragmentCount <= 0 {
		fragmentCount = additional.DefaultHighlightFragmentCount
	}
	fragmentSize := params.FragmentSize
	if fragmentSize <= 0 {
		fragmentSize = additional.DefaultHighlightFragmentSize
	}
	preTag := params.PreTag
	if preTag == "" {
		preTag = additional.DefaultHighlightPreTag
	}
	postTag := params.PostTag
	if postTag == "" {
		postTag = additional.DefaultHighlightPostTag
	}

	out := make([]string, 0, fragmentCount)
	seen := map[string]struct{}{}
	for _, value := range values {
		for _, loc := range matcher.FindAllStringIndex(value, -1) {
			fragment := highlightFragment(value, loc[0], loc[1], matcher, fragmentSize, preTag, postTag)
			if _, ok := seen[fragment]; ok {
				continue
			}
			seen[fragment] = struct{}{}
			out = append(out, fragment)
			if len(out) >= fragmentCount {
				return out
			}
		}
	}
	return out
}

func highlightFragment(value string, startByte, endByte int, matcher *regexp.Regexp, fragmentSize int, preTag, postTag string) string {
	matchStartRune := utf8.RuneCountInString(value[:startByte])
	matchEndRune := utf8.RuneCountInString(value[:endByte])
	matchSize := matchEndRune - matchStartRune
	if fragmentSize < matchSize {
		fragmentSize = matchSize
	}

	totalRunes := utf8.RuneCountInString(value)
	left := (fragmentSize - matchSize) / 2
	startRune := matchStartRune - left
	if startRune < 0 {
		startRune = 0
	}
	endRune := startRune + fragmentSize
	if endRune > totalRunes {
		endRune = totalRunes
		startRune = endRune - fragmentSize
		if startRune < 0 {
			startRune = 0
		}
	}

	start := byteIndexForRune(value, startRune)
	end := byteIndexForRune(value, endRune)
	fragment := value[start:end]
	if startRune > 0 {
		fragment = "..." + strings.TrimLeftFunc(fragment, unicode.IsSpace)
	}
	if endRune < totalRunes {
		fragment = strings.TrimRightFunc(fragment, unicode.IsSpace) + "..."
	}

	return matcher.ReplaceAllStringFunc(fragment, func(match string) string {
		return preTag + match + postTag
	})
}

func byteIndexForRune(value string, runeIndex int) int {
	if runeIndex <= 0 {
		return 0
	}

	current := 0
	for idx := range value {
		if current == runeIndex {
			return idx
		}
		current++
	}
	return len(value)
}
