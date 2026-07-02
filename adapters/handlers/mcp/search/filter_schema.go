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

package search

import (
	"encoding/json"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/entities/models"
)

// whereFilterOperators is the canonical operator enum advertised in the hybrid
// tool's `filters` schema. It is sourced from the models constants so the
// advertised enum can never drift from what filterext.Parse actually accepts.
// Keep in sync with the WhereFilterOperator* constants in
// entities/models/where_filter.go (a drift test pins this).
var whereFilterOperators = []string{
	models.WhereFilterOperatorAnd,
	models.WhereFilterOperatorOr,
	models.WhereFilterOperatorNot,
	models.WhereFilterOperatorEqual,
	models.WhereFilterOperatorNotEqual,
	models.WhereFilterOperatorLike,
	models.WhereFilterOperatorGreaterThan,
	models.WhereFilterOperatorGreaterThanEqual,
	models.WhereFilterOperatorLessThan,
	models.WhereFilterOperatorLessThanEqual,
	models.WhereFilterOperatorContainsAny,
	models.WhereFilterOperatorContainsAll,
	models.WhereFilterOperatorContainsNone,
	models.WhereFilterOperatorWithinGeoRange,
	models.WhereFilterOperatorIsNull,
}

// hybridFilterSchema returns the JSON-schema object advertised for the
// `filters` argument of weaviate-query-hybrid. It mirrors the REST WhereFilter
// (entities/models.WhereFilter) so an LLM client can emit valid filters.
//
// It is hand-authored rather than reflected from the Go type because mark3labs
// builds tool input schemas with jsonschema.Reflector{DoNotReference: true}.
// Under DoNotReference the reflector inlines every type and never emits $ref,
// so the self-referential WhereFilter (Operands []*WhereFilter) would recurse
// infinitely and overflow the stack while constructing the tool at startup.
// Nested operands are therefore conveyed via description rather than a $ref.
func hybridFilterSchema() map[string]any {
	enum := make([]any, len(whereFilterOperators))
	for i, op := range whereFilterOperators {
		enum[i] = op
	}
	return map[string]any{
		"type": "object",
		"description": "Optional where-filter, identical in shape to the REST `where` argument. " +
			"A leaf filter compares one property, e.g. {\"path\":[\"title\"],\"operator\":\"Equal\",\"valueText\":\"x\"}. " +
			"Combine leaves with {\"operator\":\"And\"|\"Or\",\"operands\":[ ...nested filters... ]}.",
		"properties": map[string]any{
			"operator": map[string]any{
				"type":        "string",
				"enum":        enum,
				"description": "Comparison operator for a leaf filter, or And/Or/Not to combine `operands`.",
			},
			"path": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Property path. Direct property: [\"title\"]. Cross-reference: [\"hasAuthor\",\"Author\",\"name\"].",
			},
			"valueText":    map[string]any{"type": "string", "description": "Value for text/string properties."},
			"valueInt":     map[string]any{"type": "integer", "description": "Value for int properties."},
			"valueNumber":  map[string]any{"type": "number", "description": "Value for number properties."},
			"valueBoolean": map[string]any{"type": "boolean", "description": "Value for boolean properties."},
			"valueDate":    map[string]any{"type": "string", "description": "Value for date properties, RFC3339 (e.g. 2020-01-01T00:00:00Z)."},
			"valueTextArray": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Values for ContainsAny/ContainsAll/ContainsNone on text properties.",
			},
			"valueIntArray": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "integer"},
				"description": "Values for ContainsAny/ContainsAll/ContainsNone on int properties.",
			},
			"valueGeoRange": map[string]any{
				"type":        "object",
				"description": "Value for WithinGeoRange: a center coordinate plus a max distance in meters.",
				"properties": map[string]any{
					"geoCoordinates": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"latitude":  map[string]any{"type": "number"},
							"longitude": map[string]any{"type": "number"},
						},
					},
					"distance": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"max": map[string]any{"type": "number", "description": "Max distance in meters."},
						},
					},
				},
			},
			"operands": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "object"},
				"description": "Nested where-filters with the same structure as this object. Use with the And/Or operators to build compound filters.",
			},
		},
	}
}

// withHybridFilterSchema injects hybridFilterSchema() as the `filters` property
// of the tool's already-reflected input schema. It must be applied AFTER
// mcp.WithInputSchema so that it runs on the generated RawInputSchema. The
// approach mirrors internal.overridePropertyDescriptions, which likewise
// mutates the raw schema rather than relying on struct reflection.
func withHybridFilterSchema() mcp.ToolOption {
	return func(t *mcp.Tool) {
		if t.RawInputSchema == nil {
			return
		}
		var schema map[string]any
		if err := json.Unmarshal(t.RawInputSchema, &schema); err != nil {
			return
		}
		props, ok := schema["properties"].(map[string]any)
		if !ok {
			return
		}
		props["filters"] = hybridFilterSchema()
		raw, err := json.Marshal(schema)
		if err != nil {
			return
		}
		t.RawInputSchema = raw
	}
}
