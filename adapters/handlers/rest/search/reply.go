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
	"errors"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

// metadataKey is the reserved key carrying retrieval metadata on each result
// object. "_additional" is a reserved property name (see
// entities/schema/validation.go), so it can never collide with a collection
// property; it also matches the GraphQL API's key for the same data.
const metadataKey = "_additional"

// buildResponse converts the traverser's raw results into the flat
// models.SearchResponse: flat objects with properties at the root and
// requested retrieval metadata under metadataKey, plus took_ms. Vectors are
// never returned; there is no count field.
func buildResponse(res []interface{}, params dto.GetParams, took time.Duration) (*models.SearchResponse, error) {
	results := make([]models.SearchResultObject, len(res))
	for i, raw := range res {
		asMap, ok := raw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("could not parse result %v", raw)
		}

		obj := make(map[string]interface{}, len(params.Properties)+1)
		for _, prop := range params.Properties {
			value, ok := asMap[prop.Name]
			if !ok || value == nil {
				continue
			}
			if propValue := buildPropertyValue(value, prop); propValue != nil {
				obj[prop.Name] = propValue
			}
		}

		metadata, err := buildMetadata(asMap, params.AdditionalProperties)
		if err != nil {
			return nil, err
		}
		if len(metadata) > 0 {
			obj[metadataKey] = metadata
		}

		results[i] = obj
	}

	return &models.SearchResponse{Results: results, TookMs: took.Milliseconds()}, nil
}

// buildPropertyValue renders one selected property. Primitive values pass
// through, object/object[] values are pruned to the selected nested
// properties, references become nested arrays. Returns nil when the value
// has an unexpected shape — omitting is safer than leaking unselected
// (possibly blob) fields.
func buildPropertyValue(value interface{}, prop search.SelectProperty) interface{} {
	if prop.IsPrimitive {
		return value
	}
	if prop.IsObject {
		return buildObjectProperty(value, prop.Props)
	}
	if refs := buildRefProperty(value, prop); refs != nil {
		return refs
	}
	return nil
}

// buildObjectProperty prunes an object / object-array value down to the
// selected nested properties, recursing into deeper objects. The storage
// layer returns ALL nested fields, so pruning here is what keeps unselected
// (possibly blob) fields from leaking.
func buildObjectProperty(value interface{}, props []search.SelectProperty) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		return pruneObjectFields(v, props)
	case []interface{}:
		out := make([]interface{}, 0, len(v))
		for _, item := range v {
			if fields, ok := item.(map[string]interface{}); ok {
				out = append(out, pruneObjectFields(fields, props))
			}
		}
		return out
	default:
		return nil
	}
}

func pruneObjectFields(fields map[string]interface{}, props []search.SelectProperty) map[string]interface{} {
	out := make(map[string]interface{}, len(props))
	for _, prop := range props {
		value, ok := fields[prop.Name]
		if !ok || value == nil {
			continue
		}
		if prop.IsObject {
			if nested := buildObjectProperty(value, prop.Props); nested != nil {
				out[prop.Name] = nested
			}
			continue
		}
		out[prop.Name] = value
	}
	return out
}

// buildRefProperty renders a cross-reference as a nested array of objects,
// e.g. "hasAuthor": [{"name": "..."}].
func buildRefProperty(value interface{}, prop search.SelectProperty) []map[string]interface{} {
	refsRaw, ok := value.([]interface{})
	if !ok || len(prop.Refs) == 0 {
		return nil
	}

	refs := make([]map[string]interface{}, 0, len(refsRaw))
	for _, refRaw := range refsRaw {
		localRef, ok := refRaw.(search.LocalRef)
		if !ok {
			continue
		}
		fields := make(map[string]interface{}, len(prop.Refs[0].RefProperties))
		for _, refProp := range prop.Refs[0].RefProperties {
			refValue, ok := localRef.Fields[refProp.Name]
			if !ok || refValue == nil {
				continue
			}
			// object-typed properties of the referenced collection get the
			// same nested pruning as root-level objects
			if refProp.IsObject {
				if nested := buildObjectProperty(refValue, refProp.Props); nested != nil {
					fields[refProp.Name] = nested
				}
				continue
			}
			fields[refProp.Name] = refValue
		}
		refs = append(refs, fields)
	}
	return refs
}

// buildMetadata picks the requested metadata out of a result. The traverser
// reports the object id at the top level and everything else under
// "_additional". A requested id that is missing or mistyped is an error; the
// remaining fields are best-effort as they are only present for certain
// searches/configs.
func buildMetadata(asMap map[string]interface{}, addl additional.Properties) (map[string]interface{}, error) {
	metadata := make(map[string]interface{}, 2)

	if addl.ID {
		id, ok := asMap["id"].(strfmt.UUID)
		if !ok {
			return nil, errors.New("could not read object id from search result")
		}
		metadata["id"] = id.String()
	}

	additionalRaw, ok := asMap["_additional"]
	if !ok {
		return metadata, nil
	}
	additionalMap, ok := additionalRaw.(map[string]interface{})
	if !ok {
		return metadata, nil
	}

	if addl.Distance {
		if distance, ok := additionalMap["distance"].(float32); ok {
			metadata["distance"] = distance
		}
	}
	if addl.Certainty {
		if certainty, ok := additionalMap["certainty"].(float64); ok {
			metadata["certainty"] = certainty
		}
	}
	if addl.Score {
		if score, ok := additionalMap["score"].(float32); ok {
			metadata["score"] = score
		}
	}
	if addl.ExplainScore {
		if explainScore, ok := additionalMap["explainScore"].(string); ok {
			metadata["explain_score"] = explainScore
		}
	}
	if addl.CreationTimeUnix {
		if creationTime, ok := additionalMap["creationTimeUnix"].(int64); ok {
			metadata["creation_time"] = creationTime
		}
	}
	if addl.LastUpdateTimeUnix {
		if lastUpdateTime, ok := additionalMap["lastUpdateTimeUnix"].(int64); ok {
			metadata["last_update_time"] = lastUpdateTime
		}
	}

	return metadata, nil
}
