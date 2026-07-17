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

// buildResponse converts the traverser's raw results into the
// models.SearchResponse envelope: per hit the always-present object id, the
// selected non-reference properties under "properties", reference selections
// under "references" and the requested retrieval metadata under "metadata",
// plus tookMs. Vectors are never returned; there is no count field.
func buildResponse(res []any, params dto.GetParams, took time.Duration) (*models.SearchResponse, error) {
	results := make([]*models.SearchResultObject, len(res))
	for i, raw := range res {
		asMap, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("could not parse result %v", raw)
		}

		// the id is requested on every search, so it must be present
		id, ok := asMap["id"].(strfmt.UUID)
		if !ok {
			return nil, errors.New("could not read object id from search result")
		}

		obj := &models.SearchResultObject{
			ID:         &id,
			Properties: make(map[string]models.JSONObject, len(params.Properties)),
			Metadata:   buildMetadata(asMap, params.AdditionalProperties),
		}

		for _, prop := range params.Properties {
			value, ok := asMap[prop.Name]
			if !ok || value == nil {
				continue
			}
			if len(prop.Refs) > 0 {
				if refs := buildRefProperty(value, prop); refs != nil {
					if obj.References == nil {
						obj.References = map[string][]models.JSONObject{}
					}
					obj.References[prop.Name] = refs
				}
				continue
			}
			if propValue := buildPropertyValue(value, prop); propValue != nil {
				obj.Properties[prop.Name] = propValue
			}
		}

		results[i] = obj
	}

	tookMs := took.Milliseconds()
	return &models.SearchResponse{Results: results, TookMs: &tookMs}, nil
}

// buildPropertyValue renders one selected non-reference property. Primitive
// values pass through, object/object[] values are pruned to the selected
// nested properties. Returns nil when the value has an unexpected shape —
// omitting is safer than leaking unselected (possibly blob) fields.
func buildPropertyValue(value any, prop search.SelectProperty) any {
	if prop.IsPrimitive {
		return value
	}
	if prop.IsObject {
		return buildObjectProperty(value, prop.Props)
	}
	return nil
}

// buildObjectProperty prunes an object / object-array value down to the
// selected nested properties, recursing into deeper objects. The storage
// layer returns ALL nested fields, so pruning here is what keeps unselected
// (possibly blob) fields from leaking.
func buildObjectProperty(value any, props []search.SelectProperty) any {
	switch v := value.(type) {
	case map[string]any:
		return pruneObjectFields(v, props)
	case []any:
		out := make([]any, 0, len(v))
		for _, item := range v {
			if fields, ok := item.(map[string]any); ok {
				out = append(out, pruneObjectFields(fields, props))
			}
		}
		return out
	default:
		return nil
	}
}

func pruneObjectFields(fields map[string]any, props []search.SelectProperty) map[string]any {
	out := make(map[string]any, len(props))
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

// buildRefProperty renders a cross-reference as an array of objects carrying
// the selected one-hop properties, e.g. [{"name": "..."}].
func buildRefProperty(value any, prop search.SelectProperty) []models.JSONObject {
	refsRaw, ok := value.([]any)
	if !ok || len(prop.Refs) == 0 {
		return nil
	}

	refs := make([]models.JSONObject, 0, len(refsRaw))
	for _, refRaw := range refsRaw {
		localRef, ok := refRaw.(search.LocalRef)
		if !ok {
			continue
		}
		fields := make(map[string]any, len(prop.Refs[0].RefProperties))
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

// buildMetadata picks the requested retrieval metadata out of the
// traverser's internal "_additional" map. Each field is best-effort — it is
// only present for certain searches/configs. Returns nil when nothing beyond
// the id was requested (or present); the id lives on the envelope itself.
func buildMetadata(asMap map[string]any, addl additional.Properties) *models.SearchResultMetadata {
	additionalMap, ok := asMap["_additional"].(map[string]any)
	if !ok {
		return nil
	}

	metadata := &models.SearchResultMetadata{}
	populated := false

	if addl.Distance {
		if distance, ok := additionalMap["distance"].(float32); ok {
			metadata.Distance = &distance
			populated = true
		}
	}
	if addl.Certainty {
		if certainty, ok := additionalMap["certainty"].(float64); ok {
			metadata.Certainty = &certainty
			populated = true
		}
	}
	if addl.Score {
		if score, ok := additionalMap["score"].(float32); ok {
			metadata.Score = &score
			populated = true
		}
	}
	if addl.ExplainScore {
		if explainScore, ok := additionalMap["explainScore"].(string); ok {
			metadata.ExplainScore = &explainScore
			populated = true
		}
	}
	if addl.CreationTimeUnix {
		if creationTime, ok := additionalMap["creationTimeUnix"].(int64); ok {
			metadata.CreationTime = &creationTime
			populated = true
		}
	}
	if addl.LastUpdateTimeUnix {
		if lastUpdateTime, ok := additionalMap["lastUpdateTimeUnix"].(int64); ok {
			metadata.LastUpdateTime = &lastUpdateTime
			populated = true
		}
	}

	if !populated {
		return nil
	}
	return metadata
}
