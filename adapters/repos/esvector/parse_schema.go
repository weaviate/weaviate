//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package esvector

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// parseSchema lightly parses the schema, while most fields stay untyped, those
// with special meaning, such as GeoCoordinates are marshalled into their
// required types
func (r *Repo) parseSchema(input map[string]interface{}, resolveDepth int, cache cache) (map[string]interface{}, error) {
	output := map[string]interface{}{}

	for key, value := range input {
		if isID(key) {
			output["uuid"] = value
		}

		if isInternal(key) {
			continue
		}

		switch typed := value.(type) {
		case map[string]interface{}:
			parsed, err := parseMapProp(typed)
			if err != nil {
				return output, fmt.Errorf("prop '%s': %v", key, err)
			}

			output[key] = parsed

		case []interface{}:
			// must be a ref
			if resolveDepth == 0 {
				// TODO: check actual resolve depth

				// don't resolve, simply convert
				refs := []*models.SingleRef{}
				for _, ref := range typed {
					refs = append(refs,
						&models.SingleRef{Beacon: strfmt.URI(ref.(map[string]interface{})["beacon"].(string))})
				}

				mref := models.MultipleRef(refs)
				output[key] = &mref
				continue
			}

			parsed, err := r.parseRefs(typed, key, cache)
			if err != nil {
				return output, fmt.Errorf("prop '%s': %v", key, err)
			}

			// ref keys are uppercased in the desired response
			refKey := uppercaseFirstLetter(key)
			output[refKey] = parsed

		default:
			// anything else remains unchanged
			output[key] = value
		}
	}

	return output, nil
}

func isID(key string) bool {
	return key == keyID.String()
}

func isInternal(key string) bool {
	return string(key[0]) == "_"
}

func parseMapProp(input map[string]interface{}) (interface{}, error) {
	lat, latOK := input["lat"]
	lon, lonOK := input["lon"]

	if latOK && lonOK {
		// this is a geoCoordinates prop
		return parseGeoProp(lat, lon)
	}

	return nil, fmt.Errorf("unknown map prop which is not a geo prop: %v", input)
}

func parseGeoProp(lat interface{}, lon interface{}) (*models.GeoCoordinates, error) {
	latFloat, ok := lat.(float64)
	if !ok {
		return nil, fmt.Errorf("explected lat to be float64, but is %T", lat)
	}

	lonFloat, ok := lon.(float64)
	if !ok {
		return nil, fmt.Errorf("explected lon to be float64, but is %T", lon)
	}

	return &models.GeoCoordinates{Latitude: float32(latFloat), Longitude: float32(lonFloat)}, nil
}

func (r *Repo) parseRefs(input []interface{}, prop string, cache cache) ([]interface{}, error) {
	output := make([]interface{}, len(input), len(input))
	if cache.hot {
		// TODO: instead of removing the classes, actually take them into account
		refs, err := r.parseCacheSchemaToRefs(cache.schema, prop)
		if err != nil {
			return nil, fmt.Errorf("parse cache: %v", err)
		}

		return refs, nil
	} else {

		for i, item := range input {
			refMap, ok := item.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("expected ref item to be a map, but got %T", item)
			}

			beacon, ok := refMap["beacon"]
			if !ok {
				return nil, fmt.Errorf("expected ref object to have field beacon, but got %#v", refMap)
			}

			ref, err := crossref.Parse(beacon.(string))
			if err != nil {
				return nil, err
			}

			switch ref.Kind {
			case kind.Thing:
				res, err := r.ThingByID(context.TODO(), ref.TargetID, 100)
				if err != nil {
					return nil, err
				}

				output[i] = get.LocalRef{
					Class:  res.ClassName,
					Fields: res.Schema.(map[string]interface{}),
				}
			case kind.Action:
				res, err := r.ActionByID(context.TODO(), ref.TargetID, 100)
				if err != nil {
					return nil, err
				}

				output[i] = get.LocalRef{
					Class:  res.ClassName,
					Fields: res.Schema.(map[string]interface{}),
				}
			}
		}
	}

	return output, nil
}

type cache struct {
	hot    bool
	schema map[string]interface{}
}

func (r *Repo) extractCache(in map[string]interface{}) cache {
	cacheField, ok := in[keyCache.String()]
	if !ok {
		return cache{}
	}

	cacheMap, ok := cacheField.(map[string]interface{})
	if !ok {
		return cache{}
	}

	hot, ok := cacheMap[keyCacheHot.String()]
	if !ok {
		return cache{}
	}

	schema := map[string]interface{}{}
	for key, value := range cacheMap {
		if key == keyCacheHot.String() {
			continue
		}

		schema[key] = value
	}

	return cache{
		hot:    hot.(bool),
		schema: schema,
	}
}

func uppercaseFirstLetter(in string) string {
	first := string(in[0])
	rest := string(in[1:])

	return strings.ToUpper(first) + rest
}

func (r *Repo) parseCacheSchemaToRefs(in map[string]interface{}, prop string) ([]interface{}, error) {
	var out []interface{}
	refClassesMap, ok := in[prop].(map[string]interface{})
	if !ok {
		// not a ref
		return nil, fmt.Errorf("not a map: %#v", in[prop])
	}

	for className, refs := range refClassesMap {

		refsSlice, ok := refs.([]interface{})
		if !ok {
			return nil, fmt.Errorf("expected refs to be slice, but got %#v", refs)
		}

		for _, ref := range refsSlice {
			refMap, ok := ref.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("expected refs to be slice, but got %#v", refs)
			}

			parsed, err := r.parseInnerRefFromCache(refMap)
			if err != nil {
				return nil, err
			}

			out = append(out, get.LocalRef{
				Class:  className,
				Fields: parsed,
			})
		}
	}

	return out, nil
}

func (r *Repo) parseInnerRefFromCache(in map[string]interface{}) (map[string]interface{}, error) {
	out := map[string]interface{}{}

	for prop, value := range in {
		if _, ok := value.(map[string]interface{}); ok {
			parsed, err := r.parseCacheSchemaToRefs(in, prop)
			if err != nil {
				return nil, err
			}

			out[uppercaseFirstLetter(prop)] = parsed
			continue
		}

		// TODO: Deal with geo location props
		out[prop] = value
	}

	return out, nil
}
