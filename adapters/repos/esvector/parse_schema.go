//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
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
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// parseSchema lightly parses the schema, while most fields stay untyped, those
// with special meaning, such as GeoCoordinates are marshalled into their
// required types
func (r *Repo) parseSchema(input map[string]interface{}, properties traverser.SelectProperties, cache cache,
	currentDepth int) (map[string]interface{}, error) {
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
			if !properties.HasRefs() {
				// the user isn't interested in resolving any refs, therefore simply
				// return the unresolved beacon
				refs := []*models.SingleRef{}
				for _, ref := range typed {
					refs = append(refs,
						&models.SingleRef{Beacon: strfmt.URI(ref.(map[string]interface{})["beacon"].(string))})
				}

				output[key] = models.MultipleRef(refs)
				continue
			}

			// properties.ShouldResolve([]string{uppercaseFirstLetter(

			// ref keys are uppercased in the desired response
			refKey := uppercaseFirstLetter(key)
			selectProp := properties.FindProperty(refKey)
			if selectProp == nil {
				// user is not interested in this prop
				continue
			}

			parsed, err := r.parseRefs(typed, key, cache, currentDepth, *selectProp)
			if err != nil {
				return output, fmt.Errorf("prop '%s': %v", key, err)
			}

			if len(parsed) > 0 {
				output[refKey] = parsed
			}

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

// parseRef is only called on the outer layer
func (r *Repo) parseRefs(input []interface{}, prop string, cache cache, depth int,
	selectProp traverser.SelectProperty) ([]interface{}, error) {

	if cache.hot {
		// start with depth=1, parseRefs was called on the outermost class
		// (depth=0), so the first ref is depth=1
		refs, err := r.parseCacheSchemaToRefs(cache.schema, prop, 1, selectProp)
		if err != nil {
			return nil, fmt.Errorf("parse cache: %v", err)
		}

		return refs, nil
	} else {
		var refs []interface{}
		for _, selectPropRef := range selectProp.Refs {
			innerProperties := selectPropRef.RefProperties
			perClass, err := r.resolveRefsWithoutCache(input, selectPropRef.ClassName, innerProperties)
			if err != nil {
				return nil, fmt.Errorf("resolve without cache: %v", err)
			}

			refs = append(refs, perClass...)
		}
		return refs, nil
	}

}

func (r *Repo) resolveRefsWithoutCache(input []interface{},
	desiredClass string, innerProperties traverser.SelectProperties) ([]interface{}, error) {
	var output []interface{}
	for i, item := range input {
		resolved, err := r.resolveRefWithoutCache(item, desiredClass, innerProperties)
		if err != nil {
			return nil, fmt.Errorf("at position %d: %v", i, err)
		}

		if resolved == nil {
			continue
		}

		output = append(output, *resolved)
	}

	return output, nil
}

func (r *Repo) resolveRefWithoutCache(item interface{}, desiredClass string,
	innerProperties traverser.SelectProperties) (*get.LocalRef, error) {
	var out get.LocalRef

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
		res, err := r.ThingByID(context.TODO(), ref.TargetID, innerProperties)
		if err != nil {
			return nil, err
		}

		if res.ClassName != desiredClass {
			return nil, nil
		}

		out.Class = res.ClassName
		out.Fields = res.Schema.(map[string]interface{})
		return &out, nil
	case kind.Action:
		res, err := r.ActionByID(context.TODO(), ref.TargetID, innerProperties)
		if err != nil {
			return nil, err
		}

		if res.ClassName != desiredClass {
			return nil, nil
		}

		out.Class = res.ClassName
		out.Fields = res.Schema.(map[string]interface{})
		return &out, nil
	default:
		return nil, fmt.Errorf("impossible kind: %v", ref.Kind)
	}
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

func (r *Repo) parseCacheSchemaToRefs(in map[string]interface{}, prop string, depth int,
	selectProp traverser.SelectProperty) ([]interface{}, error) {

	// TODO: investigate why sometimes prop is nil, but sometmes slice of len 0
	// See test schema with Plane OfAirline
	if in[prop] == nil {
		return nil, nil
	}

	var out []interface{}
	refClassesMap, ok := in[prop].(map[string]interface{})
	if !ok {
		// not a ref
		return nil, fmt.Errorf("prop %s: not a map: %#v", prop, in[prop])
	}

	for className, refs := range refClassesMap {

		// did the user specify this prop?
		ok, _ := (traverser.SelectProperties{selectProp}).ShouldResolve([]string{uppercaseFirstLetter(prop), className})
		if !ok {
			// no, then there's nothing to do here
			continue
		}

		refsSlice, ok := refs.([]interface{})
		if !ok {
			return nil, fmt.Errorf("prop %s: expected refs to be slice, but got %#v", prop, refs)
		}

		for _, ref := range refsSlice {
			refMap, ok := ref.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("prop %s: expected ref item to be map, but got %#v", prop, refs)
			}

			selectClass := selectProp.FindSelectClass(schema.ClassName(className))
			//selectClass can never be nil, because we already verified that this
			//combination is present, otherwise we would have skiped the loop
			//iteration

			parsed, err := r.parseInnerRefFromCache(refMap, depth+1, *selectClass)
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

func (r *Repo) parseInnerRefFromCache(in map[string]interface{}, depth int,
	selectClass traverser.SelectClass) (map[string]interface{}, error) {
	out := map[string]interface{}{}

	for prop, value := range in {

		if m, ok := value.(map[string]interface{}); ok {
			mp, err := parseMapProp(m)
			if err == nil {
				// this was probably a geo prop
				out[prop] = mp
				continue
			}
			innerSelectProp := selectClass.RefProperties.FindProperty(uppercaseFirstLetter(prop))
			if innerSelectProp == nil {
				// the user is not interested in this prop
				continue
			}

			// we have a map that could not be parsed into a known map prop, such as
			// a geo prop, therefore it must be the structure of an already cached
			// ref

			parsed, err := r.parseCacheSchemaToRefs(in, prop, depth, *innerSelectProp)
			if err != nil {
				return nil, err
			}

			if len(parsed) > 0 {
				out[uppercaseFirstLetter(prop)] = parsed
			}
			continue
		}

		if list, ok := value.([]interface{}); ok {
			// a list is an indication of unresolved refs which we need to resolve at
			// runtime if they user is interested in them
			innerSelectProp := selectClass.RefProperties.FindProperty(uppercaseFirstLetter(prop))
			if innerSelectProp == nil {
				// the user is not interested in this prop
				continue
			}

			if len(innerSelectProp.Refs) == 0 {
				continue
			}

			var resolved []interface{}
			for _, selectPropRef := range innerSelectProp.Refs {
				innerProperties := selectPropRef.RefProperties
				perClass, err := r.resolveRefsWithoutCache(list, selectPropRef.ClassName, innerProperties)
				if err != nil {
					return nil, fmt.Errorf("resolve without cache, because cache boundary is crossed: %v", err)
				}

				resolved = append(resolved, perClass...)
			}

			if len(resolved) > 0 {
				out[uppercaseFirstLetter(prop)] = resolved
			}
			continue
		}

		out[prop] = value
	}

	return out, nil
}
