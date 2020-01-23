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
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// parseSchema lightly parses the schema, while most fields stay untyped, those
// with special meaning, such as GeoCoordinates are marshalled into their
// required types
func (r *Repo) parseSchema(input map[string]interface{}, properties traverser.SelectProperties,
	meta bool, requestCacher *cacher) (map[string]interface{}, error) {

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
					refMap := ref.(map[string]interface{})
					singleRef := &models.SingleRef{
						Beacon: strfmt.URI(refMap["beacon"].(string)),
					}

					if meta {
						singleRef.Meta = parseRefMeta(refMap)
					}
					refs = append(refs, singleRef)
				}

				output[key] = models.MultipleRef(refs)
				continue
			}

			// ref keys are uppercased in the desired response
			refKey := uppercaseFirstLetter(key)
			selectProp := properties.FindProperty(refKey)
			if selectProp == nil {
				// user is not interested in this prop
				continue
			}

			parsed, err := r.parseRefs(typed, key, *selectProp, requestCacher)
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

func parseRefMeta(ref map[string]interface{}) *models.ReferenceMeta {
	meta, ok := ref[keyMeta.String()]
	if !ok {
		return nil
	}

	classification := meta.(map[string]interface{})[keyMetaClassification.String()]
	if classification == nil {
		// for now classification is the only viable meta option so if it's not set
		// we can return early. If other options are added in the future, we need
		// to check them too
		return nil
	}

	asMap := classification.(map[string]interface{})
	classificationOutput := &models.ReferenceMetaClassification{}
	winningDistance, ok := asMap[keyMetaClassificationWinningDistance.String()]
	if ok {
		classificationOutput.WinningDistance = winningDistance.(float64)
	}

	losingDistance, ok := asMap[keyMetaClassificationLosingDistance.String()]
	if ok {
		d := losingDistance.(float64)
		classificationOutput.LosingDistance = &d
	}

	return &models.ReferenceMeta{
		Classification: classificationOutput,
	}
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

func (r *Repo) parseRefs(input []interface{}, prop string, selectProp traverser.SelectProperty, requestCacher *cacher) ([]interface{}, error) {
	var refs []interface{}
	for _, selectPropRef := range selectProp.Refs {
		innerProperties := selectPropRef.RefProperties
		perClass, err := r.resolveRefs(input, selectPropRef.ClassName, innerProperties, requestCacher)
		if err != nil {
			return nil, fmt.Errorf("resolve without cache: %v", err)
		}

		refs = append(refs, perClass...)
	}
	return refs, nil
}

func (r *Repo) resolveRefs(input []interface{},
	desiredClass string, innerProperties traverser.SelectProperties, requestCacher *cacher) ([]interface{}, error) {
	var output []interface{}
	for i, item := range input {
		resolved, err := r.resolveRef(item, desiredClass, innerProperties, requestCacher)
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

func (r *Repo) resolveRef(item interface{}, desiredClass string,
	innerProperties traverser.SelectProperties, requestCacher *cacher) (*search.LocalRef, error) {
	var out search.LocalRef

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

	si := storageIdentifier{
		id:        ref.TargetID.String(),
		className: desiredClass,
		kind:      ref.Kind,
	}
	res, ok := requestCacher.get(si)
	if !ok {
		// silently ignore, could have been deleted in the meantime, or we're
		// asking for a non-matching selectProperty, for eaxmple if we ask for
		// Article { published { ... on { Magazine { name } ... on { Journal { name } }
		// we don't know at resolve time if this ID will point to a Magazine or a
		// Journal, so we will get a few empty responses when trying both for any
		// given ID.
		//
		// In turn this means we need to validate through automated and explorative
		// tests, that we never skip results that should be contained, as we
		// wouldn't throw an error, so the user would never notice
		return nil, nil
	}

	out.Class = res.ClassName
	out.Fields = res.Schema.(map[string]interface{})
	return &out, nil

	// switch ref.Kind {
	// case kind.Thing:
	// 	res, err := r.ThingByID(context.TODO(), ref.TargetID, innerProperties, false)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	if res.ClassName != desiredClass {
	// 		return nil, nil
	// 	}

	// 	out.Class = res.ClassName
	// 	out.Fields = res.Schema.(map[string]interface{})
	// 	return &out, nil
	// case kind.Action:
	// 	res, err := r.ActionByID(context.TODO(), ref.TargetID, innerProperties, false)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	if res.ClassName != desiredClass {
	// 		return nil, nil
	// 	}

	// 	out.Class = res.ClassName
	// 	out.Fields = res.Schema.(map[string]interface{})
	// 	return &out, nil
	// default:
	// 	return nil, fmt.Errorf("impossible kind: %v", ref.Kind)
	// }
}

type cache struct {
	hot    bool
	schema map[string]interface{}
}

func (r *Repo) extractMeta(in map[string]interface{}) *models.ObjectMeta {
	objectMetaField, ok := in[keyObjectMeta.String()]
	if !ok {
		return nil
	}

	objectMetaMap, ok := objectMetaField.(map[string]interface{})
	if !ok {
		return nil
	}

	classificationField, ok := objectMetaMap[keyMetaClassification.String()]
	if !ok {
		// for now classification is the only meta field, so we can return early if
		// no classification is set. If a second meta type is added in the future,
		// we need to check for those as well
		return nil
	}

	classificationMap, ok := classificationField.(map[string]interface{})
	if !ok {
		return nil
	}

	classification := &models.ObjectMetaClassification{}
	if id, ok := classificationMap["id"]; ok {
		classification.ID = strfmt.UUID(id.(string))
	}

	if completed, ok := classificationMap["completed"]; ok {
		t, err := strfmt.ParseDateTime(completed.(string))
		if err == nil {
			classification.Completed = t
		}
	}

	if scope, ok := classificationMap["scope"]; ok {
		classification.Scope = interfaceToStringSlice(scope.([]interface{}))
	}

	if classified, ok := classificationMap["classifiedFields"]; ok {
		classification.ClassifiedFields = interfaceToStringSlice(classified.([]interface{}))
	}

	return &models.ObjectMeta{
		Classification: classification,
	}
}

func uppercaseFirstLetter(in string) string {
	first := string(in[0])
	rest := string(in[1:])

	return strings.ToUpper(first) + rest
}
