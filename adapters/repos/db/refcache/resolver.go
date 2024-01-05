//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package refcache

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
)

type Resolver struct {
	cacher cacher
	// for groupBy feature
	withGroup                bool
	getGroupSelectProperties func(properties search.SelectProperties) search.SelectProperties
}

type cacher interface {
	Build(ctx context.Context, objects []search.Result, properties search.SelectProperties, additional additional.Properties) error
	Get(si multi.Identifier) (search.Result, bool)
}

func NewResolver(cacher cacher) *Resolver {
	return &Resolver{cacher: cacher}
}

func NewResolverWithGroup(cacher cacher) *Resolver {
	return &Resolver{
		cacher: cacher,
		// for groupBy feature
		withGroup:                true,
		getGroupSelectProperties: getGroupSelectProperties,
	}
}

func (r *Resolver) Do(ctx context.Context, objects []search.Result,
	properties search.SelectProperties, additional additional.Properties,
) ([]search.Result, error) {
	if err := r.cacher.Build(ctx, objects, properties, additional); err != nil {
		return nil, errors.Wrap(err, "build reference cache")
	}

	return r.parseObjects(objects, properties, additional)
}

func (r *Resolver) parseObjects(objects []search.Result, properties search.SelectProperties,
	additional additional.Properties,
) ([]search.Result, error) {
	for i, obj := range objects {
		parsed, err := r.parseObject(obj, properties, additional)
		if err != nil {
			return nil, errors.Wrapf(err, "parse at position %d", i)
		}

		objects[i] = parsed
	}

	return objects, nil
}

func (r *Resolver) parseObject(object search.Result, properties search.SelectProperties,
	additional additional.Properties,
) (search.Result, error) {
	if object.Schema == nil {
		return object, nil
	}

	schemaMap, ok := object.Schema.(map[string]interface{})
	if !ok {
		return object, fmt.Errorf("schema is not a map: %T", object.Schema)
	}

	schema, err := r.parseSchema(schemaMap, properties)
	if err != nil {
		return object, err
	}

	object.Schema = schema

	if r.withGroup {
		additionalProperties, err := r.parseAdditionalGroup(object.AdditionalProperties, properties)
		if err != nil {
			return object, err
		}
		object.AdditionalProperties = additionalProperties
	}
	return object, nil
}

func (r *Resolver) parseAdditionalGroup(
	additionalProperties models.AdditionalProperties,
	properties search.SelectProperties,
) (models.AdditionalProperties, error) {
	if additionalProperties != nil && additionalProperties["group"] != nil {
		if group, ok := additionalProperties["group"].(*additional.Group); ok {
			for j, hit := range group.Hits {
				schema, err := r.parseSchema(hit, r.getGroupSelectProperties(properties))
				if err != nil {
					return additionalProperties, fmt.Errorf("resolve group hit: %w", err)
				}
				group.Hits[j] = schema
			}
		}
	}
	return additionalProperties, nil
}

func (r *Resolver) parseSchema(schema map[string]interface{},
	properties search.SelectProperties,
) (map[string]interface{}, error) {
	for propName, value := range schema {
		refs, ok := value.(models.MultipleRef)
		if !ok {
			// not a ref, not interesting for us
			continue
		}

		selectProp := properties.FindProperty(propName)
		if selectProp == nil {
			// user is not interested in this prop
			continue
		}

		parsed, err := r.parseRefs(refs, propName, *selectProp)
		if err != nil {
			return schema, errors.Wrapf(err, "parse refs for prop %q", propName)
		}

		if parsed != nil {
			schema[propName] = parsed
		}
	}

	return schema, nil
}

func (r *Resolver) parseRefs(input models.MultipleRef, prop string,
	selectProp search.SelectProperty,
) ([]interface{}, error) {
	var refs []interface{}
	for _, selectPropRef := range selectProp.Refs {
		innerProperties := selectPropRef.RefProperties
		additionalProperties := selectPropRef.AdditionalProperties
		perClass, err := r.resolveRefs(input, selectPropRef.ClassName, innerProperties, additionalProperties)
		if err != nil {
			return nil, errors.Wrap(err, "resolve ref")
		}

		refs = append(refs, perClass...)
	}
	return refs, nil
}

func (r *Resolver) resolveRefs(input models.MultipleRef, desiredClass string,
	innerProperties search.SelectProperties,
	additionalProperties additional.Properties,
) ([]interface{}, error) {
	var output []interface{}
	for i, item := range input {
		resolved, err := r.resolveRef(item, desiredClass, innerProperties, additionalProperties)
		if err != nil {
			return nil, errors.Wrapf(err, "at position %d", i)
		}

		if resolved == nil {
			continue
		}

		output = append(output, *resolved)
	}

	return output, nil
}

func (r *Resolver) resolveRef(item *models.SingleRef, desiredClass string,
	innerProperties search.SelectProperties,
	additionalProperties additional.Properties,
) (*search.LocalRef, error) {
	var out search.LocalRef

	ref, err := crossref.Parse(item.Beacon.String())
	if err != nil {
		return nil, err
	}

	si := multi.Identifier{
		ID:        ref.TargetID.String(),
		ClassName: desiredClass,
	}
	res, ok := r.cacher.Get(si)
	if !ok {
		// silently ignore, could have been deleted in the meantime, or we're
		// asking for a non-matching selectProperty, for example if we ask for
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
	schema := res.Schema.(map[string]interface{})
	nested, err := r.parseSchema(schema, innerProperties)
	if err != nil {
		return nil, errors.Wrap(err, "resolve nested ref")
	}

	if additionalProperties.Vector {
		nested["vector"] = res.Vector
	}
	if additionalProperties.CreationTimeUnix {
		nested["creationTimeUnix"] = res.Created
	}
	if additionalProperties.LastUpdateTimeUnix {
		nested["lastUpdateTimeUnix"] = res.Updated
	}
	out.Fields = nested

	return &out, nil
}
