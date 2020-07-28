package refcache

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type Resolver struct {
	cacher cacher
}

type cacher interface {
	Build(ctx context.Context, objects []search.Result, properties traverser.SelectProperties, meta bool) error
	Get(si multi.Identifier) (search.Result, bool)
}

func NewResolver(cacher cacher) *Resolver {
	return &Resolver{cacher: cacher}
}

func (r *Resolver) Do(ctx context.Context, objects []search.Result,
	properties traverser.SelectProperties, meta bool) ([]search.Result, error) {
	if err := r.cacher.Build(ctx, objects, properties, meta); err != nil {
		return nil, errors.Wrap(err, "build reference cache")
	}

	return r.parseObjects(objects, properties, meta)
}

func (r *Resolver) parseObjects(objects []search.Result, properties traverser.SelectProperties,
	meta bool) ([]search.Result, error) {

	for i, obj := range objects {
		parsed, err := r.parseObject(obj, properties, meta)
		if err != nil {
			return nil, errors.Wrapf(err, "parse at position %d", i)
		}

		objects[i] = parsed
	}

	return objects, nil
}

func (r *Resolver) parseObject(object search.Result, properties traverser.SelectProperties,
	meta bool) (search.Result, error) {

	if object.Schema == nil {
		return object, nil
	}

	schemaMap, ok := object.Schema.(map[string]interface{})
	if !ok {
		return object, fmt.Errorf("schema is not a map: %T", object.Schema)
	}

	schema, err := r.parseSchema(schemaMap, properties, meta)
	if err != nil {
		return object, err
	}

	object.Schema = schema
	return object, nil
}

func (r *Resolver) parseSchema(schema map[string]interface{}, properties traverser.SelectProperties,
	meta bool) (map[string]interface{}, error) {
	for propName, value := range schema {

		refs, ok := value.(models.MultipleRef)
		if !ok {
			// not a ref, not interesting for us
			continue
		}

		// ref keys are uppercased in the desired response
		refKey := uppercaseFirstLetter(propName)
		selectProp := properties.FindProperty(refKey)
		if selectProp == nil {
			// user is not interested in this prop
			continue
		}

		parsed, err := r.parseRefs(refs, propName, *selectProp)
		if err != nil {
			return schema, errors.Wrapf(err, "parse refs for prop %q", propName)
		}

		if parsed != nil {
			schema[uppercaseFirstLetter(propName)] = parsed
		}
		delete(schema, propName) // we have the uppercased/resolved now. No more need for the unresolved
	}

	return schema, nil
}

func (r *Resolver) parseRefs(input models.MultipleRef, prop string,
	selectProp traverser.SelectProperty) ([]interface{}, error) {
	var refs []interface{}
	for _, selectPropRef := range selectProp.Refs {
		innerProperties := selectPropRef.RefProperties
		perClass, err := r.resolveRefs(input, selectPropRef.ClassName, innerProperties)
		if err != nil {
			return nil, errors.Wrap(err, "resolve ref")
		}

		refs = append(refs, perClass...)
	}
	return refs, nil
}

func (r *Resolver) resolveRefs(input models.MultipleRef, desiredClass string,
	innerProperties traverser.SelectProperties) ([]interface{}, error) {
	var output []interface{}
	for i, item := range input {
		resolved, err := r.resolveRef(item, desiredClass, innerProperties)
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
	innerProperties traverser.SelectProperties) (*search.LocalRef, error) {
	var out search.LocalRef

	ref, err := crossref.Parse(item.Beacon.String())
	if err != nil {
		return nil, err
	}

	si := multi.Identifier{
		ID:        ref.TargetID.String(),
		ClassName: desiredClass,
		Kind:      ref.Kind,
	}
	res, ok := r.cacher.Get(si)
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
	schema := res.Schema.(map[string]interface{})
	nested, err := r.parseSchema(schema, innerProperties, false)
	if err != nil {
		return nil, errors.Wrap(err, "resolve nested ref")
	}

	out.Fields = nested

	return &out, nil
}
