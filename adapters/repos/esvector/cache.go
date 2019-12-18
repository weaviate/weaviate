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
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (r *Repo) PopulateCache(ctx context.Context, kind kind.Kind, id strfmt.UUID) error {
	manager := newCacheManager(r)
	_, err := manager.populate(ctx, kind, id, 0)
	if err != nil {
		return fmt.Errorf("populate cache for %s with id %s: %v", kind.Name(), id, err)
	}

	return nil
}

type cacheManager struct {
	repo *Repo
}

func newCacheManager(r *Repo) *cacheManager {
	return &cacheManager{r}
}

type refClassAndSchema struct {
	class  string
	schema map[string]interface{}
}

func (c *cacheManager) populate(ctx context.Context, kind kind.Kind, id strfmt.UUID, depth int) (*search.Result, error) {
	obj, err := c.getObject(ctx, kind, id)
	if err != nil {
		return nil, err
	}

	if obj == nil {
		return nil, fmt.Errorf("%s with id '%s' not found", kind.Name(), id)
	}

	if obj.CacheHot {
		res := c.prepareForStoringAsCache(obj, depth)
		return res, nil
	}

	resolvedSchema := map[string]interface{}{}
	schemaMap := obj.Schema.(map[string]interface{})
	for prop, value := range schemaMap {
		if gc, ok := value.(*models.GeoCoordinates); ok {
			resolvedSchema[prop] = map[string]interface{}{
				"lat": gc.Latitude,
				"lon": gc.Longitude,
			}
			continue
		}

		if _, ok := value.(*models.MultipleRef); ok {
			return nil, fmt.Errorf("if you see this message you have found a bug in weaviate" +
				", congrutulations! please open an issue at github.com/semi-technologies/weaviate" +
				" with the following error: found *models.MultipleRef in cache population, but" +
				" expected to only ever see models.MultipleRef")
		}

		refs, ok := value.(models.MultipleRef)
		if ok {
			if depth+1 > c.repo.denormalizationDepthLimit {
				// too deep to resolve, return unresolved instead
				resolvedSchema[prop] = value
				continue
			}

			if len(refs) > c.repo.superNodeThreshold {
				resolvedSchema[prop] = value
				continue
			}

			resolvedRefs, err := c.resolveRefs(ctx, refs, depth+1)
			if err != nil {
				return nil, err
			}

			resolvedSchema[prop] = groupRefByClassType(resolvedRefs)
			continue
		}

		resolvedSchema[prop] = value
	}

	obj.Schema = resolvedSchema

	if depth == 0 {
		// only ever store the outermost layer. The indexer will make sure that
		// every class will be resolved once. So, by storing inner layers we'd
		// essentially cut the inner object short. Imagine the following chain:
		//
		// Place->inCity->City->inCountry->Country->onContinent->Continent
		//
		// Assume a depth limit of 3. The indexer will call every one of those
		// once. So when it called the "City" it resolved three levels deep,
		// i.e. City->inCountry->Country->onContinent->Continent. That is exactly
		// the cache depth we want to have for this object.
		//
		// However, when we resolve the Place, the deepest we would go is to the
		// Country, as Continent would be 4 levels deep and therefore deper than
		// the maximum depth. If we were to store each inner item, we would
		// overwrite the City (which already had a perfect cache of 3 levels), with
		// the City from the perspective of the the Place. which only goes up until
		// the Country, but never to the Continent.
		if err := c.repo.upsertCache(ctx, id.String(), obj.Kind, obj.ClassName, resolvedSchema); err != nil {
			return nil, err
		}
	}

	return obj, nil
}

func (c *cacheManager) getObject(ctx context.Context, k kind.Kind, id strfmt.UUID) (*search.Result, error) {
	switch k {
	case kind.Thing:
		// empty selectproperties make sure that we don't resolve any refs
		return c.repo.ThingByID(ctx, id, traverser.SelectProperties{}, false)
	case kind.Action:
		// empty selectproperties make sure that we don't resolve any refs
		return c.repo.ActionByID(ctx, id, traverser.SelectProperties{}, false)
	default:
		return nil, fmt.Errorf("impossible kind: %v", k)
	}
}

func (c *cacheManager) resolveRefs(ctx context.Context, refs models.MultipleRef, depth int) ([]refClassAndSchema, error) {
	var resolvedRefs []refClassAndSchema

	refSlice := []*models.SingleRef(refs)
	for _, ref := range refSlice {
		details, err := crossref.Parse(ref.Beacon.String())
		if err != nil {
			return nil, fmt.Errorf("parse %s: %v", ref.Beacon, err)
		}

		innerRef, err := c.populate(ctx, details.Kind, details.TargetID, depth)
		if err != nil {
			return nil, fmt.Errorf("populate %s: %v", ref.Beacon, err)
		}

		if innerRef.Schema == nil {
			continue
		}

		resolvedRefs = append(resolvedRefs, refClassAndSchema{
			class:  innerRef.ClassName,
			schema: innerRef.Schema.(map[string]interface{}),
		})
	}

	return resolvedRefs, nil
}

func groupRefByClassType(refs []refClassAndSchema) map[string]interface{} {
	output := map[string]interface{}{}
	for _, ref := range refs {
		if slice, ok := output[ref.class]; ok {
			output[ref.class] = append(slice.([]interface{}), ref.schema)
		} else {
			output[ref.class] = []interface{}{ref.schema}
		}
	}

	return output
}

func (r *Repo) upsertCache(ctx context.Context, id string, k kind.Kind,
	className string, cache map[string]interface{}) error {
	// copy otherwise we modify the original when adding the cacheHot field
	cacheCopy := copyMap(cache)
	cacheCopy[keyCacheHot.String()] = true

	body := map[string]interface{}{
		"doc": map[string]interface{}{
			keyCache.String(): cacheCopy,
		},
	}

	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return fmt.Errorf("upsert cache: encode json: %v", err)
	}

	req := esapi.UpdateRequest{
		Index:      classIndexFromClassName(k, className),
		DocumentID: id,
		Body:       &buf,
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("upsert cache: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		r.logger.WithField("action", "vector_index_upsert_cache").
			WithError(err).
			WithField("request", req).
			WithField("res", res).
			WithField("body_before_marshal", body).
			WithField("body", buf.String()).
			Errorf("upsert cache failed")

		return fmt.Errorf("upsert cache: %v", err)
	}

	return nil
}

func copyMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})
	for key, value := range in {
		out[key] = value
	}

	return out
}

// this function is only ever called if we had a hot cache. calling it will
// prepare the output for another insertion and will itself take refs from
// cache if it has any or leave them untouched if not cached
func (c *cacheManager) prepareForStoringAsCache(in *search.Result, depth int) *search.Result {
	schema := map[string]interface{}{}
	limit := c.repo.denormalizationDepthLimit

	for prop, value := range in.Schema.(map[string]interface{}) {
		switch v := value.(type) {
		case *models.GeoCoordinates:
			// geoocordniates need to be prepared for ES
			schema[prop] = map[string]interface{}{
				"lat": v.Latitude,
				"lon": v.Longitude,
			}

		case models.MultipleRef:
			// refs need to be taken from cache if present or left untouched otherwise
			if ref, ok := in.CacheSchema[prop]; ok && depth < limit {
				schema[prop] = c.unresolveIfTooDeep(ref, depth+1)
			} else {
				schema[prop] = value
			}
		default:
			// primitive props are good to go
			schema[prop] = value
		}
	}

	in.Schema = schema
	return in
}

// assume the following situation: the depth limit is two and you have a fully
// cached object going two levels deep with a hot cache. Now a third object is
// added referencing the original object. Since the cache of the referenced
// object is hot, we don't need to do any additional resolving. However, we
// can't just copy over the ref tree either, as it is already 2 levels deep. In
// other words we'd end up with a three level deep cache. Over large schemas
// this seemingly small inconvenience adds up to massive issues, see
// https://github.com/semi-technologies/weaviate/issues/1003 and
// https://github.com/semi-technologies/weaviate/issues/1004.
//
// To remedy this, we need to parse the hot cache from the referenced object
// recursively and undo any resolved cache once wewould otherwise cross the
// cache boundary
func (c *cacheManager) unresolveIfTooDeep(ref interface{}, depth int) interface{} {
	limit := c.repo.denormalizationDepthLimit

	// exit condition: unresolve this one
	if depth > limit {
		return c.unresolveRef(ref)
	}

	// we are expecting a resolved ref, i.e. a map
	refMap, ok := ref.(map[string]interface{})
	if !ok {
		return ref
	}

	// this ref map should have one key per target class
	for targetClass, refValue := range refMap {
		// each target class should have 0..n actual references, i.e. be a slice
		refSlice, ok := refValue.([]interface{})
		if !ok {
			continue
		}

		for i, refItem := range refSlice {
			innerRefMap, ok := refItem.(map[string]interface{})
			if !ok {
				// not a ref prop, so we don't need to alter it, leave untouched
				continue
			}

			for prop, value := range innerRefMap {
				valueMap, ok := value.(map[string]interface{})
				if !ok {
					// not a ref prop, so we don't need to alter it, leave untouched
					continue
				}

				// we need to rule out that this is a geo prop, which would also be saved
				// as a map
				_, latOK := valueMap["lat"]
				_, lonOK := valueMap["lon"]
				if latOK && lonOK {
					// clearly a geo prop, leave untouched
					continue
				}

				// a map which is not a geo prop can only be a resolved ref prop
				innerRefMap[prop] = c.unresolveIfTooDeep(value, depth+1)
				refSlice[i] = innerRefMap
				refMap[targetClass] = refSlice
				ref = refMap
			}
		}
	}

	return ref
}

func (c *cacheManager) unresolveRef(ref interface{}) []interface{} {
	var out []interface{}

	refMap, ok := ref.(map[string]interface{})
	if !ok {
		return out
	}

	for targetClass, value := range refMap {
		valueSlice, ok := value.([]interface{})
		if !ok {
			continue
		}

		for _, item := range valueSlice {
			valueMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}

			uuid, ok := valueMap["uuid"]
			if !ok {
				continue
			}

			out = append(out, map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/%ss/%s", c.kindOfClass(targetClass), uuid),
			})
		}
	}

	return out
}

func (c *cacheManager) kindOfClass(className string) string {
	s := c.repo.schemaGetter.GetSchemaSkipAuth()
	kind, _ := s.GetKindOfClass(schema.ClassName(className))
	return kind.Name()
}
