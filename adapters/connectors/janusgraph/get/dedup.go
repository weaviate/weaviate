/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package get

import (
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get"
)

type byID map[string][]interface{}
type refByID map[string][]interface{}

// nestedDedup does not check whether type assertions are valid, because it
// assumes that the results come from the query processor where everything has
// already been verified
func nestedDedup(orig []interface{}) []interface{} {

	idMap := byID{}
	for _, item := range orig {
		itemMap := item.(map[string]interface{})
		id := itemMap["uuid"].(string)
		idMap[id] = append(idMap[id], itemMap)
	}

	return idMap.merge()
}

func (m byID) merge() []interface{} {
	var res []interface{}

	for _, value := range m {
		merged := map[string]interface{}{}
		for _, oneMap := range value {
			merged = m.mergeMaps(merged, oneMap.(map[string]interface{}))
		}

		// now we have successfully merged the outer-most level. before returning
		// the results, see if we contain cross-refs and therefore need to go
		// deeper
		merged = mergeInnerProps(merged)

		res = append(res, merged)
	}

	return res
}

// if a key is present in both m1 and m2, it is either over-written (primitive
// types) or appended (slice type)
func (m byID) mergeMaps(m1, m2 map[string]interface{}) map[string]interface{} {
	for k, v := range m2 {
		m1value, ok := m1[k]
		if ok {
			// this key also exists in m1
			switch m1value.(type) {
			case []interface{}:
				// cool it is a slice, we can append to it
				m1[k] = append(m1[k].([]interface{}), m2[k].([]interface{})...)
			default:
				// for any other type, let's over-write
				m1[k] = v
			}
		} else {
			m1[k] = v
		}
	}

	return m1
}

func mergeInnerProps(inner map[string]interface{}) map[string]interface{} {
	for k, v := range inner {
		switch l := v.(type) {
		case []interface{}:
			inner[k] = dedupInner(l)
		}
	}

	return inner
}

func dedupInner(l []interface{}) []interface{} {
	idMap := refByID{}
	for _, item := range l {
		switch ref := item.(type) {
		case get.LocalRef:
			id := ref.Fields["uuid"].(string)
			idMap[id] = append(idMap[id], ref)
		case get.NetworkRef:
			idMap[string(ref.ID)] = []interface{}{ref}

		}
	}

	return idMap.merge()
}

// merge is like byID.merge(), only that it merges localrefs instead of
// primitive maps
func (m refByID) merge() []interface{} {
	var res []interface{}

	for _, value := range m {
		merged := get.LocalRef{
			Fields: map[string]interface{}{},
		}

		shouldInclude := false
		for _, oneRef := range value {
			if localRef, ok := oneRef.(get.LocalRef); ok {
				merged = m.mergeRefs(merged, localRef)
				shouldInclude = true
			} else {
				res = append(res, oneRef)
			}
		}

		// now we have successfully merged the this level. before returning
		// the results, see if we contain cross-refs and therefore need to go
		// deeper
		merged.Fields = mergeInnerProps(merged.Fields)
		if shouldInclude {
			res = append(res, merged)
		}
	}

	return res
}

func (m refByID) mergeRefs(r1, r2 get.LocalRef) get.LocalRef {
	m1 := r1.Fields
	m2 := r2.Fields
	for k, v := range m2 {
		m1value, ok := m1[k]
		if ok {
			// this key also exists in m1
			switch m1value.(type) {
			case []interface{}:
				// cool it is a slice, we can append to it
				m1[k] = append(m1[k].([]interface{}), m2[k].([]interface{})...)
			default:
				// for any other type, let's over-write
				m1[k] = v
			}
		} else {
			m1[k] = v
		}
	}

	return get.LocalRef{
		Class:  r2.Class,
		Fields: m1,
	}
}
