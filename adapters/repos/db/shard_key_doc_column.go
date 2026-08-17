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

package db

import (
	"slices"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// keyDocColumnConfigured reports whether this shard's collection asked for a
// resident key/doc column on propName.
//
// The configuration names collections and, within them, properties: a collection
// listed with properties enables exactly those, and [config.AllProperties]
// enables every property it has. Nothing is enabled by default — the index
// trades memory for ContainsAny speed and assumes near-unique values, so it is
// opted into per property rather than inferred from the schema.
func (s *Shard) keyDocColumnConfigured(propName string) bool {
	configured, ok := s.index.Config.IndexFilterableKeyDocColumn[s.index.Config.ClassName.String()]
	if !ok {
		return false
	}
	// A collection with no properties at all enables none: the environment refuses
	// that shape rather than guess, so this only decides what happens if the
	// configuration is built some other way, and a resident index is the wrong
	// thing to hand somebody by default.
	if slices.Contains(configured, config.AllProperties) {
		return true
	}
	// Against the name the schema stores rather than the one that was typed: a
	// property written "SKU" is held as "sKU", and the query path normalises too,
	// so comparing raw would configure nothing while the filter found the
	// property.
	for _, name := range configured {
		if schema.LowercaseFirstLetter(name) == propName {
			return true
		}
	}
	return false
}
