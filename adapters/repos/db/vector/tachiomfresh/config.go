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

package tachiomfresh

import (
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/schema/config"
	ent "github.com/weaviate/weaviate/entities/vectorindex/tachiomfresh"
)

// ValidateUserConfigUpdate refuses every change except the two search-time
// budgets, kCentroids and kDocsToScore. UpdateUserConfig applies those
// under the atomics the search reads. The shape of
// hfresh.ValidateUserConfigUpdate. The posting size shapes the index on
// disk, and efConstruction shapes the centroid graph. A change to either
// would need a rebuild, so it is refused.
func ValidateUserConfigUpdate(initial, updated config.VectorIndexConfig) error {
	initialParsed, ok := initial.(ent.UserConfig)
	if !ok {
		return errors.Errorf("initial is not UserConfig, but %T", initial)
	}

	updatedParsed, ok := updated.(ent.UserConfig)
	if !ok {
		return errors.Errorf("updated is not UserConfig, but %T", updated)
	}

	immutableFields := []immutableParameter{
		{
			name:     "distance",
			accessor: func(c ent.UserConfig) interface{} { return c.Distance },
		},
		{
			name:     "maxPostingSize",
			accessor: func(c ent.UserConfig) interface{} { return c.MaxPostingSize },
		},
		{
			name:     "efConstruction",
			accessor: func(c ent.UserConfig) interface{} { return c.EFConstruction },
		},
		{
			name:     "multivector.enabled",
			accessor: func(c ent.UserConfig) interface{} { return c.Multivector.Enabled },
		},
		{
			name:     "multivector.aggregation",
			accessor: func(c ent.UserConfig) interface{} { return c.Multivector.Aggregation },
		},
		{
			name:     "multivector.muvera.enabled",
			accessor: func(c ent.UserConfig) interface{} { return c.Multivector.MuveraConfig.Enabled },
		},
	}

	for _, u := range immutableFields {
		if err := validateImmutableField(u, initialParsed, updatedParsed); err != nil {
			return err
		}
	}

	return nil
}

type immutableParameter struct {
	accessor func(c ent.UserConfig) interface{}
	name     string
}

func validateImmutableField(u immutableParameter,
	previous, next ent.UserConfig,
) error {
	oldField := u.accessor(previous)
	newField := u.accessor(next)
	if oldField != newField {
		return errors.Errorf("%s is immutable: attempted change from \"%v\" to \"%v\"",
			u.name, oldField, newField)
	}

	return nil
}
