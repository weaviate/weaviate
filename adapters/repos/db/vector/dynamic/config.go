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

package dynamic

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/schema"
	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type Config struct {
	ID                       string
	TargetVector             string
	Logger                   logrus.FieldLogger
	RootPath                 string
	ShardName                string
	ClassName                string
	PrometheusMetrics        *monitoring.PrometheusMetrics
	VectorForIDThunk         common.VectorForID[float32]
	TempVectorForIDThunk     common.TempVectorForID
	DistanceProvider         distancer.Provider
	MakeCommitLoggerThunk    hnsw.MakeCommitLogger
	TombstoneCallbacks       cyclemanager.CycleCallbackGroup
	ShardCompactionCallbacks cyclemanager.CycleCallbackGroup
	ShardFlushCallbacks      cyclemanager.CycleCallbackGroup
}

func (c Config) Validate() error {
	ec := &errorcompounder.ErrorCompounder{}

	if c.ID == "" {
		ec.Addf("id cannot be empty")
	}

	if c.DistanceProvider == nil {
		ec.Addf("distancerProvider cannot be nil")
	}

	return ec.ToError()
}

func ValidateUserConfigUpdate(initial, updated schema.VectorIndexConfig) error {
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
	}

	for _, u := range immutableFields {
		if err := validateImmutableField(u, initialParsed, updatedParsed); err != nil {
			return err
		}
	}
	if err := flat.ValidateUserConfigUpdate(initialParsed.FlatUC, updatedParsed.FlatUC); err != nil {
		return err
	}
	if err := hnsw.ValidateUserConfigUpdate(initialParsed.HnswUC, updatedParsed.HnswUC); err != nil {
		return err
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
