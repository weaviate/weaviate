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

package cuvs_index

import (
	cuvs "github.com/rapidsai/cuvs/go"
	"github.com/rapidsai/cuvs/go/cagra"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

type Config struct {
	ID               string
	TargetVector     string
	Logger           logrus.FieldLogger
	RootPath         string
	DistanceMetric   cuvs.Distance
	CuvsIndexParams  *cagra.IndexParams
	CuvsSearchParams *cagra.SearchParams
	CuvsPoolMemory   int
}

func (c Config) Validate() error {
	ec := &errorcompounder.ErrorCompounder{}

	if c.ID == "" {
		ec.Addf("id cannot be empty")
	}

	if c.RootPath == "" {
		ec.Addf("rootPath cannot be empty")
	}

	// if c.DistanceMetric == nil {
	// 	ec.Addf("distancerProvider cannot be nil")
	// }

	return ec.ToError()
}

func ValidateUserConfigUpdate(initial, updated schemaConfig.VectorIndexConfig) error {
	// initialParsed, ok := initial.(flatent.UserConfig)
	// if !ok {
	// 	return errors.Errorf("initial is not UserConfig, but %T", initial)
	// }

	// updatedParsed, ok := updated.(flatent.UserConfig)
	// if !ok {
	// 	return errors.Errorf("updated is not UserConfig, but %T", updated)
	// }

	return nil
}
