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

//go:build cuvs

package cuvs_index

import (
	cuvs "github.com/rapidsai/cuvs/go"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type Config struct {
	ID             string
	TargetVector   string
	Logger         logrus.FieldLogger
	DistanceMetric cuvs.Distance
	AllocChecker   memwatch.AllocChecker
}

func (c Config) Validate() error {
	ec := &errorcompounder.ErrorCompounder{}

	if c.ID == "" {
		ec.Addf("id cannot be empty")
	}

	// if c.DistanceMetric == nil {
	// 	ec.Addf("distancerProvider cannot be nil")
	// }

	return ec.ToError()
}
