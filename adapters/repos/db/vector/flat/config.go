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

package flat

import (
	"github.com/sirupsen/logrus"
	"github.com/liutizhong/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/liutizhong/weaviate/entities/errorcompounder"
	"github.com/liutizhong/weaviate/usecases/memwatch"
)

type Config struct {
	ID               string
	RootPath         string
	TargetVector     string
	Logger           logrus.FieldLogger
	DistanceProvider distancer.Provider
	AllocChecker     memwatch.AllocChecker
}

func (c Config) Validate() error {
	ec := errorcompounder.New()

	if c.ID == "" {
		ec.Addf("id cannot be empty")
	}

	if c.RootPath == "" {
		ec.Addf("rootPath cannot be empty")
	}

	if c.DistanceProvider == nil {
		ec.Addf("distancerProvider cannot be nil")
	}

	return ec.ToError()
}
