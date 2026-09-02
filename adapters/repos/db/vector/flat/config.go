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

package flat

import (
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type Config struct {
	// ID is the index's physical identity: it names everything the index
	// stores on disk. "main" for the legacy unnamed vector, canonically
	// "vectors_<name>" for a named vector.
	ID       string
	RootPath string
	// TargetVector is the index's logical name — the named vector it serves.
	// It routes object-vector lookup and diagnostics and never names storage.
	TargetVector      string
	Logger            logrus.FieldLogger
	DistanceProvider  distancer.Provider
	AllocChecker      memwatch.AllocChecker
	MakeBucketOptions lsmkv.MakeBucketOptions
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
