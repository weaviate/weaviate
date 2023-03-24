//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package gemini

import (
	//GW "github.com/sirupsen/logrus"
	//GW "github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	//GW "github.com/weaviate/weaviate/adapters/repos/db/vector/gemini/distancer"
    //GW
	//GW"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Config for a new Gemini index, this contains information that is derived
// internally, e.g. by the shard. All User-settable config is specified in
// Config.UserConfig
type Config struct {
	// internal
	RootPath              string
	ID                    string
	//GW MakeCommitLoggerThunk MakeCommitLogger
	//GW VectorForIDThunk      VectorForID
	//GW Logger                logrus.FieldLogger
	//GW DistanceProvider      distancer.Provider
	PrometheusMetrics     *monitoring.PrometheusMetrics

	// metadata for monitoring
	ShardName string
	ClassName string
}

/*
func (c Config) Validate() error {
	ec := &errorcompounder.ErrorCompounder{}

	if c.ID == "" {
		ec.Addf("id cannot be empty")
	}

	if c.RootPath == "" {
		ec.Addf("rootPath cannot be empty")
	}

	if c.MakeCommitLoggerThunk == nil {
		ec.Addf("makeCommitLoggerThunk cannot be nil")
	}

	if c.VectorForIDThunk == nil {
		ec.Addf("vectorForIDThunk cannot be nil")
	}

	if c.DistanceProvider == nil {
		ec.Addf("distancerProvider cannot be nil")
	}

	return ec.ToError()
}
*/
