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

package helpers

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

const (
	DefaultSlowLogThreshold = 5 * time.Second
)

type SlowQueryReporter interface {
	LogIfSlow(context.Context, time.Time, map[string]any)
	// Enabled reports whether LogIfSlow can log anything at all, so a caller
	// can skip preparing details nothing would read.
	Enabled() bool
}

type BaseSlowReporter struct {
	threshold *runtime.DynamicValue[time.Duration]
	enabled   *runtime.DynamicValue[bool]
	logger    logrus.FieldLogger
}

func NewSlowQueryReporter(
	enabled *runtime.DynamicValue[bool],
	threshold *runtime.DynamicValue[time.Duration],
	logger logrus.FieldLogger,
) *BaseSlowReporter {
	logger.WithField("action", "slow_log_startup").Debugf("Starting SlowQueryReporter with %s threshold", threshold.Get())
	return &BaseSlowReporter{
		threshold: threshold,
		enabled:   enabled,
		logger:    logger,
	}
}

func (sq *BaseSlowReporter) Enabled() bool {
	return sq.enabled.Get()
}

// LogIfSlow prints a warning log if the request takes longer than the threshold.
// Usage:
//
//		startTime := time.Now()
//		defer s.slowQueryReporter.LogIfSlow(startTime, map[string]any{
//			"key": "value"
//	  })
//
// TODO (sebneira): Consider providing fields out of the box (e.g. shard info). Right now we're
// limited because of circular dependencies.
func (sq *BaseSlowReporter) LogIfSlow(ctx context.Context, startTime time.Time, fields map[string]any) {
	if !sq.Enabled() {
		return
	}

	threshold := sq.threshold.Get()
	if threshold <= 0 {
		threshold = DefaultSlowLogThreshold
	}

	took := time.Since(startTime)

	slowQuery := took > threshold

	sampledQuery := rand.Float64() < 0.01 // 1% sampling

	if slowQuery || sampledQuery {
		if fields == nil {
			fields = map[string]any{}
		}

		detailFields := ExtractSlowQueryDetails(ctx)
		if detailFields != nil {
			maps.Copy(fields, detailFields)
		}
		fields["took"] = took
		if slowQuery {
			sq.logger.WithFields(fields).Warn(fmt.Sprintf("Slow query detected (%s)", took.Round(time.Millisecond)))
		} else {
			sq.logger.WithFields(fields).Info(fmt.Sprintf("Sampled query (%s)", took.Round(time.Millisecond)))
		}
		return
	}
}
