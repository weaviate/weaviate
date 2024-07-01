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

package helpers

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	enabledEnvVar           = "QUERY_SLOW_LOG_ENABLED"
	thresholdEnvVar         = "QUERY_SLOW_LOG_THRESHOLD"
	defaultSlowLogThreshold = 5 * time.Second
)

type SlowQueryReporter interface {
	LogIfSlow(time.Time, map[string]any)
}

type BaseSlowReporter struct {
	threshold time.Duration
	logger    logrus.FieldLogger
}

func NewSlowQueryReporterFromEnv(logger logrus.FieldLogger) SlowQueryReporter {
	if logger == nil {
		fmt.Println("Unexpected nil logger for SlowQueryReporter. Reporter disabled.")
		return &NoopSlowReporter{}
	}

	enabled := false
	if enabledStr, ok := os.LookupEnv(enabledEnvVar); ok {
		// TODO: Log warning if bool can't be parsed
		enabled, _ = strconv.ParseBool(enabledStr)
		fmt.Println("en", enabledStr, enabled)
	}
	if !enabled {
		return &NoopSlowReporter{}
	}

	threshold := defaultSlowLogThreshold
	if thresholdStr, ok := os.LookupEnv(thresholdEnvVar); ok {
		thresholdP, err := time.ParseDuration(thresholdStr)
		if err != nil {
			logger.WithField("action", "startup").Warningf("Unexpected value \"%s\" for %s. Please set a duration (i.e. 10s). Continuing with default value (%s).", thresholdStr, thresholdEnvVar, threshold)
		} else {
			threshold = thresholdP
		}
	}
	return NewSlowQueryReporter(threshold, logger)
}

func NewSlowQueryReporter(threshold time.Duration, logger logrus.FieldLogger) *BaseSlowReporter {
	logger.WithField("action", "startup").Printf("Starting SlowQueryReporter with %s threshold", threshold)
	return &BaseSlowReporter{
		threshold: threshold,
		logger:    logger,
	}
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
func (sq *BaseSlowReporter) LogIfSlow(startTime time.Time, fields map[string]any) {
	took := time.Since(startTime)
	if took > sq.threshold {
		if fields == nil {
			fields = map[string]any{}
		}
		fields["took"] = took
		sq.logger.WithFields(fields).Warn(fmt.Sprintf("Slow query detected (%s)", took.Round(time.Millisecond)))
	}
}

// NoopSlowReporter is used when the reporter is disabled.
type NoopSlowReporter struct{}

func (sq *NoopSlowReporter) LogIfSlow(startTime time.Time, fields map[string]any) {
}
