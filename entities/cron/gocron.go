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

package cron

import (
	"fmt"
	"time"

	gocron "github.com/netresearch/go-cron"
	"github.com/sirupsen/logrus"
)

// RunOnEveryNode is the tick gate for a cron job every node runs, rather than
// only the leader.
func RunOnEveryNode() bool { return true }

// EverySpec renders d as an `@every` cron descriptor.
func EverySpec(d time.Duration) string { return fmt.Sprintf("@every %s", d) }

// Parser is the schedule parser both the validator and the scheduler run, so a
// spec one accepts is one the other can schedule.
func Parser() gocron.Parser { return gocron.FullParser() }

func NewGoCronLogger(logger logrus.FieldLogger, infoLevel logrus.Level) *GoCronLogger {
	return &GoCronLogger{logger: logger, infoLevel: infoLevel}
}

type GoCronLogger struct {
	logger    logrus.FieldLogger
	infoLevel logrus.Level
}

func (l *GoCronLogger) Info(msg string, keysAndValues ...any) {
	l.logger.WithFields(l.toFields(keysAndValues)).
		Log(l.infoLevel, msg)
}

func (l *GoCronLogger) Error(err error, msg string, keysAndValues ...any) {
	logger := l.logger.WithFields(l.toFields(keysAndValues))
	if err == nil {
		logger.Error(msg)
		return
	}
	logger.Errorf("%s: %v", msg, err)
}

func (l *GoCronLogger) toFields(keysAndValues []any) logrus.Fields {
	fields := logrus.Fields{}
	if ln := len(keysAndValues); ln > 0 {
		for i := 0; i < ln; i += 2 {
			fields[fmt.Sprintf("c_%s", keysAndValues[i])] = keysAndValues[i+1]
		}
	}
	return fields
}
