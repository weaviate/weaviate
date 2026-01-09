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

	"github.com/netresearch/go-cron"
	"github.com/sirupsen/logrus"
)

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
	l.logger.WithFields(l.toFields(keysAndValues)).
		WithError(err).
		Error(msg)
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

// ----------------------------------------------------------------------------

// equivalent of cron.WithSeconds() option
func SecondsParser() cron.Parser {
	return cron.MustNewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
}

func StandardParser() cron.Parser {
	return cron.StandardParser()
}
