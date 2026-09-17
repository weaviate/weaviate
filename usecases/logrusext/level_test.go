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

package logrusext

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

// LevelEnabled gates whether a hot-path log line is built at all, so a logger it
// cannot read must still be logged to rather than silently dropped.
func TestLevelEnabled(t *testing.T) {
	atLevel := func(level logrus.Level) *logrus.Logger {
		l, _ := test.NewNullLogger()
		l.SetLevel(level)
		return l
	}

	testCases := []struct {
		name     string
		logger   logrus.FieldLogger
		level    logrus.Level
		expected bool
	}{
		{name: "logger below trace", logger: atLevel(logrus.InfoLevel), level: logrus.TraceLevel, expected: false},
		{name: "logger at trace", logger: atLevel(logrus.TraceLevel), level: logrus.TraceLevel, expected: true},
		{name: "logger below debug", logger: atLevel(logrus.InfoLevel), level: logrus.DebugLevel, expected: false},
		{name: "logger at debug", logger: atLevel(logrus.DebugLevel), level: logrus.DebugLevel, expected: true},
		{name: "logger above debug", logger: atLevel(logrus.TraceLevel), level: logrus.DebugLevel, expected: true},
		{
			name:     "entry below trace",
			logger:   atLevel(logrus.InfoLevel).WithField("action", "lsm_compaction"),
			level:    logrus.TraceLevel,
			expected: false,
		},
		{
			name:     "entry at trace",
			logger:   atLevel(logrus.TraceLevel).WithField("action", "lsm_compaction"),
			level:    logrus.TraceLevel,
			expected: true,
		},
		{name: "unreadable logger", logger: unreadableLogger{}, level: logrus.TraceLevel, expected: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, LevelEnabled(tc.logger, tc.level))
		})
	}
}

// unreadableLogger is a FieldLogger whose level LevelEnabled cannot inspect.
type unreadableLogger struct{ logrus.FieldLogger }
