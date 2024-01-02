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

package cluster

import (
	"regexp"

	"github.com/sirupsen/logrus"
)

type logParser struct {
	logrus logrus.FieldLogger
	regexp *regexp.Regexp
}

func newLogParser(logrus logrus.FieldLogger) *logParser {
	return &logParser{
		logrus: logrus,
		regexp: regexp.MustCompile(`(.*)\[(DEBUG|ERR|ERROR|INFO|WARNING|WARN)](.*)`),
	}
}

func (l *logParser) Write(in []byte) (int, error) {
	res := l.regexp.FindSubmatch(in)
	if len(res) != 4 {
		// unable to parse log message
		l.logrus.Warnf("unable to parse memberlist log message: %s", in)
	}

	switch string(res[2]) {
	case "ERR", "ERROR":
		l.logrus.Error(string(res[3]))
	case "WARN", "WARNING":
		l.logrus.Warn(string(res[3]))
	case "DEBUG":
		l.logrus.Debug(string(res[3]))
	case "INFO":
		l.logrus.Info(string(res[3]))
	default:
		l.logrus.Warnf("unable to parse memberlist log level from message: %s", in)
	}

	return len(in), nil
}
