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

package log

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/sirupsen/logrus"
)

func NewHCLogrusLogger(name string, logger *logrus.Logger) hclog.Logger {
	return &hclogLogrus{
		entry: logrus.NewEntry(logger),
		name:  fmt.Sprintf("%s ", name),
	}
}

// hclogLogrus is an adapter logger between `logrus.Logger` and `hclog.Logger`.
type hclogLogrus struct {
	// entry is global set of fields shared by single logger
	entry *logrus.Entry
	name  string
}

func (hclogger *hclogLogrus) GetLevel() hclog.Level {
	switch hclogger.entry.Logger.Level {
	case logrus.TraceLevel:
		return hclog.Trace
	case logrus.DebugLevel:
		return hclog.Debug
	case logrus.InfoLevel:
		return hclog.Info
	case logrus.WarnLevel:
		return hclog.Warn
	case logrus.ErrorLevel:
		return hclog.Error
	case logrus.FatalLevel:
	case logrus.PanicLevel:
		return hclog.Error
	}
	return hclog.DefaultLevel
}

func (hclogger *hclogLogrus) Log(level hclog.Level, msg string, args ...interface{}) {
	switch level {
	case hclog.Trace:
		hclogger.Trace(msg, args...)
	case hclog.Debug:
		hclogger.Debug(msg, args...)
	case hclog.Info:
	case hclog.NoLevel:
		hclogger.Info(msg, args...)
	case hclog.Warn:
		hclogger.Warn(msg, args...)
	case hclog.Error:
		hclogger.Error(msg, args...)
	case hclog.Off:
	}
}

func (hclogger *hclogLogrus) ImpliedArgs() []interface{} {
	var fields []interface{}
	for k, v := range hclogger.entry.Data {
		fields = append(fields, k)
		fields = append(fields, v)
	}
	return fields
}

func (hclogger *hclogLogrus) Name() string {
	return hclogger.name
}

func (hclogger *hclogLogrus) Trace(msg string, args ...interface{}) {
	hclogger.logToLogrus(logrus.TraceLevel, msg, args...)
}

func (hclogger *hclogLogrus) Debug(msg string, args ...interface{}) {
	hclogger.logToLogrus(logrus.DebugLevel, msg, args...)
}

func (hclogger *hclogLogrus) Info(msg string, args ...interface{}) {
	hclogger.logToLogrus(logrus.InfoLevel, msg, args...)
}

func (hclogger *hclogLogrus) Warn(msg string, args ...interface{}) {
	hclogger.logToLogrus(logrus.WarnLevel, msg, args...)
}

func (hclogger *hclogLogrus) Error(msg string, args ...interface{}) {
	hclogger.logToLogrus(logrus.ErrorLevel, msg, args...)
}

func (hclogger *hclogLogrus) logToLogrus(level logrus.Level, msg string, args ...interface{}) {
	// we create new log entry merging per-logger `fields` (hclogger.entry)
	entry := hclogger.entry.WithFields(hclogger.loggerWith(args).WithField("action", strings.TrimSpace(hclogger.name)).Data)
	entry.Log(level, msg)
}

func (hclogger *hclogLogrus) IsTrace() bool {
	return hclogger.entry.Logger.IsLevelEnabled(logrus.TraceLevel)
}

func (hclogger *hclogLogrus) IsDebug() bool {
	return hclogger.entry.Logger.IsLevelEnabled(logrus.DebugLevel)
}

func (hclogger *hclogLogrus) IsInfo() bool {
	return hclogger.entry.Logger.IsLevelEnabled(logrus.InfoLevel)
}

func (hclogger *hclogLogrus) IsWarn() bool {
	return hclogger.entry.Logger.IsLevelEnabled(logrus.WarnLevel)
}

func (hclogger *hclogLogrus) IsError() bool {
	return hclogger.entry.Logger.IsLevelEnabled(logrus.ErrorLevel)
}

func (hclogger *hclogLogrus) With(args ...interface{}) hclog.Logger {
	return &hclogLogrus{
		name:  hclogger.name,
		entry: hclogger.loggerWith(args).WithField("action", strings.TrimSpace(hclogger.name)),
	}
}

func (hclogger *hclogLogrus) loggerWith(args []interface{}) *logrus.Entry {
	l := hclogger.entry
	ml := len(args)
	var key string
	for i := 0; i < ml-1; i += 2 {
		keyVal := args[i]
		if keyStr, ok := keyVal.(string); ok {
			key = keyStr
		} else {
			key = fmt.Sprintf("%v", keyVal)
		}
		val := args[i+1]
		if f, ok := val.(hclog.Format); ok {
			val = fmt.Sprintf(f[0].(string), f[1:])
		}
		l = l.WithField(key, val)
	}
	return l
}

func (hclogger *hclogLogrus) Named(name string) hclog.Logger {
	return hclogger.ResetNamed(name + hclogger.name)
}

func (hclogger *hclogLogrus) ResetNamed(name string) hclog.Logger {
	return &hclogLogrus{
		name:  name,
		entry: hclogger.entry,
	}
}

func (hclogger *hclogLogrus) SetLevel(l hclog.Level) {
	hclogger.entry.Level = logrus.Level(l)
}

func (hclogger *hclogLogrus) StandardLogger(*hclog.StandardLoggerOptions) *log.Logger {
	return log.Default()
}

func (hclogger *hclogLogrus) StandardWriter(*hclog.StandardLoggerOptions) io.Writer {
	return os.Stdout
}
