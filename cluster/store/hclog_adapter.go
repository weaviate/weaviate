/*
	Copyright NetFoundry Inc.

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	https://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

package store

import (
	"fmt"
	"io"
	"log"
	"runtime"
	"strings"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/sirupsen/logrus"
)

func NewHcLogrusLogger(logger *logrus.Logger) hclog.Logger {
	return &hclogAdapter{
		entry: logrus.NewEntry(logger),
	}
}

type hclogAdapter struct {
	entry *logrus.Entry
	sync.Mutex
	name string
}

func (hclogger *hclogAdapter) GetLevel() hclog.Level {
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
		return hclog.Error
	}
	return hclog.DefaultLevel
}

func (hclogger *hclogAdapter) Log(level hclog.Level, msg string, args ...interface{}) {
	switch level {
	case hclog.Trace:
		hclogger.Trace(msg, args...)
	case hclog.Debug:
		hclogger.Debug(msg, args...)
	case hclog.Info:
		hclogger.Info(msg, args...)
	case hclog.Warn:
		hclogger.Warn(msg, args...)
	case hclog.Error:
		hclogger.Error(msg, args...)
	case hclog.Off:
	}
}

func (hclogger *hclogAdapter) ImpliedArgs() []interface{} {
	var fields []interface{}
	for k, v := range hclogger.entry.Data {
		fields = append(fields, k)
		fields = append(fields, v)
	}
	return fields
}

func (hclogger *hclogAdapter) Name() string {
	return hclogger.name
}

func (hclogger *hclogAdapter) Trace(msg string, args ...interface{}) {
	hclogger.logToLogrus(logrus.TraceLevel, msg, args...)
}

func (hclogger *hclogAdapter) Debug(msg string, args ...interface{}) {
	hclogger.logToLogrus(logrus.DebugLevel, msg, args...)
}

func (hclogger *hclogAdapter) Info(msg string, args ...interface{}) {
	hclogger.logToLogrus(logrus.InfoLevel, msg, args...)
}

func (hclogger *hclogAdapter) Warn(msg string, args ...interface{}) {
	hclogger.logToLogrus(logrus.WarnLevel, msg, args...)
}

func (hclogger *hclogAdapter) Error(msg string, args ...interface{}) {
	hclogger.logToLogrus(logrus.ErrorLevel, msg, args...)
}

func (hclogger *hclogAdapter) logToLogrus(level logrus.Level, msg string, args ...interface{}) {
	logger := hclogger.entry
	if len(args) > 0 {
		logger = hclogger.LoggerWith(args)
	}
	frame := hclogger.getCaller()
	logger = logger.WithField("file", frame.File).WithField("func", frame.Function)
	logger.Log(level, hclogger.name+msg)
}

func (hclogger *hclogAdapter) IsTrace() bool {
	return hclogger.entry.Logger.IsLevelEnabled(logrus.TraceLevel)
}

func (hclogger *hclogAdapter) IsDebug() bool {
	return hclogger.entry.Logger.IsLevelEnabled(logrus.DebugLevel)
}

func (hclogger *hclogAdapter) IsInfo() bool {
	return hclogger.entry.Logger.IsLevelEnabled(logrus.InfoLevel)
}

func (hclogger *hclogAdapter) IsWarn() bool {
	return hclogger.entry.Logger.IsLevelEnabled(logrus.WarnLevel)
}

func (hclogger *hclogAdapter) IsError() bool {
	return hclogger.entry.Logger.IsLevelEnabled(logrus.ErrorLevel)
}

func (hclogger *hclogAdapter) With(args ...interface{}) hclog.Logger {
	return &hclogAdapter{
		entry: hclogger.LoggerWith(args),
	}
}

func (hclogger *hclogAdapter) LoggerWith(args []interface{}) *logrus.Entry {
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

func (hclogger *hclogAdapter) Named(name string) hclog.Logger {
	return hclogger.ResetNamed(name + hclogger.name)
}

func (hclogger *hclogAdapter) ResetNamed(name string) hclog.Logger {
	return &hclogAdapter{
		name:  name,
		entry: hclogger.entry,
	}
}

func (hclogger *hclogAdapter) SetLevel(hclog.Level) {
	panic("implement me")
}

func (hclogger *hclogAdapter) StandardLogger(*hclog.StandardLoggerOptions) *log.Logger {
	panic("implement me")
}

func (hclogger *hclogAdapter) StandardWriter(*hclog.StandardLoggerOptions) io.Writer {
	panic("implement me")
}

var (
	// qualified package name, cached at first use
	localPackage string

	// Positions in the call stack when tracing to report the calling method
	minimumCallerDepth = 1

	// Used for caller information initialisation
	callerInitOnce sync.Once
)

const (
	maximumCallerDepth      int = 25
	knownLocalPackageFrames int = 4
)

// getCaller retrieves the name of the first non-logrus calling function
// derived from logrus code
func (hclogger *hclogAdapter) getCaller() *runtime.Frame {
	// cache this package's fully-qualified name
	callerInitOnce.Do(func() {
		pcs := make([]uintptr, maximumCallerDepth)
		_ = runtime.Callers(0, pcs)

		// dynamic get the package name and the minimum caller depth
		for i := 0; i < maximumCallerDepth; i++ {
			funcName := runtime.FuncForPC(pcs[i]).Name()
			if strings.Contains(funcName, "getCaller") {
				localPackage = hclogger.getPackageName(funcName)
				// fmt.Printf("local package: %v\n", localPackage)
				break
			}
		}

		minimumCallerDepth = knownLocalPackageFrames
	})

	// Restrict the lookback frames to avoid runaway lookups
	pcs := make([]uintptr, maximumCallerDepth)
	depth := runtime.Callers(minimumCallerDepth, pcs)
	frames := runtime.CallersFrames(pcs[:depth])

	for f, again := frames.Next(); again; f, again = frames.Next() {
		pkg := hclogger.getPackageName(f.Function)

		// If the caller isn't part of this package, we're done
		if pkg != localPackage {
			//fmt.Printf("frame func: %v\n", f.Function)
			return &f //nolint:scopelint
		}
	}

	// fmt.Printf("frame func not found\n")

	// if we got here, we failed to find the caller's context
	return nil
}

// derived from logrus code
// getPackageName reduces a fully qualified function name to the package name
// There really ought to be a better way...
func (hclogger *hclogAdapter) getPackageName(f string) string {
	for {
		lastPeriod := strings.LastIndex(f, ".")
		lastSlash := strings.LastIndex(f, "/")
		if lastPeriod > lastSlash {
			f = f[:lastPeriod]
		} else {
			break
		}
	}

	return f
}
