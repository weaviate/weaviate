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

package rest

import (
	"github.com/sirupsen/logrus"
)

type WeaviateJSONFormatter struct {
	gitHash, imageTag, serverVersion, goVersion string
	*logrus.JSONFormatter
}

func (wf *WeaviateJSONFormatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Data["build_git_commit"] = wf.gitHash
	e.Data["build_image_tag"] = wf.imageTag
	e.Data["build_wv_version"] = wf.serverVersion
	e.Data["build_go_version"] = wf.goVersion
	return wf.JSONFormatter.Format(e)
}

type WeaviateTextFormatter struct {
	gitHash, imageTag, serverVersion, goVersion string
	*logrus.TextFormatter
}

func (wf *WeaviateTextFormatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Data["build_git_commit"] = wf.gitHash
	e.Data["build_image_tag"] = wf.imageTag
	e.Data["build_wv_version"] = wf.serverVersion
	e.Data["build_go_version"] = wf.goVersion
	return wf.TextFormatter.Format(e)
}

// logLevelFromString converts a string to a logrus log level, defaulting to info if the string is
// not recognized.
func logLevelFromString(level string) logrus.Level {
	switch level {
	case "panic":
		return logrus.PanicLevel
	case "fatal":
		return logrus.FatalLevel
	case "error":
		return logrus.ErrorLevel
	case "warn", "warning":
		return logrus.WarnLevel
	case "info":
		return logrus.InfoLevel
	case "debug":
		return logrus.DebugLevel
	case "trace":
		return logrus.TraceLevel
	default:
		return logrus.InfoLevel
	}
}
