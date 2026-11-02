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

package rest

import (
	"bytes"
	"errors"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/banner"
	"github.com/weaviate/weaviate/usecases/build"
)

type WeaviateJSONFormatter struct {
	*logrus.JSONFormatter
	gitHash, imageTag, serverVersion, goVersion string
}

func NewWeaviateJSONFormatter() logrus.Formatter {
	return &WeaviateJSONFormatter{
		&logrus.JSONFormatter{},
		build.Revision,
		build.Branch,
		build.Version,
		build.GoVersion,
	}
}

func (wf *WeaviateJSONFormatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Data["build_git_commit"] = wf.gitHash
	e.Data["build_image_tag"] = wf.imageTag
	e.Data["build_wv_version"] = wf.serverVersion
	e.Data["build_go_version"] = wf.goVersion
	return wf.JSONFormatter.Format(e)
}

type WeaviateTextFormatter struct {
	*logrus.TextFormatter
	gitHash, imageTag, serverVersion, goVersion string
}

func NewWeaviateTextFormatter() logrus.Formatter {
	return &WeaviateTextFormatter{
		&logrus.TextFormatter{},
		build.Revision,
		build.Branch,
		build.Version,
		build.GoVersion,
	}
}

func (wf *WeaviateTextFormatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Data["build_git_commit"] = wf.gitHash
	e.Data["build_image_tag"] = wf.imageTag
	e.Data["build_wv_version"] = wf.serverVersion
	e.Data["build_go_version"] = wf.goVersion
	if e.Data["action"] != banner.Action {
		return wf.TextFormatter.Format(e)
	}
	// TextFormatter quotes messages containing newlines, which flattens the
	// banner. Format the fields without the message and append it verbatim.
	msg := e.Message
	e.Message = ""
	out, err := wf.TextFormatter.Format(e)
	e.Message = msg
	if err != nil {
		return nil, err
	}
	out = bytes.TrimRight(out, "\n")
	out = append(append(out, '\n'), msg...)
	// A message without a trailing newline must not glue the next entry
	// onto its last line.
	if len(out) == 0 || out[len(out)-1] != '\n' {
		out = append(out, '\n')
	}
	return out, nil
}

var errlogLevelNotRecognized = errors.New("log level not recognized")

// logLevelFromString converts a string to a logrus log level, returns a logLevelNotRecognized
// error if the string is not recognized. level is case insensitive.
func logLevelFromString(level string) (logrus.Level, error) {
	switch strings.ToLower(level) {
	case "panic":
		return logrus.PanicLevel, nil
	case "fatal":
		return logrus.FatalLevel, nil
	case "error":
		return logrus.ErrorLevel, nil
	case "warn", "warning":
		return logrus.WarnLevel, nil
	case "info":
		return logrus.InfoLevel, nil
	case "debug":
		return logrus.DebugLevel, nil
	case "trace":
		return logrus.TraceLevel, nil
	default:
		return 0, errlogLevelNotRecognized
	}
}
