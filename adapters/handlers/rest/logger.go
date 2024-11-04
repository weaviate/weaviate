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
	return wf.TextFormatter.Format(e)
}
