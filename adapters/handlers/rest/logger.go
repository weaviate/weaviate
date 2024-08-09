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
	goruntime "runtime"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/config"
)

type WeaviateJSONFormatter struct {
	*logrus.JSONFormatter
}

func (wf *WeaviateJSONFormatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Data["git_commit"] = config.GitHash
	e.Data["wv_version"] = config.ImageTag
	e.Data["go_version"] = goruntime.Version()
	return wf.JSONFormatter.Format(e)
}

type WeaviateTextFormatter struct {
	*logrus.TextFormatter
}

func (wf *WeaviateTextFormatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Data["git_commit"] = config.GitHash
	e.Data["wv_version"] = config.ImageTag
	e.Data["go_version"] = goruntime.Version()
	return wf.TextFormatter.Format(e)
}
