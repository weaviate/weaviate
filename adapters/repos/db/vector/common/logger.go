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

package common

import (
	"io"

	"github.com/sirupsen/logrus"
)

// LoggerOrDiscard substitutes a logger that throws its output away when the
// caller supplied none. The indexes hand their logger on to commit loggers,
// caches and quantizers that dereference it without a nil check of their own.
func LoggerOrDiscard(logger logrus.FieldLogger) logrus.FieldLogger {
	if logger != nil {
		return logger
	}

	discard := logrus.New()
	discard.Out = io.Discard
	return discard
}
