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

import "github.com/sirupsen/logrus"

// LevelEnabled reports whether logger emits entries at level, so hot paths can
// skip building fields the logger would discard. A logger of unknown type counts
// as enabled rather than silently losing the line.
func LevelEnabled(logger logrus.FieldLogger, level logrus.Level) bool {
	switch l := logger.(type) {
	case *logrus.Logger:
		return l.IsLevelEnabled(level)
	case *logrus.Entry:
		return l.Logger.IsLevelEnabled(level)
	default:
		return true
	}
}
