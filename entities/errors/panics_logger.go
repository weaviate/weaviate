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

package errors

import (
	"runtime/debug"
	"strings"

	"github.com/sirupsen/logrus"
)

func PrintStack(logger logrus.FieldLogger) {
	for _, line := range strings.Split(strings.TrimSuffix(string(debug.Stack()), "\n"), "\n") {
		logger.WithField("action", "print_stack").Errorf("  %s", line)
	}
}
