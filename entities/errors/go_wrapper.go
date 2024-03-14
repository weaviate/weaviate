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

package errors

import (
	"os"
	"runtime/debug"

	"github.com/weaviate/weaviate/usecases/configbase"

	"github.com/sirupsen/logrus"
)

func GoWrapper(f func(), logger logrus.FieldLogger) {
	go func() {
		defer func() {
			if !configbase.Enabled(os.Getenv("DISABLE_RECOVERY_ON_PANIC")) {
				if r := recover(); r != nil {
					logger.Errorf("Recovered from panic: %v", r)
					debug.PrintStack()
				}
			}
		}()
		f()
	}()
}
