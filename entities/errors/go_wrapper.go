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
	"fmt"
	"os"
	"runtime/debug"

	entcfg "github.com/weaviate/weaviate/entities/config"
	entsentry "github.com/weaviate/weaviate/entities/sentry"

	"github.com/sirupsen/logrus"
)

func GoWrapper(f func(), logger logrus.FieldLogger) {
	go func() {
		defer func() {
			if !entcfg.Enabled(os.Getenv("DISABLE_RECOVERY_ON_PANIC")) {
				if r := recover(); r != nil {
					logger.Errorf("Recovered from panic: %v", r)
					entsentry.Recover(r)
					debug.PrintStack()
				}
			}
		}()
		f()
	}()
}

func GoWrapperWithErrorCh(f func(), logger logrus.FieldLogger) chan error {
	errChan := make(chan error, 1)
	go func() {
		defer func() {
			if !entcfg.Enabled(os.Getenv("DISABLE_RECOVERY_ON_PANIC")) {
				if r := recover(); r != nil {
					logger.Errorf("Recovered from panic: %v", r)
					entsentry.Recover(r)
					debug.PrintStack()
					errChan <- fmt.Errorf("panic occurred: %v", r)
				}
			}
		}()
		f()
		errChan <- nil
	}()
	return errChan
}

func GoWrapperWithBlock(f func(), logger logrus.FieldLogger) error {
	return <-GoWrapperWithErrorCh(f, logger)
}
