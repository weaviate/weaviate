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
	"runtime/debug"

	"github.com/sirupsen/logrus"

	"golang.org/x/sync/errgroup"
)

// ErrorGroupWrapper is a custom type that embeds errgroup.Group.
type ErrorGroupWrapper struct {
	*errgroup.Group
	ReturnError error
	Variables   []interface{}
	Logger      logrus.FieldLogger
}

// NewErrorGroupWrapper creates a new ErrorGroupWrapper.
func NewErrorGroupWrapper(logger logrus.FieldLogger, vars ...interface{}) *ErrorGroupWrapper {
	return &ErrorGroupWrapper{
		Group:       new(errgroup.Group),
		ReturnError: nil,
		Variables:   vars,
		Logger:      logger,
	}
}

// Go overrides the Go method to add panic recovery logic.
func (egw *ErrorGroupWrapper) Go(f func() error, localVars ...interface{}) {
	egw.Group.Go(func() error {
		defer func() {
			if r := recover(); r != nil {
				egw.Logger.WithField("panic", r).Errorf("Recovered from panic: %v, local variables %v, additional localVars %v\n", r, localVars, egw.Variables)
				debug.PrintStack()
				egw.ReturnError = fmt.Errorf("panic occurred: %v", r)
			}
		}()
		return f()
	})
}

// Wait waits for all goroutines to finish and returns the first non-nil error.
func (egw *ErrorGroupWrapper) Wait() error {
	if err := egw.Group.Wait(); err != nil {
		return err
	}
	return egw.ReturnError
}
