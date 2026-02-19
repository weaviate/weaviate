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
	"context"
	"fmt"
)

func NewCanceledCause(format string, a ...any) error {
	err := fmt.Errorf(format, a...)
	return &CanceledCause{
		err: fmt.Sprintf("%s: %s", context.Canceled, err),
	}
}

type CanceledCause struct {
	err string
}

func (c *CanceledCause) Error() string {
	return c.err
}

func (c *CanceledCause) Unwrap() error {
	return context.Canceled
}
