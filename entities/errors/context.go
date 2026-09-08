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

// NewCanceledCause pairs context.Canceled with reason, so a caller can tell one
// cancellation from another. A nil reason gives back plain context.Canceled:
// CanceledCause.Error would render it as "%!s(<nil>)".
func NewCanceledCause(reason error) error {
	if reason == nil {
		return context.Canceled
	}
	return &CanceledCause{reason: reason}
}

// CanceledCause carries why a context was cancelled, so callers can distinguish
// causes without matching on the message.
type CanceledCause struct {
	reason error
}

func (c *CanceledCause) Error() string {
	return fmt.Sprintf("%s: %s", context.Canceled, c.reason)
}

// Unwrap reports both, so errors.Is finds context.Canceled and the reason.
func (c *CanceledCause) Unwrap() []error {
	return []error{context.Canceled, c.reason}
}
