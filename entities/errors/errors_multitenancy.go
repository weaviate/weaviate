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
	"github.com/pkg/errors"
)

var (
	ErrTenantNotActive = errors.New("tenant not active")
	ErrTenantNotFound  = errors.New("tenant not found")
)

func IsTenantNotFound(err error) bool {
	return errors.Is(errors.Unwrap(err), ErrTenantNotFound)
}
