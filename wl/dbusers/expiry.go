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

package dbusers

import (
	"errors"
	"fmt"
	"time"
)

// ValidatingExpiry is the apikey.ExpiryResolver of a node with
// AUTHENTICATION_DB_USERS_ENABLED and a well-formed Weaviate license key.
type ValidatingExpiry struct{}

func NewValidatingExpiry() *ValidatingExpiry { return &ValidatingExpiry{} }

// Resolve returns requested in UTC, truncated to the milliseconds REST renders.
// It refuses a time that is not after now, and a UTC year past 9999, which
// JSON cannot encode.
func (*ValidatingExpiry) Resolve(requested *time.Time) (time.Time, error) {
	if requested == nil {
		return time.Time{}, nil
	}
	t := requested.UTC().Truncate(time.Millisecond)
	if !t.After(time.Now()) {
		return time.Time{}, fmt.Errorf("expiresAt %s is not in the future", t.Format(time.RFC3339Nano))
	}
	if t.Year() > 9999 {
		return time.Time{}, errors.New("expiresAt must be before the year 10000 in UTC")
	}
	return t, nil
}
