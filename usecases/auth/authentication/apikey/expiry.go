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

package apikey

import "time"

// ExpiryResolver turns the expiry a request asks for into the ExpiresAt to
// store. A nil requested returns the zero time, which means never.
type ExpiryResolver interface {
	Resolve(requested *time.Time) (time.Time, error)
	// ResolveImported resolves the expiry of an imported record, which may
	// already have passed.
	ResolveImported(requested *time.Time) (time.Time, error)
}

// RefusingExpiry returns an ExpiryResolver that returns err for any requested
// time.
func RefusingExpiry(err error) ExpiryResolver { return refusingExpiry{err: err} }

type refusingExpiry struct{ err error }

func (r refusingExpiry) Resolve(requested *time.Time) (time.Time, error) {
	if requested == nil {
		return time.Time{}, nil
	}
	return time.Time{}, r.err
}

func (r refusingExpiry) ResolveImported(requested *time.Time) (time.Time, error) {
	return r.Resolve(requested)
}
