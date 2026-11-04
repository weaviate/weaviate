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

package lsmkv

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsNotFoundIsDeleted(t *testing.T) {
	errDeleted := NewErrDeleted(time.Now())

	tests := []struct {
		name         string
		err          error
		wantNotFound bool
		wantDeleted  bool
	}{
		{name: "nil", err: nil},
		{name: "unrelated", err: errors.New("not found")},
		{name: "NotFound", err: NotFound, wantNotFound: true},
		{name: "wrapped NotFound", err: fmt.Errorf("get: %w", NotFound), wantNotFound: true},
		{name: "Deleted", err: Deleted, wantDeleted: true},
		{name: "wrapped Deleted", err: fmt.Errorf("get: %w", Deleted), wantDeleted: true},
		{name: "ErrDeleted", err: errDeleted, wantDeleted: true},
		{name: "wrapped ErrDeleted", err: fmt.Errorf("get: %w", errDeleted), wantDeleted: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantNotFound, IsNotFound(tt.err), "IsNotFound")
			assert.Equal(t, tt.wantDeleted, IsDeleted(tt.err), "IsDeleted")
			assert.Equal(t, tt.wantNotFound || tt.wantDeleted, IsDeletedOrNotFound(tt.err), "IsDeletedOrNotFound")
		})
	}
}

func BenchmarkIsNotFound(b *testing.B) {
	// a merge cursor asks every segment's state, which is mostly nil or Deleted
	for _, err := range []error{nil, NotFound, Deleted} {
		b.Run(fmt.Sprintf("errors.Is/%v", err), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if errors.Is(err, NotFound) != (err == NotFound) { //nolint:errorlint // reference
					b.Fatal("miss")
				}
			}
		})
		b.Run(fmt.Sprintf("IsNotFound/%v", err), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if IsNotFound(err) != (err == NotFound) { //nolint:errorlint // reference
					b.Fatal("miss")
				}
			}
		})
	}
}
