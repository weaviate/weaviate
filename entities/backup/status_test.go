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

package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatusIsCancellation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		status Status
		want   bool
	}{
		{status: Started, want: false},
		{status: Transferring, want: false},
		{status: Transferred, want: false},
		{status: Finalizing, want: false},
		{status: Success, want: false},
		{status: Cancelling, want: true},
		{status: Cancelled, want: true},
		{status: Failed, want: false},
		{status: "", want: false},
	}

	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			assert.Equal(t, tc.want, tc.status.IsCancellation())
		})
	}
}
