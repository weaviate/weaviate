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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidatingExpiry(t *testing.T) {
	future := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	lastUTCSecond := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

	cases := []struct {
		name      string
		imported  bool
		requested *time.Time
		want      time.Time
		wantErr   bool
	}{
		{name: "sub-millisecond digits are dropped", requested: new(future.Add(1234567 * time.Nanosecond)), want: future.Add(time.Millisecond).UTC()},
		{name: "the last second of 9999 in UTC is accepted", requested: &lastUTCSecond, want: lastUTCSecond},
		{name: "import: a time that truncates to the Unix epoch, which an empty string decodes to, is refused", imported: true, requested: new(time.Unix(0, 500*1000).UTC()), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolve := NewValidatingExpiry().Resolve
			if tc.imported {
				resolve = NewValidatingExpiry().ResolveImported
			}
			got, err := resolve(tc.requested)

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
