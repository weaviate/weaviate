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

package rbacconf

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Validation(t *testing.T) {
	configs := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name:    "admin and viewers - correct",
			config:  Config{RootUsers: []string{"1"}, Viewers: []string{"2"}},
			wantErr: false,
		},
		{
			name:    "only admins - correct",
			config:  Config{RootUsers: []string{"1", "2"}},
			wantErr: false,
		},
		{
			name:    "only viewers - incorrect",
			config:  Config{Viewers: []string{"1", "2"}},
			wantErr: true,
		},
		{
			name:    "overlap viewers and admins - incorrect",
			config:  Config{Viewers: []string{"1", "2"}, RootUsers: []string{"1", "3"}},
			wantErr: true,
		},
	}

	for _, tt := range configs {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
