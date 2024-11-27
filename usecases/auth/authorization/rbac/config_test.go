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

package rbac

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
			name:    "admin and users correct",
			config:  Config{AllUsers: []string{"1", "2"}, Admins: []string{"1", "2"}},
			wantErr: false,
		},
		{
			name:    "admin and users and viewers correct",
			config:  Config{AllUsers: []string{"1", "2"}, Admins: []string{"1"}, Viewers: []string{"2"}},
			wantErr: false,
		},
		{
			name:    "only users",
			config:  Config{AllUsers: []string{"1", "2"}},
			wantErr: true,
		},
		{
			name:    "only admins",
			config:  Config{Admins: []string{"1", "2"}},
			wantErr: true,
		},
		{
			name:    "only viewers",
			config:  Config{Viewers: []string{"1", "2"}},
			wantErr: true,
		},
		{
			name:    "overlap viewers and admins",
			config:  Config{Viewers: []string{"1", "2"}, Admins: []string{"1", "3"}},
			wantErr: true,
		},
		{
			name:    "no admin",
			config:  Config{AllUsers: []string{"1", "2"}},
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
