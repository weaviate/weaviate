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

package config

import (
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/usecases/auth/authorization/adminlist"
)

func Test_Validation(t *testing.T) {
	configs := []struct {
		name    string
		config  Authorization
		wantErr bool
	}{
		{
			name:    "Only adminlist",
			config:  Authorization{AdminList: adminlist.Config{Enabled: true}},
			wantErr: false,
		},
		{
			name:    "Only rbac",
			config:  Authorization{Rbac: rbacconf.Config{Enabled: true, RootUsers: []string{"1"}}},
			wantErr: false,
		},
		{
			name:    "Only adminlist - wrong config",
			config:  Authorization{AdminList: adminlist.Config{Enabled: true, Users: []string{"1"}, ReadOnlyUsers: []string{"1"}}},
			wantErr: true,
		},
		{
			name: "both adminlist and rbac",
			config: Authorization{
				AdminList: adminlist.Config{Enabled: true},
				Rbac:      rbacconf.Config{Enabled: true, RootUsers: []string{"1"}},
			},
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
