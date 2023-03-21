//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package userindex

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/userindex"
)

func TestCoordinator(t *testing.T) {
	type test struct {
		name           string
		class          string
		authErr        error
		expectErr      bool
		expectedErrMsg string
		hosts          []string
		localList      []userindex.Index
		remoteList     []userindex.Index
		localErr       error
		remoteErr      error
		expected       *models.IndexStatusList
	}

	tests := []test{
		{
			name:           "not authorized",
			authErr:        fmt.Errorf("nope! not you!"),
			expectErr:      true,
			expectedErrMsg: "nope! not you!",
		},
		{
			name:  "no other nodes, only local",
			class: "MyClass",
			localList: []userindex.Index{
				{
					ID:     "hello",
					Shards: []string{"shard1"},
				},
			},
			expected: &models.IndexStatusList{
				ClassName:  "MyClass",
				Total:      1,
				ShardCount: 1,
				Indexes: []*models.IndexStatus{
					{
						ID: "hello",
					},
				},
			},
		},
		{
			name:           "local errors",
			class:          "MyClass",
			localErr:       fmt.Errorf("ohoh"),
			expectErr:      true,
			expectedErrMsg: "ohoh",
		},
		{
			name:  "with remote",
			class: "MyClass",
			hosts: []string{"otherhost"},
			localList: []userindex.Index{
				{
					ID:     "hello",
					Shards: []string{"shard1"},
					Paths:  []string{"shard1path"},
				},
			},
			remoteList: []userindex.Index{
				{
					ID:     "hello",
					Shards: []string{"shard2"},
					Paths:  []string{"shard2path"},
				},
			},
			expected: &models.IndexStatusList{
				ClassName:  "MyClass",
				Total:      1,
				ShardCount: 2,
				Indexes: []*models.IndexStatus{
					{
						ID:    "hello",
						Paths: []string{"shard1path", "shard2path"},
					},
				},
			},
		},
		{
			name:  "remote errors",
			class: "MyClass",
			hosts: []string{"otherhost"},
			localList: []userindex.Index{
				{
					ID:     "hello",
					Shards: []string{"shard1"},
					Paths:  []string{"shard1path"},
				},
			},
			remoteErr:      fmt.Errorf("sorry!"),
			expectErr:      true,
			expectedErrMsg: "otherhost: sorry!",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			auth := fakeAuthorizer{test.authErr}
			local := fakeUIS{res: test.localList, err: test.localErr}
			remote := fakeRemoteUIS{res: test.remoteList, err: test.remoteErr}
			hosts := fakeHosts{hosts: test.hosts}
			c := New(local, auth, hosts, remote)
			res, err := c.Get(context.Background(), nil, test.class)
			if test.expectErr {
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), test.expectedErrMsg)
				return
			}

			assert.Equal(t, test.expected, res)
		})
	}
}

type fakeAuthorizer struct {
	err error
}

func (f fakeAuthorizer) Authorize(principal *models.Principal,
	verb, resource string,
) error {
	return f.err
}

type fakeUIS struct {
	res []userindex.Index
	err error
}

func (f fakeUIS) UserIndexStatus(ctx context.Context, class string) ([]userindex.Index, error) {
	return f.res, f.err
}

type fakeRemoteUIS struct {
	res []userindex.Index
	err error
}

func (f fakeRemoteUIS) UserIndexStatus(ctx context.Context,
	host, class string,
) ([]userindex.Index, error) {
	return f.res, f.err
}

type fakeHosts struct {
	hosts []string
}

func (f fakeHosts) Hostnames() []string {
	return f.hosts
}
