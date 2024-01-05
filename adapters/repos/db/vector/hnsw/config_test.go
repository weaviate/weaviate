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

package hnsw

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func Test_ValidConfig(t *testing.T) {
	err := validConfig().Validate()
	assert.Nil(t, err)
}

func Test_InValidConfig(t *testing.T) {
	type test struct {
		config      func() Config
		expectedErr error
	}

	tests := []test{
		{
			config: func() Config {
				v := validConfig()
				v.ID = ""
				return v
			},
			expectedErr: errors.Errorf("id cannot be empty"),
		},
		{
			config: func() Config {
				v := validConfig()
				v.RootPath = ""
				return v
			},
			expectedErr: errors.Errorf("rootPath cannot be empty"),
		},
		{
			config: func() Config {
				v := validConfig()
				v.MakeCommitLoggerThunk = nil
				return v
			},
			expectedErr: errors.Errorf("makeCommitLoggerThunk cannot be nil"),
		},
		{
			config: func() Config {
				v := validConfig()
				v.VectorForIDThunk = nil
				return v
			},
			expectedErr: errors.Errorf("vectorForIDThunk cannot be nil"),
		},
	}

	for _, test := range tests {
		t.Run(test.expectedErr.Error(), func(t *testing.T) {
			err := test.config().Validate()
			assert.Equal(t, test.expectedErr.Error(), err.Error())
		})
	}
}

func validConfig() Config {
	return Config{
		RootPath:              "some path",
		ID:                    "someid",
		MakeCommitLoggerThunk: func() (CommitLogger, error) { return nil, nil },
		VectorForIDThunk:      func(context.Context, uint64) ([]float32, error) { return nil, nil },
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
	}
}
