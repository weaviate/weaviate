package hnsw

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
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
		test{
			config: func() Config {
				v := validConfig()
				v.ID = ""
				return v
			},
			expectedErr: fmt.Errorf("id cannot be empty"),
		},
		test{
			config: func() Config {
				v := validConfig()
				v.RootPath = ""
				return v
			},
			expectedErr: fmt.Errorf("rootPath cannot be empty"),
		},
		test{
			config: func() Config {
				v := validConfig()
				v.MaximumConnections = 0
				return v
			},
			expectedErr: fmt.Errorf("maximumConnections must be greater than 0"),
		},
		test{
			config: func() Config {
				v := validConfig()
				v.EFConstruction = 0
				return v
			},
			expectedErr: fmt.Errorf("efConstruction must be greater than 0"),
		},
		test{
			config: func() Config {
				v := validConfig()
				v.MakeCommitLoggerThunk = nil
				return v
			},
			expectedErr: fmt.Errorf("makeCommitLoggerThunk cannot be nil"),
		},
		test{
			config: func() Config {
				v := validConfig()
				v.VectorForIDThunk = nil
				return v
			},
			expectedErr: fmt.Errorf("vectorForIDThunk cannot be nil"),
		},
	}

	for _, test := range tests {
		t.Run(test.expectedErr.Error(), func(t *testing.T) {
			err := test.config().Validate()
			assert.Equal(t, test.expectedErr, err)
		})
	}
}

func validConfig() Config {

	return Config{
		RootPath:              "some path",
		ID:                    "someid",
		MakeCommitLoggerThunk: func() CommitLogger { return nil },
		VectorForIDThunk:      func(context.Context, int32) ([]float32, error) { return nil, nil },
		EFConstruction:        17,
		MaximumConnections:    50,
	}
}
