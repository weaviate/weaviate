//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package replica

import (
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ValidateConfig(t *testing.T) {
	tests := []struct {
		name          string
		initialconfig *models.ReplicationConfig
		resultConfig  *models.ReplicationConfig
		expectedErr   error
	}{
		{
			name:          "config not provided",
			initialconfig: nil,
			resultConfig:  &models.ReplicationConfig{Factor: 1},
		},
		{
			name:          "config provided, factor not provided",
			initialconfig: &models.ReplicationConfig{},
			resultConfig:  &models.ReplicationConfig{Factor: 1},
		},
		{
			name:          "config provided, factor < 0",
			initialconfig: &models.ReplicationConfig{Factor: -1},
			resultConfig:  &models.ReplicationConfig{Factor: 1},
		},
		{
			name:          "config provided, valid factor",
			initialconfig: &models.ReplicationConfig{Factor: 7},
			resultConfig:  &models.ReplicationConfig{Factor: 7},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			class := &models.Class{
				ReplicationConfig: test.initialconfig,
			}
			err := ValidateConfig(class)
			if test.expectedErr != nil {
				assert.EqualError(t, test.expectedErr, err.Error())
			} else {
				assert.Nil(t, err)
				assert.EqualValues(t, test.resultConfig, class.ReplicationConfig)
			}
		})
	}
}

func Test_ValidateConfigUpdate(t *testing.T) {
	tests := []struct {
		name          string
		initial       *models.ReplicationConfig
		update        *models.ReplicationConfig
		expectedError error
	}{
		{
			name:    "attempting to increase replicas beyond cluster size",
			initial: &models.ReplicationConfig{Factor: 3},
			update:  &models.ReplicationConfig{Factor: 4},
			expectedError: fmt.Errorf(
				"cannot scale to 4 replicas, cluster has only 3 nodes"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateConfigUpdate(
				&models.Class{ReplicationConfig: test.initial},
				&models.Class{ReplicationConfig: test.update},
				&fakeNodeCounter{3})
			if test.expectedError == nil {
				assert.Nil(t, err)
			} else {
				require.NotNil(t, err, "update validation must error")
				assert.Equal(t, test.expectedError.Error(), err.Error())
			}
		})
	}
}

type fakeNodeCounter struct{ count int }

func (f *fakeNodeCounter) NodeCount() int {
	return f.count
}
