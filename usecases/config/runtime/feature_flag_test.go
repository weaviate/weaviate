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

package runtime_test

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestFeatureFlagNoLD(t *testing.T) {
	// Test Integer
	logger, _ := test.NewNullLogger()
	fInt := configRuntime.NewFeatureFlag("test-feature-flag", 5, nil, "", logger)
	require.Equal(t, 5, fInt.Get())

	// Test Float
	fFloat := configRuntime.NewFeatureFlag("test-feature-flag", 5.5, nil, "", logger)
	require.Equal(t, 5.5, fFloat.Get())

	// Test String
	fString := configRuntime.NewFeatureFlag("test-feature-flag", "my-super-flag", nil, "", logger)
	require.Equal(t, "my-super-flag", fString.Get())

	// Test Bool
	fBool := configRuntime.NewFeatureFlag("test-feature-flag", true, nil, "", logger)
	require.Equal(t, true, fBool.Get())
}

func TestFeatureFlagNoLDWithDefaultEnvInt(t *testing.T) {
	os.Setenv("VALID_INT_FF", "3")
	os.Setenv("INVALID_INT_FF", "true")
	logger, _ := test.NewNullLogger()

	fInt := configRuntime.NewFeatureFlag("test-feature-flag", 5, nil, "VALID_INT_FF", logger)
	require.Equal(t, 3, fInt.Get())

	fInt = configRuntime.NewFeatureFlag("test-feature-flag", 5, nil, "INVALID_INT_FF", logger)
	require.Equal(t, 5, fInt.Get())

	fInt = configRuntime.NewFeatureFlag("test-feature-flag", 5, nil, "ABSENT", logger)
	require.Equal(t, 5, fInt.Get())
}

func TestFeatureFlagNoLDWithDefaultEnvFloat(t *testing.T) {
	os.Setenv("VALID_FLOAT_FF", "3.3")
	os.Setenv("VALID_INT_OR_FLOAT_FF", "3")
	os.Setenv("INVALID_FLOAT_FF", "true")
	logger, _ := test.NewNullLogger()

	fFloat := configRuntime.NewFeatureFlag("test-feature-flag", 5.5, nil, "VALID_FLOAT_FF", logger)
	require.Equal(t, 3.3, fFloat.Get())

	fFloat = configRuntime.NewFeatureFlag("test-feature-flag", 5.5, nil, "VALID_INT_OR_FLOAT_FF", logger)
	require.Equal(t, float64(3), fFloat.Get())

	fFloat = configRuntime.NewFeatureFlag("test-feature-flag", 5.5, nil, "INVALID_FLOAT_FF", logger)
	require.Equal(t, 5.5, fFloat.Get())

	fFloat = configRuntime.NewFeatureFlag("test-feature-flag", 5.5, nil, "ABSENT", logger)
	require.Equal(t, 5.5, fFloat.Get())
}

func TestFeatureFlagNoLDWithDefaultEnvString(t *testing.T) {
	os.Setenv("VALID_INT_OR_STRING_FF", "3")
	os.Setenv("VALID_FLOAT_OR_STRING_FF", "3.3")
	os.Setenv("VALID_BOOL_OR_STRING_FF", "true")
	os.Setenv("VALID_STRING_FF", "whatever-i-want")
	logger, _ := test.NewNullLogger()

	fString := configRuntime.NewFeatureFlag("test-feature-flag", "value", nil, "VALID_INT_OR_STRING_FF", logger)
	require.Equal(t, "3", fString.Get())

	fString = configRuntime.NewFeatureFlag("test-feature-flag", "value", nil, "VALID_FLOAT_OR_STRING_FF", logger)
	require.Equal(t, "3.3", fString.Get())

	fString = configRuntime.NewFeatureFlag("test-feature-flag", "value", nil, "VALID_BOOL_OR_STRING_FF", logger)
	require.Equal(t, "true", fString.Get())

	fString = configRuntime.NewFeatureFlag("test-feature-flag", "value", nil, "VALID_STRING_FF", logger)
	require.Equal(t, "whatever-i-want", fString.Get())

	fString = configRuntime.NewFeatureFlag("test-feature-flag", "value", nil, "ABSENT", logger)
	require.Equal(t, "value", fString.Get())
}

func TestFeatureFlagNoLDWithDefaultEnvBool(t *testing.T) {
	os.Setenv("VALID_BOOL_TRUE_FF", "true")
	os.Setenv("VALID_BOOL_FALSE_FF", "false")
	os.Setenv("INVALID_BOOL_FF", "10")
	logger, _ := test.NewNullLogger()

	fBool := configRuntime.NewFeatureFlag("test-feature-flag", false, nil, "VALID_BOOL_TRUE_FF", logger)
	require.Equal(t, true, fBool.Get())

	fBool = configRuntime.NewFeatureFlag("test-feature-flag", true, nil, "VALID_BOOL_FALSE_FF", logger)
	require.Equal(t, false, fBool.Get())

	fBool = configRuntime.NewFeatureFlag("test-feature-flag", true, nil, "INVALID_BOOL_FF", logger)
	require.Equal(t, true, fBool.Get())

	fBool = configRuntime.NewFeatureFlag("test-feature-flag", true, nil, "ABSENT", logger)
	require.Equal(t, true, fBool.Get())
}
