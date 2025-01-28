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

	"github.com/stretchr/testify/require"
	runtimeConfig "github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestFeatureFlagNoLD(t *testing.T) {
	// Test Integer
	fInt := runtimeConfig.NewFeatureFlag("test-feature-flag", 5)
	require.Equal(t, 5, fInt.Get())

	// Test Float
	fFloat := runtimeConfig.NewFeatureFlag("test-feature-flag", 5.5)
	require.Equal(t, 5.5, fFloat.Get())

	// Test String
	fString := runtimeConfig.NewFeatureFlag("test-feature-flag", "my-super-flag")
	require.Equal(t, "my-super-flag", fString.Get())

	// Test Bool
	fBool := runtimeConfig.NewFeatureFlag("test-feature-flag", true)
	require.Equal(t, true, fBool.Get())
}

func TestFeatureFlagNoLDWithDefaultEnvInt(t *testing.T) {
	os.Setenv("VALID_INT_FF", "3")
	os.Setenv("INVALID_INT_FF", "true")

	fInt := runtimeConfig.NewFeatureFlag("test-feature-flag", 5).WithEnvDefault("VALID_INT_FF")
	require.Equal(t, 3, fInt.Get())

	fInt = runtimeConfig.NewFeatureFlag("test-feature-flag", 5).WithEnvDefault("INVALID_INT_FF")
	require.Equal(t, 5, fInt.Get())

	fInt = runtimeConfig.NewFeatureFlag("test-feature-flag", 5).WithEnvDefault("ABSENT")
	require.Equal(t, 5, fInt.Get())
}

func TestFeatureFlagNoLDWithDefaultEnvFloat(t *testing.T) {
	os.Setenv("VALID_FLOAT_FF", "3.3")
	os.Setenv("VALID_INT_OR_FLOAT_FF", "3")
	os.Setenv("INVALID_FLOAT_FF", "true")

	fFloat := runtimeConfig.NewFeatureFlag("test-feature-flag", 5.5).WithEnvDefault("VALID_FLOAT_FF")
	require.Equal(t, 3.3, fFloat.Get())

	fFloat = runtimeConfig.NewFeatureFlag("test-feature-flag", 5.5).WithEnvDefault("VALID_INT_OR_FLOAT_FF")
	require.Equal(t, float64(3), fFloat.Get())

	fFloat = runtimeConfig.NewFeatureFlag("test-feature-flag", 5.5).WithEnvDefault("INVALID_FLOAT_FF")
	require.Equal(t, 5.5, fFloat.Get())

	fFloat = runtimeConfig.NewFeatureFlag("test-feature-flag", 5.5).WithEnvDefault("ABSENT")
	require.Equal(t, 5.5, fFloat.Get())
}

func TestFeatureFlagNoLDWithDefaultEnvString(t *testing.T) {
	os.Setenv("VALID_INT_OR_STRING_FF", "3")
	os.Setenv("VALID_FLOAT_OR_STRING_FF", "3.3")
	os.Setenv("VALID_BOOL_OR_STRING_FF", "true")
	os.Setenv("VALID_STRING_FF", "whatever-i-want")

	fString := runtimeConfig.NewFeatureFlag("test-feature-flag", "value").WithEnvDefault("VALID_INT_OR_STRING_FF")
	require.Equal(t, "3", fString.Get())

	fString = runtimeConfig.NewFeatureFlag("test-feature-flag", "value").WithEnvDefault("VALID_FLOAT_OR_STRING_FF")
	require.Equal(t, "3.3", fString.Get())

	fString = runtimeConfig.NewFeatureFlag("test-feature-flag", "value").WithEnvDefault("VALID_BOOL_OR_STRING_FF")
	require.Equal(t, "true", fString.Get())

	fString = runtimeConfig.NewFeatureFlag("test-feature-flag", "value").WithEnvDefault("VALID_STRING_FF")
	require.Equal(t, "whatever-i-want", fString.Get())

	fString = runtimeConfig.NewFeatureFlag("test-feature-flag", "value").WithEnvDefault("ABSENT")
	require.Equal(t, "value", fString.Get())
}

func TestFeatureFlagNoLDWithDefaultEnvBool(t *testing.T) {
	os.Setenv("VALID_BOOL_TRUE_FF", "true")
	os.Setenv("VALID_BOOL_FALSE_FF", "false")
	os.Setenv("INVALID_BOOL_FF", "10")

	fBool := runtimeConfig.NewFeatureFlag("test-feature-flag", false).WithEnvDefault("VALID_BOOL_TRUE_FF")
	require.Equal(t, true, fBool.Get())

	fBool = runtimeConfig.NewFeatureFlag("test-feature-flag", true).WithEnvDefault("VALID_BOOL_FALSE_FF")
	require.Equal(t, false, fBool.Get())

	fBool = runtimeConfig.NewFeatureFlag("test-feature-flag", true).WithEnvDefault("INVALID_BOOL_FF")
	require.Equal(t, true, fBool.Get())

	fBool = runtimeConfig.NewFeatureFlag("test-feature-flag", true).WithEnvDefault("ABSENT")
	require.Equal(t, true, fBool.Get())
}
