package v2_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	v2 "github.com/weaviate/weaviate/usecases/config/v2"
)

func TestFeatureFlagNoLD(t *testing.T) {
	// Test Integer
	fInt := v2.NewFeatureFlag("test-feature-flag", 5)
	require.Equal(t, 5, fInt.Get())

	// Test Float
	fFloat := v2.NewFeatureFlag("test-feature-flag", 5.5)
	require.Equal(t, 5.5, fFloat.Get())

	// Test String
	fString := v2.NewFeatureFlag("test-feature-flag", "my-super-flag")
	require.Equal(t, "my-super-flag", fString.Get())

	// Test Bool
	fBool := v2.NewFeatureFlag("test-feature-flag", true)
	require.Equal(t, true, fBool.Get())
}
