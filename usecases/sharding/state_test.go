package sharding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestState(t *testing.T) {
	cfg, err := ParseConfig(map[string]interface{}{"desiredCount": float64(4)})
	require.Nil(t, err)

	state, err := InitState("my-index", cfg)
	require.Nil(t, err)

	_ = state
}
