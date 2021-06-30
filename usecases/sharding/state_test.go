package sharding

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestState(t *testing.T) {
	size := 1000

	cfg, err := ParseConfig(map[string]interface{}{"desiredCount": float64(4)})
	require.Nil(t, err)

	state, err := InitState("my-index", cfg)
	require.Nil(t, err)

	physicalCount := map[string]int{}

	for i := 0; i < size; i++ {
		name := make([]byte, 16)
		rand.Read(name)

		phid := state.Physical(name)
		physicalCount[phid]++
	}

	// verify each shard contains at least 15% of data. The expected value would
	// be 25%, but since this is random, we should take a lower value to reduce
	// flakyness

	for name, count := range physicalCount {
		if owns := float64(count) / float64(size); owns < 0.15 {
			t.Errorf("expected shard %q to own at least 15%%, but it only owns %f", name, owns)
		}
	}
}
