package spfresh

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionMap(t *testing.T) {
	m := NewVersionMap(10, 5)

	for i := range 1000 {
		m.AllocPageFor(uint64(i))
		version, abort := m.Increment(0, uint64(i))
		require.False(t, abort)
		require.Equal(t, VectorVersion(1), version)
	}
}
