package argon

import (
	"testing"

	"github.com/alexedwards/argon2id"
	"github.com/stretchr/testify/require"
)

func BenchmarkName(b *testing.B) {
	tests := []struct {
		name   string
		params *argon2id.Params
	}{
		{
			name:   "default",
			params: argon2id.DefaultParams,
		},
		{
			name:   "second",
			params: &argon2id.Params{Memory: 64 * 1024, Parallelism: 8, Iterations: 3, SaltLength: 16, KeyLength: 32},
		},
		{
			name:   "lower second",
			params: &argon2id.Params{Memory: 1000 * 1024, Parallelism: 8, Iterations: 1, SaltLength: 16, KeyLength: 32},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			hash, err := argon2id.CreateHash("pa$$word", tt.params)
			require.NoError(b, err)
			for i := 0; i < b.N; i++ {
				match, err := argon2id.ComparePasswordAndHash("pa$$word", hash)
				require.NoError(b, err)
				require.True(b, match)
			}
		})
	}
}
