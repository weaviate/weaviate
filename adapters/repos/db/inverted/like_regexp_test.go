package inverted

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLikeRegexp(t *testing.T) {
	type test struct {
		input         []byte
		subject       []byte
		shouldMatch   bool
		expectedError error
	}

	run := func(t *testing.T, tests []test) {
		for _, test := range tests {
			t.Run(fmt.Sprintf("for input %q and subject %q", string(test.input),
				string(test.subject)), func(t *testing.T) {
				res, err := parseLikeRegexp(test.input)
				if test.expectedError != nil {
					assert.Equal(t, test.expectedError, err)
					return
				}

				require.Nil(t, err)
				assert.Equal(t, test.shouldMatch, res.regexp.Match(test.subject))
			})
		}
	}

	t.Run("without a wildcard", func(t *testing.T) {
		input := []byte("car")
		tests := []test{
			{input: input, subject: []byte("car"), shouldMatch: true},
			{input: input, subject: []byte("care"), shouldMatch: false},
			{input: input, subject: []byte("supercar"), shouldMatch: false},
		}

		run(t, tests)
	})

	t.Run("with a single-character wildcard", func(t *testing.T) {
		input := []byte("car?")
		tests := []test{
			{input: input, subject: []byte("car"), shouldMatch: false},
			{input: input, subject: []byte("cap"), shouldMatch: false},
			{input: input, subject: []byte("care"), shouldMatch: true},
			{input: input, subject: []byte("supercar"), shouldMatch: false},
			{input: input, subject: []byte("carer"), shouldMatch: false},
		}

		run(t, tests)
	})

	t.Run("with a multi-character wildcard", func(t *testing.T) {
		input := []byte("car*")
		tests := []test{
			{input: input, subject: []byte("car"), shouldMatch: true},
			{input: input, subject: []byte("cap"), shouldMatch: false},
			{input: input, subject: []byte("care"), shouldMatch: true},
			{input: input, subject: []byte("supercar"), shouldMatch: false},
			{input: input, subject: []byte("carer"), shouldMatch: true},
		}

		run(t, tests)
	})

	t.Run("with several wildcards", func(t *testing.T) {
		input := []byte("*c?r*")
		tests := []test{
			{input: input, subject: []byte("car"), shouldMatch: true},
			{input: input, subject: []byte("cap"), shouldMatch: false},
			{input: input, subject: []byte("care"), shouldMatch: true},
			{input: input, subject: []byte("supercar"), shouldMatch: true},
			{input: input, subject: []byte("carer"), shouldMatch: true},
		}

		run(t, tests)
	})
}
