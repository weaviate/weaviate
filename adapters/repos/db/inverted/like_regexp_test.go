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

func TestLikeRegexp_ForOptimizability(t *testing.T) {
	type test struct {
		input               []byte
		shouldBeOptimizable bool
		expectedMin         []byte
	}

	run := func(t *testing.T, tests []test) {
		for _, test := range tests {
			t.Run(fmt.Sprintf("for input %q", string(test.input)), func(t *testing.T) {
				res, err := parseLikeRegexp(test.input)
				require.Nil(t, err)
				assert.Equal(t, test.shouldBeOptimizable, res.optimizable)
				assert.Equal(t, test.expectedMin, res.min)
			})
		}
	}

	tests := []test{
		{input: []byte("car"), shouldBeOptimizable: true, expectedMin: []byte("car")},
		{input: []byte("car*"), shouldBeOptimizable: true, expectedMin: []byte("car")},
		{input: []byte("car?"), shouldBeOptimizable: true, expectedMin: []byte("car")},
		{input: []byte("c?r"), shouldBeOptimizable: true, expectedMin: []byte("c")},
		{input: []byte("car*taker"), shouldBeOptimizable: true, expectedMin: []byte("car")},
		{input: []byte("car?tak*?*er"), shouldBeOptimizable: true, expectedMin: []byte("car")},
		{input: []byte("?car"), shouldBeOptimizable: false, expectedMin: []byte{}},
		{input: []byte("*car"), shouldBeOptimizable: false, expectedMin: []byte{}},
	}

	run(t, tests)
}
