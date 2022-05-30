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

package sorter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_comparator_compare_functions(t *testing.T) {
	t.Run("compare string asc", func(t *testing.T) {
		// given
		cmp := newComparator("asc")
		a := "bee"
		b := "action"
		// when
		res2values := cmp.compareString(&a, &b)
		resFirstValueNil := cmp.compareString(nil, &b)
		resSecondValueNil := cmp.compareString(&a, nil)
		resNilValues := cmp.compareString(nil, nil)
		// then
		assert.False(t, res2values)
		assert.True(t, resFirstValueNil)
		assert.False(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare string desc", func(t *testing.T) {
		// given
		cmp := newComparator("desc")
		a := "bee"
		b := "action"
		// when
		res2values := cmp.compareString(&a, &b)
		resFirstValueNil := cmp.compareString(nil, &b)
		resSecondValueNil := cmp.compareString(&a, nil)
		resNilValues := cmp.compareString(nil, nil)
		// then
		assert.True(t, res2values)
		assert.False(t, resFirstValueNil)
		assert.True(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare float64 asc", func(t *testing.T) {
		// given
		cmp := newComparator("asc")
		a := float64(100)
		b := float64(-100)
		// when
		res2values := cmp.compareFloat64(&a, &b)
		resFirstValueNil := cmp.compareFloat64(nil, &b)
		resSecondValueNil := cmp.compareFloat64(&a, nil)
		resNilValues := cmp.compareFloat64(nil, nil)
		// then
		assert.False(t, res2values)
		assert.True(t, resFirstValueNil)
		assert.False(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare float64 desc", func(t *testing.T) {
		// given
		cmp := newComparator("desc")
		a := float64(100)
		b := float64(-100)
		// when
		res2values := cmp.compareFloat64(&a, &b)
		resFirstValueNil := cmp.compareFloat64(nil, &b)
		resSecondValueNil := cmp.compareFloat64(&a, nil)
		resNilValues := cmp.compareFloat64(nil, nil)
		// then
		assert.True(t, res2values)
		assert.False(t, resFirstValueNil)
		assert.True(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare date asc", func(t *testing.T) {
		// given
		cmp := newComparator("asc")
		a, err := newSortBy(cmp).parseDate("2000-01-01T00:00:00+02:00")
		require.Nil(t, err)
		b, err := newSortBy(cmp).parseDate("1900-01-01T00:00:00+02:00")
		require.Nil(t, err)
		// when
		res2values := cmp.compareDate(&a, &b)
		resFirstValueNil := cmp.compareDate(nil, &b)
		resSecondValueNil := cmp.compareDate(&a, nil)
		resNilValues := cmp.compareDate(nil, nil)
		// then
		assert.False(t, res2values)
		assert.True(t, resFirstValueNil)
		assert.False(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare date desc", func(t *testing.T) {
		// given
		cmp := newComparator("desc")
		a, err := newSortBy(cmp).parseDate("2000-01-01T00:00:00+02:00")
		require.Nil(t, err)
		b, err := newSortBy(cmp).parseDate("1900-01-01T00:00:00+02:00")
		require.Nil(t, err)
		// when
		res2values := cmp.compareDate(&a, &b)
		resFirstValueNil := cmp.compareDate(nil, &b)
		resSecondValueNil := cmp.compareDate(&a, nil)
		resNilValues := cmp.compareDate(nil, nil)
		// then
		assert.True(t, res2values)
		assert.False(t, resFirstValueNil)
		assert.True(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare bool asc", func(t *testing.T) {
		// given
		cmp := newComparator("asc")
		a := false
		b := false
		// when
		res2values := cmp.compareBool(&a, &b)
		resFirstValueNil := cmp.compareBool(nil, &b)
		resSecondValueNil := cmp.compareBool(&a, nil)
		resNilValues := cmp.compareBool(nil, nil)
		// then
		assert.False(t, res2values)
		assert.True(t, resFirstValueNil)
		assert.False(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare bool desc", func(t *testing.T) {
		// given
		cmp := newComparator("desc")
		a := false
		b := false
		// when
		res2values := cmp.compareBool(&a, &b)
		resFirstValueNil := cmp.compareBool(nil, &b)
		resSecondValueNil := cmp.compareBool(&a, nil)
		resNilValues := cmp.compareBool(nil, nil)
		// then
		assert.True(t, res2values)
		assert.False(t, resFirstValueNil)
		assert.True(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})
}
