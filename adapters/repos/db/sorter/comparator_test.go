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
		sorter := comparator{order: "asc"}
		a := "bee"
		b := "action"
		// when
		res2values := sorter.compareString(&a, &b)
		resFirstValueNil := sorter.compareString(nil, &b)
		resSecondValueNil := sorter.compareString(&a, nil)
		resNilValues := sorter.compareString(nil, nil)
		// then
		assert.False(t, res2values)
		assert.True(t, resFirstValueNil)
		assert.False(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare string desc", func(t *testing.T) {
		// given
		sorter := comparator{order: "desc"}
		a := "bee"
		b := "action"
		// when
		res2values := sorter.compareString(&a, &b)
		resFirstValueNil := sorter.compareString(nil, &b)
		resSecondValueNil := sorter.compareString(&a, nil)
		resNilValues := sorter.compareString(nil, nil)
		// then
		assert.True(t, res2values)
		assert.False(t, resFirstValueNil)
		assert.True(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare float64 asc", func(t *testing.T) {
		// given
		sorter := comparator{order: "asc"}
		a := float64(100)
		b := float64(-100)
		// when
		res2values := sorter.compareFloat64(&a, &b)
		resFirstValueNil := sorter.compareFloat64(nil, &b)
		resSecondValueNil := sorter.compareFloat64(&a, nil)
		resNilValues := sorter.compareFloat64(nil, nil)
		// then
		assert.False(t, res2values)
		assert.True(t, resFirstValueNil)
		assert.False(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare float64 desc", func(t *testing.T) {
		// given
		sorter := comparator{order: "desc"}
		a := float64(100)
		b := float64(-100)
		// when
		res2values := sorter.compareFloat64(&a, &b)
		resFirstValueNil := sorter.compareFloat64(nil, &b)
		resSecondValueNil := sorter.compareFloat64(&a, nil)
		resNilValues := sorter.compareFloat64(nil, nil)
		// then
		assert.True(t, res2values)
		assert.False(t, resFirstValueNil)
		assert.True(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare date asc", func(t *testing.T) {
		// given
		sorter := comparator{order: "asc"}
		a, err := sortBy{}.parseDate("2000-01-01T00:00:00+02:00")
		require.Nil(t, err)
		b, err := sortBy{}.parseDate("1900-01-01T00:00:00+02:00")
		require.Nil(t, err)
		// when
		res2values := sorter.compareDate(&a, &b)
		resFirstValueNil := sorter.compareDate(nil, &b)
		resSecondValueNil := sorter.compareDate(&a, nil)
		resNilValues := sorter.compareDate(nil, nil)
		// then
		assert.False(t, res2values)
		assert.True(t, resFirstValueNil)
		assert.False(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare date desc", func(t *testing.T) {
		// given
		sorter := comparator{order: "desc"}
		a, err := sortBy{}.parseDate("2000-01-01T00:00:00+02:00")
		require.Nil(t, err)
		b, err := sortBy{}.parseDate("1900-01-01T00:00:00+02:00")
		require.Nil(t, err)
		// when
		res2values := sorter.compareDate(&a, &b)
		resFirstValueNil := sorter.compareDate(nil, &b)
		resSecondValueNil := sorter.compareDate(&a, nil)
		resNilValues := sorter.compareDate(nil, nil)
		// then
		assert.True(t, res2values)
		assert.False(t, resFirstValueNil)
		assert.True(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare bool asc", func(t *testing.T) {
		// given
		sorter := comparator{order: "asc"}
		a := false
		b := false
		// when
		res2values := sorter.compareBool(&a, &b)
		resFirstValueNil := sorter.compareBool(nil, &b)
		resSecondValueNil := sorter.compareBool(&a, nil)
		resNilValues := sorter.compareBool(nil, nil)
		// then
		assert.False(t, res2values)
		assert.True(t, resFirstValueNil)
		assert.False(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})

	t.Run("compare bool desc", func(t *testing.T) {
		// given
		sorter := comparator{order: "desc"}
		a := false
		b := false
		// when
		res2values := sorter.compareBool(&a, &b)
		resFirstValueNil := sorter.compareBool(nil, &b)
		resSecondValueNil := sorter.compareBool(&a, nil)
		resNilValues := sorter.compareBool(nil, nil)
		// then
		assert.True(t, res2values)
		assert.False(t, resFirstValueNil)
		assert.True(t, resSecondValueNil)
		assert.False(t, resNilValues)
	})
}
