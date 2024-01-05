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

package sorter

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasicComparator_String(t *testing.T) {
	Orange := "Orange"
	orange := "orange"
	apple := "apple"

	t.Run("strings asc", func(t *testing.T) {
		comp := newStringComparator("asc")

		params := []struct {
			a        *string
			b        *string
			expected int
		}{
			{&Orange, &orange, 0},
			{&orange, &orange, 0},
			{&apple, &apple, 0},
			{&orange, &apple, 1},
			{&apple, &orange, -1},
			{nil, &apple, -1},
			{&orange, nil, 1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})

	t.Run("strings desc", func(t *testing.T) {
		comp := newStringComparator("desc")

		params := []struct {
			a        *string
			b        *string
			expected int
		}{
			{&Orange, &orange, 0},
			{&orange, &orange, 0},
			{&apple, &apple, 0},
			{&orange, &apple, -1},
			{&apple, &orange, 1},
			{nil, &apple, 1},
			{&orange, nil, -1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})
}

func TestBasicComparator_StringArray(t *testing.T) {
	o_b_a := []string{"orange", "banana", "apple"}
	p := []string{"pear"}
	o_a := []string{"orange", "apple"}
	o_b := []string{"orange", "banana"}

	t.Run("strings array asc", func(t *testing.T) {
		comp := newStringArrayComparator("asc")

		params := []struct {
			a        *[]string
			b        *[]string
			expected int
		}{
			{&o_b_a, &o_b_a, 0},
			{&p, &p, 0},
			{&o_a, &o_a, 0},
			{&o_b, &o_b, 0},
			{&o_a, &o_b, -1},
			{&o_b, &o_b_a, -1},
			{&p, &o_b_a, 1},
			{nil, &o_a, -1},
			{&p, nil, 1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})

	t.Run("strings array desc", func(t *testing.T) {
		comp := newStringArrayComparator("desc")

		params := []struct {
			a        *[]string
			b        *[]string
			expected int
		}{
			{&o_b_a, &o_b_a, 0},
			{&p, &p, 0},
			{&o_a, &o_a, 0},
			{&o_b, &o_b, 0},
			{&o_a, &o_b, 1},
			{&o_b, &o_b_a, 1},
			{&p, &o_b_a, -1},
			{nil, &o_a, 1},
			{&p, nil, -1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})
}

func TestBasicComparator_Float64(t *testing.T) {
	f_10 := -10.0
	f100 := 100.0
	f0 := 0.0

	t.Run("floats asc", func(t *testing.T) {
		comp := newFloat64Comparator("asc")

		params := []struct {
			a        *float64
			b        *float64
			expected int
		}{
			{&f_10, &f_10, 0},
			{&f100, &f100, 0},
			{&f0, &f0, 0},
			{&f100, &f_10, 1},
			{&f0, &f100, -1},
			{nil, &f_10, -1},
			{&f0, nil, 1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})

	t.Run("floats desc", func(t *testing.T) {
		comp := newFloat64Comparator("desc")

		params := []struct {
			a        *float64
			b        *float64
			expected int
		}{
			{&f_10, &f_10, 0},
			{&f100, &f100, 0},
			{&f0, &f0, 0},
			{&f100, &f_10, -1},
			{&f0, &f100, 1},
			{nil, &f_10, 1},
			{&f0, nil, -1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})
}

func TestBasicComparator_Float64Array(t *testing.T) {
	f_3_2_1 := []float64{3, 2, 1}
	f_4 := []float64{4}
	f_3_1 := []float64{3, 1}
	f_3_2 := []float64{3, 2}

	t.Run("floats array asc", func(t *testing.T) {
		comp := newFloat64ArrayComparator("asc")

		params := []struct {
			a        *[]float64
			b        *[]float64
			expected int
		}{
			{&f_3_2_1, &f_3_2_1, 0},
			{&f_4, &f_4, 0},
			{&f_3_1, &f_3_1, 0},
			{&f_3_2, &f_3_2, 0},
			{&f_3_1, &f_3_2, -1},
			{&f_3_2, &f_3_2_1, -1},
			{&f_4, &f_3_2_1, 1},
			{nil, &f_3_1, -1},
			{&f_4, nil, 1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})

	t.Run("floats array desc", func(t *testing.T) {
		comp := newFloat64ArrayComparator("desc")

		params := []struct {
			a        *[]float64
			b        *[]float64
			expected int
		}{
			{&f_3_2_1, &f_3_2_1, 0},
			{&f_4, &f_4, 0},
			{&f_3_1, &f_3_1, 0},
			{&f_3_2, &f_3_2, 0},
			{&f_3_1, &f_3_2, 1},
			{&f_3_2, &f_3_2_1, 1},
			{&f_4, &f_3_2_1, -1},
			{nil, &f_3_1, 1},
			{&f_4, nil, -1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})
}

func TestBasicComparator_Date(t *testing.T) {
	t1 := time.Now()
	t2 := time.Now().Add(time.Second)
	t3 := time.Now().Add(2 * time.Second)

	t.Run("dates asc", func(t *testing.T) {
		comp := newDateComparator("asc")

		params := []struct {
			a        *time.Time
			b        *time.Time
			expected int
		}{
			{&t1, &t1, 0},
			{&t3, &t3, 0},
			{&t2, &t2, 0},
			{&t3, &t1, 1},
			{&t2, &t3, -1},
			{nil, &t1, -1},
			{&t2, nil, 1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})

	t.Run("dates desc", func(t *testing.T) {
		comp := newDateComparator("desc")

		params := []struct {
			a        *time.Time
			b        *time.Time
			expected int
		}{
			{&t1, &t1, 0},
			{&t3, &t3, 0},
			{&t2, &t2, 0},
			{&t3, &t1, -1},
			{&t2, &t3, 1},
			{nil, &t1, 1},
			{&t2, nil, -1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})
}

func TestBasicComparator_DateArray(t *testing.T) {
	t1 := time.Now()
	t2 := time.Now().Add(time.Second)
	t3 := time.Now().Add(2 * time.Second)
	t4 := time.Now().Add(3 * time.Second)

	t_3_2_1 := []time.Time{t3, t2, t1}
	t_4 := []time.Time{t4}
	t_3_1 := []time.Time{t3, t1}
	t_3_2 := []time.Time{t3, t2}

	t.Run("dates array asc", func(t *testing.T) {
		comp := newDateArrayComparator("asc")

		params := []struct {
			a        *[]time.Time
			b        *[]time.Time
			expected int
		}{
			{&t_3_2_1, &t_3_2_1, 0},
			{&t_4, &t_4, 0},
			{&t_3_1, &t_3_1, 0},
			{&t_3_2, &t_3_2, 0},
			{&t_3_1, &t_3_2, -1},
			{&t_3_2, &t_3_2_1, -1},
			{&t_4, &t_3_2_1, 1},
			{nil, &t_3_1, -1},
			{&t_4, nil, 1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})

	t.Run("dates array desc", func(t *testing.T) {
		comp := newDateArrayComparator("desc")

		params := []struct {
			a        *[]time.Time
			b        *[]time.Time
			expected int
		}{
			{&t_3_2_1, &t_3_2_1, 0},
			{&t_4, &t_4, 0},
			{&t_3_1, &t_3_1, 0},
			{&t_3_2, &t_3_2, 0},
			{&t_3_1, &t_3_2, 1},
			{&t_3_2, &t_3_2_1, 1},
			{&t_4, &t_3_2_1, -1},
			{nil, &t_3_1, 1},
			{&t_4, nil, -1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})
}

func TestBasicComparator_Bool(t *testing.T) {
	fa := false
	tr := true

	t.Run("bools asc", func(t *testing.T) {
		comp := newBoolComparator("asc")

		params := []struct {
			a        *bool
			b        *bool
			expected int
		}{
			{&fa, &fa, 0},
			{&tr, &tr, 0},
			{&fa, &tr, -1},
			{nil, &fa, -1},
			{&tr, nil, 1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})

	t.Run("bools desc", func(t *testing.T) {
		comp := newBoolComparator("desc")

		params := []struct {
			a        *bool
			b        *bool
			expected int
		}{
			{&fa, &fa, 0},
			{&tr, &tr, 0},
			{&fa, &tr, 1},
			{nil, &fa, 1},
			{&tr, nil, -1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})
}

func TestBasicComparator_BoolArray(t *testing.T) {
	fa_tr_fa := []bool{false, true, false}
	tr := []bool{true}
	fa_fa := []bool{false, false}
	fa_tr := []bool{false, true}

	t.Run("bools array asc", func(t *testing.T) {
		comp := newBoolArrayComparator("asc")

		params := []struct {
			a        *[]bool
			b        *[]bool
			expected int
		}{
			{&fa_tr_fa, &fa_tr_fa, 0},
			{&tr, &tr, 0},
			{&fa_fa, &fa_fa, 0},
			{&fa_tr, &fa_tr, 0},
			{&fa_fa, &fa_tr, -1},
			{&fa_tr, &fa_tr_fa, -1},
			{&tr, &fa_tr_fa, 1},
			{nil, &fa_fa, -1},
			{&tr, nil, 1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})

	t.Run("bools array desc", func(t *testing.T) {
		comp := newBoolArrayComparator("desc")

		params := []struct {
			a        *[]bool
			b        *[]bool
			expected int
		}{
			{&fa_tr_fa, &fa_tr_fa, 0},
			{&tr, &tr, 0},
			{&fa_fa, &fa_fa, 0},
			{&fa_tr, &fa_tr, 0},
			{&fa_fa, &fa_tr, 1},
			{&fa_tr, &fa_tr_fa, 1},
			{&tr, &fa_tr_fa, -1},
			{nil, &fa_fa, 1},
			{&tr, nil, -1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})
}

func TestBasicComparator_Int(t *testing.T) {
	i_10 := -10
	i100 := 100
	i0 := 0

	t.Run("ints asc", func(t *testing.T) {
		comp := newIntComparator("asc")

		params := []struct {
			a        *int
			b        *int
			expected int
		}{
			{&i_10, &i_10, 0},
			{&i100, &i100, 0},
			{&i0, &i0, 0},
			{&i100, &i_10, 1},
			{&i0, &i100, -1},
			{nil, &i_10, -1},
			{&i0, nil, 1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})

	t.Run("ints desc", func(t *testing.T) {
		comp := newIntComparator("desc")

		params := []struct {
			a        *int
			b        *int
			expected int
		}{
			{&i_10, &i_10, 0},
			{&i100, &i100, 0},
			{&i0, &i0, 0},
			{&i100, &i_10, -1},
			{&i0, &i100, 1},
			{nil, &i_10, 1},
			{&i0, nil, -1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})
}

func TestBasicComparator_Any(t *testing.T) {
	in := -10
	fl := 100.0
	st := "string"
	ti := time.Now()
	bo := true
	an := struct{}{}

	t.Run("any asc", func(t *testing.T) {
		comp := newAnyComparator("asc")

		params := []struct {
			a        interface{}
			b        interface{}
			expected int
		}{
			{&in, &in, 0},
			{&fl, &fl, 0},
			{&st, &st, 0},
			{&ti, &ti, 0},
			{&bo, &bo, 0},
			{&an, &an, 0},
			{&in, &fl, 0},
			{&fl, &st, 0},
			{&st, &ti, 0},
			{&ti, &bo, 0},
			{&bo, &an, 0},
			{&an, &in, 0},
			{nil, &in, -1},
			{nil, &fl, -1},
			{nil, &st, -1},
			{&ti, nil, 1},
			{&bo, nil, 1},
			{&an, nil, 1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})

	t.Run("any desc", func(t *testing.T) {
		comp := newAnyComparator("desc")

		params := []struct {
			a        interface{}
			b        interface{}
			expected int
		}{
			{&in, &in, 0},
			{&fl, &fl, 0},
			{&st, &st, 0},
			{&ti, &ti, 0},
			{&bo, &bo, 0},
			{&an, &an, 0},
			{&in, &fl, 0},
			{&fl, &st, 0},
			{&st, &ti, 0},
			{&ti, &bo, 0},
			{&bo, &an, 0},
			{&an, &in, 0},
			{nil, &in, 1},
			{nil, &fl, 1},
			{nil, &st, 1},
			{&ti, nil, -1},
			{&bo, nil, -1},
			{&an, nil, -1},
			{nil, nil, 0},
		}

		for i, p := range params {
			t.Run(fmt.Sprintf("data #%d", i), func(t *testing.T) {
				assert.Equal(t, p.expected, comp.compare(p.a, p.b))
			})
		}
	})
}
