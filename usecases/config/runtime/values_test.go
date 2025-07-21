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

package runtime

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDynamicValue_YAML(t *testing.T) {
	t.Run("YAML unmarshal should always set `default` value", func(t *testing.T) {
		val := struct {
			Foo    *DynamicValue[int]           `yaml:"foo"`
			Bar    *DynamicValue[float64]       `yaml:"bar"`
			Alice  *DynamicValue[bool]          `yaml:"alice"`
			Dave   *DynamicValue[time.Duration] `yaml:"dave"`
			Status *DynamicValue[string]        `yaml:"status"`
			Slice  *DynamicValue[[]string]      `yaml:"slice"`
		}{}
		buf := `
foo: 2
bar: 4.5
alice: true
dave: 20s
status: "done"
slice: ["one", "two", "three"]
`
		dec := yaml.NewDecoder(strings.NewReader(buf))
		dec.KnownFields(true)
		err := dec.Decode(&val)
		require.NoError(t, err)

		assert.Equal(t, 2, val.Foo.def)
		assert.Equal(t, 2, val.Foo.Get())
		assert.Nil(t, nil, val.Foo.val)

		assert.Equal(t, 4.5, val.Bar.def)
		assert.Equal(t, 4.5, val.Bar.Get())
		assert.Nil(t, val.Bar.val)

		assert.Equal(t, true, val.Alice.def)
		assert.Equal(t, true, val.Alice.Get())
		assert.Nil(t, val.Alice.val)

		assert.Equal(t, 20*time.Second, val.Dave.def)
		assert.Equal(t, 20*time.Second, val.Dave.Get())
		assert.Nil(t, val.Dave.val)

		assert.Equal(t, "done", val.Status.def)
		assert.Equal(t, "done", val.Status.Get())
		assert.Nil(t, val.Status.val)

		assert.Equal(t, []string{"one", "two", "three"}, val.Slice.def)
		assert.Equal(t, []string{"one", "two", "three"}, val.Slice.Get())
		assert.Nil(t, val.Slice.val)
	})
}

func TestDynamicValue_JSON(t *testing.T) {
	t.Run("JSON unmarshal should always set `default` value", func(t *testing.T) {
		val := struct {
			Foo    *DynamicValue[int]           `json:"foo"`
			Bar    *DynamicValue[float64]       `json:"bar"`
			Alice  *DynamicValue[bool]          `json:"alice"`
			Dave   *DynamicValue[time.Duration] `json:"dave"`
			Status *DynamicValue[string]        `json:"status"`
			Slice  *DynamicValue[[]string]      `json:"slice"`
		}{}
		buf := `
foo: 2
bar: 4.5
alice: true
dave: 20s
status: "done"
slice: ["one", "two", "three"]
`
		dec := yaml.NewDecoder(strings.NewReader(buf))
		dec.KnownFields(true)
		err := dec.Decode(&val)
		require.NoError(t, err)

		assert.Equal(t, 2, val.Foo.def)
		assert.Equal(t, 2, val.Foo.Get())
		assert.Nil(t, nil, val.Foo.val)

		assert.Equal(t, 4.5, val.Bar.def)
		assert.Equal(t, 4.5, val.Bar.Get())
		assert.Nil(t, val.Bar.val)

		assert.Equal(t, true, val.Alice.def)
		assert.Equal(t, true, val.Alice.Get())
		assert.Nil(t, val.Alice.val)

		assert.Equal(t, 20*time.Second, val.Dave.def)
		assert.Equal(t, 20*time.Second, val.Dave.Get())
		assert.Nil(t, val.Dave.val)

		assert.Equal(t, "done", val.Status.def)
		assert.Equal(t, "done", val.Status.Get())
		assert.Nil(t, val.Status.val)

		assert.Equal(t, []string{"one", "two", "three"}, val.Slice.def)
		assert.Equal(t, []string{"one", "two", "three"}, val.Slice.Get())
		assert.Nil(t, val.Slice.val)
	})
}

func TestDynamicValue(t *testing.T) {
	var (
		dInt      DynamicValue[int]
		dFloat    DynamicValue[float64]
		dBool     DynamicValue[bool]
		dDuration DynamicValue[time.Duration]
		dString   DynamicValue[string]
		dSlice    DynamicValue[[]string]
	)

	// invariant: Zero value of any `DynamicValue` is usable and should return correct
	// underlying `zero-value` as default.
	assert.Equal(t, int(0), dInt.Get())
	assert.Equal(t, float64(0), dFloat.Get())
	assert.Equal(t, false, dBool.Get())
	assert.Equal(t, time.Duration(0), dDuration.Get())
	assert.Equal(t, "", dString.Get())
	assert.Equal(t, []string(nil), dSlice.Get())

	// invariant: `NewDynamicValue` constructor should set custom default and should override
	// the `zero-value`
	dInt2 := NewDynamicValue(25)
	dFloat2 := NewDynamicValue(18.6)
	dBool2 := NewDynamicValue(true)
	dDuration2 := NewDynamicValue(4 * time.Second)
	dString2 := NewDynamicValue("progress")
	dSlice2 := NewDynamicValue([]string{"one", "two"})

	assert.Equal(t, int(25), dInt2.Get())
	assert.Equal(t, float64(18.6), dFloat2.Get())
	assert.Equal(t, true, dBool2.Get())
	assert.Equal(t, time.Duration(4*time.Second), dDuration2.Get())
	assert.Equal(t, "progress", dString2.Get())
	assert.Equal(t, []string{"one", "two"}, dSlice2.Get())

	// invariant: After setting dynamic default via `SetValue`, this value should
	// override both `zero-value` and `custom-default`.
	dInt.SetValue(30)
	dFloat.SetValue(22.7)
	dBool.SetValue(false)
	dDuration.SetValue(10 * time.Second)
	dString.SetValue("backlog")
	dSlice.SetValue([]string{})

	assert.Equal(t, int(30), dInt.Get())
	assert.Equal(t, float64(22.7), dFloat.Get())
	assert.Equal(t, false, dBool.Get())
	assert.Equal(t, time.Duration(10*time.Second), dDuration.Get())
	assert.Equal(t, "backlog", dString.Get())
	assert.Equal(t, []string{}, dSlice.Get())

	// invariant: Zero value pointer type should return correct zero value with `Get()` without panic
	var (
		zeroInt    *DynamicValue[int]
		zeroFloat  *DynamicValue[float64]
		zeroBool   *DynamicValue[bool]
		zeroDur    *DynamicValue[time.Duration]
		zeroString *DynamicValue[string]
		zeroSlice  *DynamicValue[[]string]
	)

	assert.Equal(t, 0, zeroInt.Get())
	assert.Equal(t, float64(0), zeroFloat.Get())
	assert.Equal(t, false, zeroBool.Get())
	assert.Equal(t, time.Duration(0), zeroDur.Get())
	assert.Equal(t, "", zeroString.Get())
	assert.Equal(t, []string(nil), zeroSlice.Get())
}

func TestDyanamicValue_Reset(t *testing.T) {
	foo := NewDynamicValue[int](8)
	bar := NewDynamicValue[float64](8.9)
	alice := NewDynamicValue[bool](true)
	dave := NewDynamicValue[time.Duration](3 * time.Second)
	status := NewDynamicValue[string]("hello")
	slice := NewDynamicValue[[]string]([]string{"a", "b", "c"})

	assert.Nil(t, foo.val)
	foo.SetValue(10)
	assert.Equal(t, 10, *foo.val)
	foo.Reset() // reset should only reset val. not default
	assert.Nil(t, foo.val)

	assert.Nil(t, bar.val)
	bar.SetValue(9.0)
	assert.Equal(t, 9.0, *bar.val)
	bar.Reset() // reset should only reset val. not default
	assert.Nil(t, bar.val)

	assert.Nil(t, alice.val)
	alice.SetValue(true)
	assert.Equal(t, true, *alice.val)
	alice.Reset() // reset should only reset val. not default
	assert.Nil(t, alice.val)

	assert.Nil(t, dave.val)
	dave.SetValue(5 * time.Second)
	assert.Equal(t, 5*time.Second, *dave.val)
	dave.Reset() // reset should only reset val. not default
	assert.Nil(t, dave.val)

	assert.Nil(t, status.val)
	status.SetValue("world")
	assert.Equal(t, "world", *status.val)
	status.Reset() // reset should only reset val. not default
	assert.Nil(t, status.val)

	assert.Nil(t, slice.val)
	slice.SetValue([]string{"a", "b"})
	assert.Equal(t, []string{"a", "b"}, *slice.val)
	slice.Reset() // reset should only reset val. not default
	assert.Nil(t, slice.val)
}

func Test_String(t *testing.T) {
	s := time.Second * 2

	zeroInt := NewDynamicValue(89)
	zeroFloat := NewDynamicValue(7.8)
	zeroBool := NewDynamicValue(true)
	zeroDur := NewDynamicValue(s)
	zeroString := NewDynamicValue("weaviate")

	assert.Equal(t, "89", fmt.Sprintf("%v", zeroInt))
	assert.Equal(t, "7.8", fmt.Sprintf("%v", zeroFloat))
	assert.Equal(t, "true", fmt.Sprintf("%v", zeroBool))
	assert.Equal(t, s.String(), fmt.Sprintf("%v", zeroDur))
	assert.Equal(t, "weaviate", fmt.Sprintf("%v", zeroString))
}
