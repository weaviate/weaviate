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

func TestDynamicValue_Unmarshal(t *testing.T) {
	t.Run("built-in types", func(t *testing.T) {
		// assert if all supported types unmarshal correctly
		val := struct {
			Foo    int           `yaml:"foo"`
			Bar    float64       `yaml:"bar"`
			Alice  bool          `yaml:"alice"`
			Dave   time.Duration `yaml:"dave"`
			Status string        `yaml:"status"`
			Slice  []string      `yaml:"slice"`
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
		assert.Equal(t, 2, val.Foo)
		assert.Equal(t, 4.5, val.Bar)
		assert.Equal(t, true, val.Alice)
		assert.Equal(t, 20*time.Second, val.Dave)
		assert.Equal(t, "done", val.Status)
		assert.Equal(t, []string{"one", "two", "three"}, val.Slice)
	})

	t.Run("derived types", func(t *testing.T) {
		// check for derived types as well
		type MyInt int
		type MyFloat float64
		type MyBool bool
		type MyString string

		buf := `
foo: 2
bar: 4.5
alice: true
status: "done"
`

		val := struct {
			Foo    MyInt    `yaml:"foo"`
			Bar    MyFloat  `yaml:"bar"`
			Alice  MyBool   `yaml:"alice"`
			Status MyString `yaml:"status"`
		}{}

		dec := yaml.NewDecoder(strings.NewReader(buf))
		dec.KnownFields(true)
		err := dec.Decode(&val)
		require.NoError(t, err)
		assert.Equal(t, MyInt(2), val.Foo)
		assert.Equal(t, MyFloat(4.5), val.Bar)
		assert.Equal(t, MyBool(true), val.Alice)
		assert.Equal(t, MyString("done"), val.Status)
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
