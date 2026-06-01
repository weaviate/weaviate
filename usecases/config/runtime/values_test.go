//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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

func TestDynamicValue_String(t *testing.T) {
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

func TestDynamicValue_Validation(t *testing.T) {
	intValid := 1
	intValid2 := 2
	intInvalid := -1
	intValidate := func(val int) error {
		if val < 0 {
			return fmt.Errorf("int < 0")
		}
		return nil
	}

	floatValid := 1.
	floatValid2 := 2.
	floatInvalid := -1.
	floatValidate := func(val float64) error {
		if val < 0 {
			return fmt.Errorf("float < 0")
		}
		return nil
	}

	boolValid := true
	boolValid2 := true
	boolInvalid := false
	boolValidate := func(val bool) error {
		if !val {
			return fmt.Errorf("bool = false")
		}
		return nil
	}

	durationValid := time.Second
	durationValid2 := 2 * time.Second
	durationInvalid := -time.Second
	durationValidate := func(val time.Duration) error {
		if val < 0 {
			return fmt.Errorf("duration < 0")
		}
		return nil
	}

	stringValid := "string"
	stringValid2 := "string2"
	stringInvalid := ""
	stringValidate := func(val string) error {
		if val == "" {
			return fmt.Errorf("string = \"\"")
		}
		return nil
	}

	sliceValid := []string{"string"}
	sliceValid2 := []string{"string1", "string2"}
	sliceInvalid := []string{}
	sliceValidate := func(val []string) error {
		if len(val) == 0 {
			return fmt.Errorf("slice = []")
		}
		return nil
	}

	t.Run("create valid", func(t *testing.T) {
		dvInt, err := NewDynamicValueWithValidation(intValid, intValidate)
		assert.NoError(t, err)
		assert.Equal(t, intValid, dvInt.Get())

		dvFloat, err := NewDynamicValueWithValidation(floatValid, floatValidate)
		assert.NoError(t, err)
		assert.Equal(t, floatValid, dvFloat.Get())

		dvBool, err := NewDynamicValueWithValidation(boolValid, boolValidate)
		assert.NoError(t, err)
		assert.Equal(t, boolValid, dvBool.Get())

		dvDuration, err := NewDynamicValueWithValidation(durationValid, durationValidate)
		assert.NoError(t, err)
		assert.Equal(t, durationValid, dvDuration.Get())

		dvString, err := NewDynamicValueWithValidation(stringValid, stringValidate)
		assert.NoError(t, err)
		assert.Equal(t, stringValid, dvString.Get())

		dvSlice, err := NewDynamicValueWithValidation(sliceValid, sliceValidate)
		assert.NoError(t, err)
		assert.Equal(t, sliceValid, dvSlice.Get())
	})

	t.Run("create invalid", func(t *testing.T) {
		dvInt, err := NewDynamicValueWithValidation(intInvalid, intValidate)
		assert.Error(t, err)
		assert.Nil(t, dvInt)

		dvFloat, err := NewDynamicValueWithValidation(floatInvalid, floatValidate)
		assert.Error(t, err)
		assert.Nil(t, dvFloat)

		dvBool, err := NewDynamicValueWithValidation(boolInvalid, boolValidate)
		assert.Error(t, err)
		assert.Nil(t, dvBool)

		dvDuration, err := NewDynamicValueWithValidation(durationInvalid, durationValidate)
		assert.Error(t, err)
		assert.Nil(t, dvDuration)

		dvString, err := NewDynamicValueWithValidation(stringInvalid, stringValidate)
		assert.Error(t, err)
		assert.Nil(t, dvString)

		dvSlice, err := NewDynamicValueWithValidation(sliceInvalid, sliceValidate)
		assert.Error(t, err)
		assert.Nil(t, dvSlice)
	})

	t.Run("setValue invalid", func(t *testing.T) {
		dvInt, _ := NewDynamicValueWithValidation(intValid, intValidate)
		assert.Error(t, dvInt.SetValue(intInvalid))
		assert.Equal(t, intValid, dvInt.Get())
		assert.NoError(t, dvInt.SetValue(intValid2))
		assert.Equal(t, intValid2, dvInt.Get())

		dvFloat, _ := NewDynamicValueWithValidation(floatValid, floatValidate)
		assert.Error(t, dvFloat.SetValue(floatInvalid))
		assert.Equal(t, floatValid, dvFloat.Get())
		assert.NoError(t, dvFloat.SetValue(floatValid2))
		assert.Equal(t, floatValid2, dvFloat.Get())

		dvBool, _ := NewDynamicValueWithValidation(boolValid, boolValidate)
		assert.Error(t, dvBool.SetValue(boolInvalid))
		assert.Equal(t, boolValid, dvBool.Get())
		assert.NoError(t, dvBool.SetValue(boolValid2))
		assert.Equal(t, boolValid2, dvBool.Get())

		dvDuration, _ := NewDynamicValueWithValidation(durationValid, durationValidate)
		assert.Error(t, dvDuration.SetValue(durationInvalid))
		assert.Equal(t, durationValid, dvDuration.Get())
		assert.NoError(t, dvDuration.SetValue(durationValid2))
		assert.Equal(t, durationValid2, dvDuration.Get())

		dvString, _ := NewDynamicValueWithValidation(stringValid, stringValidate)
		assert.Error(t, dvString.SetValue(stringInvalid))
		assert.Equal(t, stringValid, dvString.Get())
		assert.NoError(t, dvString.SetValue(stringValid2))
		assert.Equal(t, stringValid2, dvString.Get())

		dvSlice, _ := NewDynamicValueWithValidation(sliceValid, sliceValidate)
		assert.Error(t, dvSlice.SetValue(sliceInvalid))
		assert.Equal(t, sliceValid, dvSlice.Get())
		assert.NoError(t, dvSlice.SetValue(sliceValid2))
		assert.Equal(t, sliceValid2, dvSlice.Get())
	})
}
