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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDynamicValue_Unmarshal(t *testing.T) {
	// assert if all supported types unmarshal correctly
	val := struct {
		Foo    int           `yaml:"foo"`
		Bar    float64       `yaml:"bar"`
		Alice  bool          `yaml:"alice"`
		Dave   time.Duration `yaml:"dave"`
		Status string        `yaml:"status"`
	}{}
	buf := `
foo: 2
bar: 4.5
alice: true
dave: 20s
status: "done"
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
}

func TestDynamicValue(t *testing.T) {
	var (
		dInt      DynamicValue[int]
		dFloat    DynamicValue[float64]
		dBool     DynamicValue[bool]
		dDuration DynamicValue[time.Duration]
	)

	// invariant: Zero value of any `DynamicValue` is usable and should return correct
	// underlying `zero-value` as default.
	assert.Equal(t, int(0), dInt.Get())
	assert.Equal(t, float64(0), dFloat.Get())
	assert.Equal(t, false, dBool.Get())
	assert.Equal(t, time.Duration(0), dDuration.Get())

	// invariant: After setting custom default via `SetDefault`, this custom default should
	// override the `zero-value`.
	dInt.SetDefault(20)
	dFloat.SetDefault(12.5)
	dBool.SetDefault(true)
	dDuration.SetDefault(2 * time.Second)

	assert.Equal(t, int(20), dInt.Get())
	assert.Equal(t, float64(12.5), dFloat.Get())
	assert.Equal(t, true, dBool.Get())
	assert.Equal(t, time.Duration(2*time.Second), dDuration.Get())

	// invariant: `NewDynamicValue` constructor should set custom default and should override
	// the `zero-value`
	dInt2 := NewDynamicValue(25)
	dFloat2 := NewDynamicValue(18.6)
	dBool2 := NewDynamicValue(true)
	dDuration2 := NewDynamicValue(4 * time.Second)

	assert.Equal(t, int(25), dInt2.Get())
	assert.Equal(t, float64(18.6), dFloat2.Get())
	assert.Equal(t, true, dBool2.Get())
	assert.Equal(t, time.Duration(4*time.Second), dDuration2.Get())

	// invariant: After setting dynamic default via `SetValue`, this value should
	// override both `zero-value` and `custom-default`.

	dInt.SetValue(30)
	dFloat.SetValue(22.7)
	dBool.SetValue(false)
	dDuration.SetValue(10 * time.Second)

	assert.Equal(t, int(30), dInt.Get())
	assert.Equal(t, float64(22.7), dFloat.Get())
	assert.Equal(t, false, dBool.Get())
	assert.Equal(t, time.Duration(10*time.Second), dDuration.Get())
}
