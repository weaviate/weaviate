package errorcompounder

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestErrorCompounder(t *testing.T) {
	err1 := errors.New("111")
	err2 := errors.New("222")
	err3 := errors.New("333")
	err4 := errors.New("444")
	err5 := errors.New("555")

	t.Run("compound errors", func(t *testing.T) {
		run := func(t *testing.T, ec ErrorCompounder) {
			t.Helper()

			ec.Add(err1)
			ec.Add(err2)
			ec.Add(err3)
			ec.Add(err4)
			ec.Add(err5)

			err := ec.ToError()
			assert.ErrorContains(t, err, "111, 222, 333, 444, 555")
		}

		run(t, New())
		run(t, NewSafe())
	})

	t.Run("compound wraps", func(t *testing.T) {
		run := func(t *testing.T, ec ErrorCompounder) {
			t.Helper()

			ec.AddWrapf(err1, "format%d", 1)
			ec.AddWrapf(err2, "format%d", 2)
			ec.AddWrapf(err3, "format%d", 3)

			err := ec.ToError()
			assert.ErrorContains(t, err, "format1: 111, format2: 222, format3: 333")
		}

		run(t, New())
		run(t, NewSafe())
	})

	t.Run("compound formats", func(t *testing.T) {
		run := func(t *testing.T, ec ErrorCompounder) {
			t.Helper()

			ec.Addf("format%d", 1)
			ec.Addf("format%d", 2)
			ec.Addf("format%d", 3)

			err := ec.ToError()
			assert.ErrorContains(t, err, "format1, format2, format3")
		}

		run(t, New())
		run(t, NewSafe())
	})

	t.Run("len / empty", func(t *testing.T) {
		run := func(t *testing.T, ec ErrorCompounder) {
			t.Helper()

			assert.True(t, ec.Empty())
			assert.Equal(t, 0, ec.Len())

			ec.Add(nil)
			ec.AddWrapf(nil, "format%d", 1)

			assert.True(t, ec.Empty())
			assert.Equal(t, 0, ec.Len())

			ec.Addf("format%d", 2)
			ec.Add(nil)
			ec.Add(err3)
			ec.AddWrapf(nil, "format%d", 4)
			ec.AddWrapf(err5, "format%d", 5)

			assert.False(t, ec.Empty())
			assert.Equal(t, 3, ec.Len())

			err := ec.ToError()
			assert.ErrorContains(t, err, "format2, 333, format5: 555")
		}

		run(t, New())
		run(t, NewSafe())
	})

	t.Run("groups levels", func(t *testing.T) {
		run := func(t *testing.T, create func() ErrorCompounder) {
			t.Helper()

			ec1 := create()
			ec1.AddGroups(err1)
			err := ec1.ToError()
			assert.ErrorContains(t, err, "111")
			ec1.AddGroups(err5)
			err = ec1.ToError()
			assert.ErrorContains(t, err, "111, 555")

			ec2 := create()
			ec2.AddGroups(err1, "lvl1")
			err = ec2.ToError()
			assert.ErrorContains(t, err, "\"lvl1\": {111}")
			ec2.AddGroups(err2, "lvl1")
			err = ec2.ToError()
			assert.ErrorContains(t, err, "\"lvl1\": {111, 222}")
			ec2.AddGroups(err3, "lvl1")
			err = ec2.ToError()
			assert.ErrorContains(t, err, "\"lvl1\": {111, 222, 333}")
			ec2.AddGroups(err5)
			err = ec2.ToError()
			assert.ErrorContains(t, err, "555, \"lvl1\": {111, 222, 333}")

			ec3 := create()
			ec3.AddGroups(err1, "lvl1", "lvl2")
			err = ec3.ToError()
			assert.ErrorContains(t, err, "\"lvl1\": {\"lvl2\": {111}}")
			ec3.AddGroups(err2, "lvl1", "lvl2")
			err = ec3.ToError()
			assert.ErrorContains(t, err, "\"lvl1\": {\"lvl2\": {111, 222}}")
			ec3.AddGroups(err3, "lvl1", "lvl2")
			err = ec3.ToError()
			assert.ErrorContains(t, err, "\"lvl1\": {\"lvl2\": {111, 222, 333}}")
			ec3.AddGroups(err5)
			err = ec3.ToError()
			assert.ErrorContains(t, err, "555, \"lvl1\": {\"lvl2\": {111, 222, 333}}")

			ec4 := create()
			ec4.AddGroups(err1, "lvl1", "lvl2", "lvl3")
			err = ec4.ToError()
			assert.ErrorContains(t, err, "\"lvl1\": {\"lvl2\": {\"lvl3\": {111}}}")
			ec4.AddGroups(err2, "lvl1", "lvl2", "lvl3")
			err = ec4.ToError()
			assert.ErrorContains(t, err, "\"lvl1\": {\"lvl2\": {\"lvl3\": {111, 222}}}")
			ec4.AddGroups(err3, "lvl1", "lvl2", "lvl3")
			err = ec4.ToError()
			assert.ErrorContains(t, err, "\"lvl1\": {\"lvl2\": {\"lvl3\": {111, 222, 333}}}")
			ec4.AddGroups(err5)
			err = ec4.ToError()
			assert.ErrorContains(t, err, "555, \"lvl1\": {\"lvl2\": {\"lvl3\": {111, 222, 333}}}")

			ec5 := create()
			ec5.AddGroups(err1)
			ec5.AddGroups(err2, "lvl1")
			ec5.AddGroups(err3, "lvl1", "lvl2")
			ec5.AddGroups(err4, "lvl1", "lvl2", "lvl3")
			ec5.AddGroups(err5, "lvl1", "lvl2", "lvl3", "lvl4")
			err = ec5.ToError()
			assert.ErrorContains(t, err, "111, \"lvl1\": {222, \"lvl2\": {333, \"lvl3\": {444, \"lvl4\": {555}}}}")

			ec6 := create()
			ec6.AddGroups(nil)
			ec6.AddGroups(nil, "lvl0")
			ec6.AddGroups(nil, "lvl0", "lvl0")
			ec6.AddGroups(nil, "lvl0", "lvl0", "lvl0")
			err = ec6.ToError()
			assert.NoError(t, err)
		}

		run(t, func() ErrorCompounder { return New() })
		run(t, func() ErrorCompounder { return NewSafe() })
	})

	t.Run("groups len / empty", func(t *testing.T) {
		run := func(t *testing.T, ec ErrorCompounder) {
			t.Helper()

			ec.AddGroups(nil)
			ec.AddGroups(nil, "lvl0")
			ec.AddGroups(nil, "lvl0", "lvl0")
			ec.AddGroups(nil, "lvl0", "lvl0", "lvl0")
			assert.True(t, ec.Empty())
			assert.Equal(t, 0, ec.Len())

			ec.AddGroups(err5, "lvl1", "lvl2", "lvl3", "lvl4")
			assert.False(t, ec.Empty())
			assert.Equal(t, 1, ec.Len())

			ec.AddGroups(err4, "lvl1", "lvl2", "lvl3")
			assert.False(t, ec.Empty())
			assert.Equal(t, 2, ec.Len())

			ec.AddGroups(err3, "lvl1", "lvl2")
			assert.False(t, ec.Empty())
			assert.Equal(t, 3, ec.Len())

			ec.AddGroups(err2, "lvl1")
			assert.False(t, ec.Empty())
			assert.Equal(t, 4, ec.Len())

			ec.AddGroups(err1)
			assert.False(t, ec.Empty())
			assert.Equal(t, 5, ec.Len())

			ec.AddGroups(nil)
			ec.AddGroups(nil, "lvl0")
			ec.AddGroups(nil, "lvl0", "lvl0")
			ec.AddGroups(nil, "lvl0", "lvl0", "lvl0")
			assert.False(t, ec.Empty())
			assert.Equal(t, 5, ec.Len())

			ec.AddGroups(err5, "lvl1", "lvl2", "lvl3", "lvl4")
			assert.False(t, ec.Empty())
			assert.Equal(t, 6, ec.Len())

			ec.AddGroups(err4, "lvl1", "lvl2", "lvl3")
			assert.False(t, ec.Empty())
			assert.Equal(t, 7, ec.Len())

			ec.AddGroups(err3, "lvl1", "lvl2")
			assert.False(t, ec.Empty())
			assert.Equal(t, 8, ec.Len())

			ec.AddGroups(err2, "lvl1")
			assert.False(t, ec.Empty())
			assert.Equal(t, 9, ec.Len())

			ec.AddGroups(err1)
			assert.False(t, ec.Empty())
			assert.Equal(t, 10, ec.Len())
		}

		run(t, New())
		run(t, NewSafe())
	})
}
