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
			assert.ErrorContains(t, ec1.ToError(), "111")
			ec1.AddGroups(err5)
			assert.ErrorContains(t, ec1.ToError(), "111, 555")

			ec2 := create()
			ec2.AddGroups(err1, "lvl1")
			assert.ErrorContains(t, ec2.ToError(), "\"lvl1\": {111}")
			ec2.AddGroups(err2, "lvl1")
			assert.ErrorContains(t, ec2.ToError(), "\"lvl1\": {111, 222}")
			ec2.AddGroups(err3, "lvl1")
			assert.ErrorContains(t, ec2.ToError(), "\"lvl1\": {111, 222, 333}")
			ec2.AddGroups(err5)
			assert.ErrorContains(t, ec2.ToError(), "555, \"lvl1\": {111, 222, 333}")

			ec3 := create()
			ec3.AddGroups(err1, "lvl1", "lvl2")
			assert.ErrorContains(t, ec3.ToError(), "\"lvl1\": {\"lvl2\": {111}}")
			ec3.AddGroups(err2, "lvl1", "lvl2")
			assert.ErrorContains(t, ec3.ToError(), "\"lvl1\": {\"lvl2\": {111, 222}}")
			ec3.AddGroups(err3, "lvl1", "lvl2")
			assert.ErrorContains(t, ec3.ToError(), "\"lvl1\": {\"lvl2\": {111, 222, 333}}")
			ec3.AddGroups(err5)
			assert.ErrorContains(t, ec3.ToError(), "555, \"lvl1\": {\"lvl2\": {111, 222, 333}}")

			ec4 := create()
			ec4.AddGroups(err1, "lvl1", "lvl2", "lvl3")
			assert.ErrorContains(t, ec4.ToError(), "\"lvl1\": {\"lvl2\": {\"lvl3\": {111}}}")
			ec4.AddGroups(err2, "lvl1", "lvl2", "lvl3")
			assert.ErrorContains(t, ec4.ToError(), "\"lvl1\": {\"lvl2\": {\"lvl3\": {111, 222}}}")
			ec4.AddGroups(err3, "lvl1", "lvl2", "lvl3")
			assert.ErrorContains(t, ec4.ToError(), "\"lvl1\": {\"lvl2\": {\"lvl3\": {111, 222, 333}}}")
			ec4.AddGroups(err5)
			assert.ErrorContains(t, ec4.ToError(), "555, \"lvl1\": {\"lvl2\": {\"lvl3\": {111, 222, 333}}}")

			ec5 := create()
			ec5.AddGroups(err1)
			ec5.AddGroups(err2, "lvl1")
			ec5.AddGroups(err3, "lvl1", "lvl2")
			ec5.AddGroups(err4, "lvl1", "lvl2", "lvl3")
			ec5.AddGroups(err5, "lvl1", "lvl2", "lvl3", "lvl4")
			assert.ErrorContains(t, ec5.ToError(), "111, \"lvl1\": {222, \"lvl2\": {333, \"lvl3\": {444, \"lvl4\": {555}}}}")

			ec6 := create()
			ec6.AddGroups(nil)
			ec6.AddGroups(nil, "lvl0")
			ec6.AddGroups(nil, "lvl0", "lvl0")
			ec6.AddGroups(nil, "lvl0", "lvl0", "lvl0")
			assert.NoError(t, ec6.ToError())
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

	t.Run("limited", func(t *testing.T) {
		run := func(t *testing.T, ec ErrorCompounder) {
			t.Helper()

			ec.Add(nil)
			assert.NoError(t, ec.ToErrorLimited(3))

			ec.Add(err1)
			ec.Add(err2)
			assert.ErrorContains(t, ec.ToError(), "111, 222")
			assert.ErrorContains(t, ec.ToErrorLimited(3), "111, 222")

			ec.Add(err3)
			ec.Add(err4)
			ec.Add(err5)
			assert.ErrorContains(t, ec.ToError(), "111, 222, 333, 444, 555")
			assert.ErrorContains(t, ec.ToErrorLimited(3), "111, 222, 333")
		}

		run(t, New())
		run(t, NewSafe())
	})

	t.Run("groups limited", func(t *testing.T) {
		run := func(t *testing.T, create func() ErrorCompounder) {
			t.Helper()

			ec1 := create()
			ec1.AddGroups(err1)
			ec1.AddGroups(err3)
			ec1.AddGroups(err5)
			assert.ErrorContains(t, ec1.ToError(), "111, 333, 555")
			assert.ErrorContains(t, ec1.ToErrorLimited(2), "111, 333")

			ec2 := create()
			ec2.AddGroups(err1, "lvl1")
			assert.ErrorContains(t, ec2.ToError(), "\"lvl1\": {111}")
			assert.ErrorContains(t, ec2.ToErrorLimited(2), "\"lvl1\": {111}")
			ec2.AddGroups(err2, "lvl1")
			assert.ErrorContains(t, ec2.ToError(), "\"lvl1\": {111, 222}")
			assert.ErrorContains(t, ec2.ToErrorLimited(2), "\"lvl1\": {111, 222}")
			ec2.AddGroups(err3, "lvl1")
			assert.ErrorContains(t, ec2.ToError(), "\"lvl1\": {111, 222, 333}")
			assert.ErrorContains(t, ec2.ToErrorLimited(2), "\"lvl1\": {111, 222}")
			ec2.AddGroups(err5)
			assert.ErrorContains(t, ec2.ToError(), "555, \"lvl1\": {111, 222, 333}")
			assert.ErrorContains(t, ec2.ToErrorLimited(2), "555, \"lvl1\": {111}")

			ec3 := create()
			ec3.AddGroups(err1, "lvl1", "lvl2")
			assert.ErrorContains(t, ec3.ToError(), "\"lvl1\": {\"lvl2\": {111}}")
			assert.ErrorContains(t, ec3.ToErrorLimited(2), "\"lvl1\": {\"lvl2\": {111}}")
			ec3.AddGroups(err2, "lvl1", "lvl2")
			assert.ErrorContains(t, ec3.ToError(), "\"lvl1\": {\"lvl2\": {111, 222}}")
			assert.ErrorContains(t, ec3.ToErrorLimited(2), "\"lvl1\": {\"lvl2\": {111, 222}}")
			ec3.AddGroups(err3, "lvl1", "lvl2")
			assert.ErrorContains(t, ec3.ToError(), "\"lvl1\": {\"lvl2\": {111, 222, 333}}")
			assert.ErrorContains(t, ec3.ToErrorLimited(2), "\"lvl1\": {\"lvl2\": {111, 222}}")
			ec3.AddGroups(err5)
			assert.ErrorContains(t, ec3.ToError(), "555, \"lvl1\": {\"lvl2\": {111, 222, 333}}")
			assert.ErrorContains(t, ec3.ToErrorLimited(2), "555, \"lvl1\": {\"lvl2\": {111}}")

			ec4 := create()
			ec4.AddGroups(err1, "lvl1", "lvl2", "lvl3")
			assert.ErrorContains(t, ec4.ToError(), "\"lvl1\": {\"lvl2\": {\"lvl3\": {111}}}")
			assert.ErrorContains(t, ec4.ToErrorLimited(2), "\"lvl1\": {\"lvl2\": {\"lvl3\": {111}}}")
			ec4.AddGroups(err2, "lvl1", "lvl2", "lvl3")
			assert.ErrorContains(t, ec4.ToError(), "\"lvl1\": {\"lvl2\": {\"lvl3\": {111, 222}}}")
			assert.ErrorContains(t, ec4.ToErrorLimited(2), "\"lvl1\": {\"lvl2\": {\"lvl3\": {111, 222}}}")
			ec4.AddGroups(err3, "lvl1", "lvl2", "lvl3")
			assert.ErrorContains(t, ec4.ToError(), "\"lvl1\": {\"lvl2\": {\"lvl3\": {111, 222, 333}}}")
			assert.ErrorContains(t, ec4.ToErrorLimited(2), "\"lvl1\": {\"lvl2\": {\"lvl3\": {111, 222}}}")
			ec4.AddGroups(err5)
			assert.ErrorContains(t, ec4.ToError(), "555, \"lvl1\": {\"lvl2\": {\"lvl3\": {111, 222, 333}}}")
			assert.ErrorContains(t, ec4.ToErrorLimited(2), "555, \"lvl1\": {\"lvl2\": {\"lvl3\": {111}}}")

			ec5 := create()
			ec5.AddGroups(err1)
			ec5.AddGroups(err2, "lvl1")
			ec5.AddGroups(err3, "lvl1", "lvl2")
			ec5.AddGroups(err4, "lvl1", "lvl2", "lvl3")
			ec5.AddGroups(err5, "lvl1", "lvl2", "lvl3", "lvl4")
			assert.ErrorContains(t, ec5.ToError(), "111, \"lvl1\": {222, \"lvl2\": {333, \"lvl3\": {444, \"lvl4\": {555}}}}")
			assert.ErrorContains(t, ec5.ToErrorLimited(3), "111, \"lvl1\": {222, \"lvl2\": {333}}")

			ec6 := create()
			ec6.AddGroups(nil)
			ec6.AddGroups(nil, "lvl0")
			ec6.AddGroups(nil, "lvl0", "lvl0")
			ec6.AddGroups(nil, "lvl0", "lvl0", "lvl0")
			assert.NoError(t, ec6.ToError())
			assert.NoError(t, ec6.ToErrorLimited(2))
		}

		run(t, func() ErrorCompounder { return New() })
		run(t, func() ErrorCompounder { return NewSafe() })
	})

	t.Run("first", func(t *testing.T) {
		run := func(t *testing.T, ec ErrorCompounder) {
			t.Helper()

			ec.Add(nil)
			assert.NoError(t, ec.First())

			ec.Add(err1)
			assert.ErrorIs(t, ec.First(), err1)

			ec.Add(err2)
			ec.Add(err3)
			assert.ErrorIs(t, ec.First(), err1)
		}

		run(t, New())
		run(t, NewSafe())
	})

	t.Run("groups first (ignored)", func(t *testing.T) {
		run := func(t *testing.T, create func() ErrorCompounder) {
			t.Helper()

			ec1 := create()
			ec1.AddGroups(err1)
			ec1.AddGroups(err2)
			assert.ErrorIs(t, ec1.First(), err1)

			ec2 := create()
			ec2.AddGroups(err2, "lvl1")
			ec2.AddGroups(err3, "lvl1")
			assert.ErrorIs(t, ec2.First(), err2)
			ec2.AddGroups(err5)
			assert.ErrorIs(t, ec2.First(), err5)

			ec3 := create()
			ec3.AddGroups(err2, "lvl1", "lvl2", "lvl3")
			assert.ErrorIs(t, ec3.First(), err2)
			ec3.AddGroups(err3, "lvl1", "lvl2")
			assert.ErrorIs(t, ec3.First(), err3)
			ec3.AddGroups(err4, "lvl1")
			assert.ErrorIs(t, ec3.First(), err4)
			ec3.AddGroups(err5)
			assert.ErrorIs(t, ec3.First(), err5)

			ec4 := create()
			ec4.AddGroups(err1)
			assert.ErrorIs(t, ec4.First(), err1)
			ec4.AddGroups(err2, "lvl1")
			assert.ErrorIs(t, ec4.First(), err1)
			ec4.AddGroups(err3, "lvl1", "lvl2")
			assert.ErrorIs(t, ec4.First(), err1)
			ec4.AddGroups(err4, "lvl1", "lvl2", "lvl3")
			assert.ErrorIs(t, ec4.First(), err1)
			ec4.AddGroups(err5, "lvl1", "lvl2", "lvl3", "lvl4")
			assert.ErrorIs(t, ec4.First(), err1)
			assert.ErrorContains(t, ec4.ToError(), "111, \"lvl1\": {222, \"lvl2\": {333, \"lvl3\": {444, \"lvl4\": {555}}}}")

			ec6 := create()
			ec6.AddGroups(nil)
			ec6.AddGroups(nil, "lvl0")
			ec6.AddGroups(nil, "lvl0", "lvl0")
			ec6.AddGroups(nil, "lvl0", "lvl0", "lvl0")
			assert.NoError(t, ec6.First())
		}

		run(t, func() ErrorCompounder { return New() })
		run(t, func() ErrorCompounder { return NewSafe() })
	})
}
