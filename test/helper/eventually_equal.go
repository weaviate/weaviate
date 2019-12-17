package testhelper

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type fakeT struct {
	lastError error
}

func (f *fakeT) Reset() {
	f.lastError = nil
}

func (f *fakeT) Errorf(msg string, args ...interface{}) {
	f.lastError = fmt.Errorf(msg, args...)
}

// AssertEventuallyEqual retries the 'actual' thunk every 10ms for a total of
// 300ms. If a single one succeeds, it returns, if all fails it eventually
// fails
func AssertEventuallyEqual(t *testing.T, expected interface{}, actualThunk func() interface{}, msg ...interface{}) {
	interval := 10 * time.Millisecond
	timeout := 4000 * time.Millisecond
	elapsed := 0 * time.Millisecond
	fakeT := &fakeT{}

	for elapsed < timeout {
		fakeT.Reset()
		actual := actualThunk()
		assert.Equal(fakeT, expected, actual, msg...)

		if fakeT.lastError == nil {
			return
		}

		time.Sleep(interval)
		elapsed += interval
	}

	t.Errorf("waiting for %s, but never succeeded:\n\n%s", elapsed, fakeT.lastError)
}
