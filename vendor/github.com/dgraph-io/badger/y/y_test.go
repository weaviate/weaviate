/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloser(t *testing.T) {
	c := NewCloser()

	// Create one LevelCloser, with three registrations.
	lc1 := c.Register("foo")
	{
		lc2 := c.Register("foo")
		lc3 := c.Register("foo")

		require.Equal(t, lc1, lc2)
		require.Equal(t, lc1, lc3)
	}

	// Make two wait for the signal, but just to mix things up, call Done() once.
	doneChan := make(chan int)
	for i := 0; i < 2; i++ {
		go func() {
			<-lc1.HasBeenClosed()
			doneChan <- 1
		}()
	}

	lc1.Done()

	// Check that nothing has finished yet.
	select {
	case <-doneChan:
		require.Fail(t, "premature finish")
	default:
	}

	// Signal and wait for both to finish.
	c.SignalAll()
	for i := 0; i < 2; i++ {
		<-doneChan
	}

	select {
	case <-lc1.HasBeenClosed():
	default:
		require.Fail(t, "HasBeenClosed should always supply values")
	}
}
