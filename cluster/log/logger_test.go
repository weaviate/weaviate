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

package log

import (
	"bytes"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_hclogger(t *testing.T) {
	buf := bytes.Buffer{}

	r := logrus.New()
	r.SetOutput(&buf)

	v := NewHCLogrusLogger("test", r)

	v.Warn("Election time out")
	assert.Contains(t, buf.String(), "Election time out")
	buf.Reset()

	v.Warn("heartbeat timeout reached", "last-leader-addr", "fake", "last-leader-id", "fake")
	assert.NotContains(t, buf.String(), "Election time out")
	assert.Contains(t, buf.String(), "heartbeat timeout reached")
	assert.Contains(t, buf.String(), "last-leader-addr=fake")
	assert.Contains(t, buf.String(), "last-leader-id=fake")
	buf.Reset()

	v.Warn("Election time out")
	assert.Contains(t, buf.String(), "Election time out")
	assert.NotContains(t, buf.String(), "heartbeat timeout reached")
	assert.NotContains(t, buf.String(), "last-leader-addr=fake")
	assert.NotContains(t, buf.String(), "last-leader-id=fake")
	buf.Reset()

	// check if any fields added to it later should be available in future log lines
	v = v.With("oh-new", "oh-new-value")
	v.Warn("Election time out")
	assert.Contains(t, buf.String(), "Election time out")
	assert.Contains(t, buf.String(), "oh-new=oh-new-value")
	assert.NotContains(t, buf.String(), "heartbeat timeout reached")
	assert.NotContains(t, buf.String(), "last-leader-addr=fake")
	assert.NotContains(t, buf.String(), "last-leader-id=fake")
	buf.Reset()

	// ResetNamed API
	{
		v.Warn("Election time out")
		assert.Contains(t, buf.String(), "Election time out")
		assert.Contains(t, buf.String(), "action=test")
		buf.Reset()

		v = v.ResetNamed("test2") // test -> test2
		v.Warn("Election time out")
		assert.Contains(t, buf.String(), "Election time out")
		assert.Contains(t, buf.String(), "action=test2") // renamed successfully
		buf.Reset()
	}

	// After renaming, no duplicate fileds from previous log lines
	{
		v.Warn("heartbeat timeout reached", "last-leader-addr", "fake", "last-leader-id", "fake")
		assert.NotContains(t, buf.String(), "Election time out")
		assert.Contains(t, buf.String(), "heartbeat timeout reached")
		assert.Contains(t, buf.String(), "last-leader-addr=fake")
		assert.Contains(t, buf.String(), "last-leader-id=fake")
		buf.Reset()

		v.Warn("Election time out")
		assert.Contains(t, buf.String(), "Election time out")
		assert.NotContains(t, buf.String(), "heartbeat timeout reached")
		assert.NotContains(t, buf.String(), "last-leader-addr=fake")
		assert.NotContains(t, buf.String(), "last-leader-id=fake")
		buf.Reset()
	}

	// After renaming, logger should respect fields added via future `With()` api
	{
		v = v.With("oh-new", "oh-new-value")
		v.Warn("Election time out")
		assert.Contains(t, buf.String(), "Election time out")
		assert.Contains(t, buf.String(), "oh-new=oh-new-value")
		assert.NotContains(t, buf.String(), "heartbeat timeout reached")
		assert.NotContains(t, buf.String(), "last-leader-addr=fake")
		assert.NotContains(t, buf.String(), "last-leader-id=fake")
		buf.Reset()
	}
}
