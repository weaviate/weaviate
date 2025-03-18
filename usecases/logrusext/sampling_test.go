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

package logrusext

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestSampler_Simple(t *testing.T) {
	logger, hook := test.NewNullLogger()
	sampler := NewSampler(logger, 3, 100*time.Millisecond)

	for range 10 {
		sampler.WithSampling(func(l logrus.FieldLogger) {
			l.Infof("hello")
		})
	}

	require.Len(t, hook.AllEntries(), 3)
	hook.Reset()

	time.Sleep(200 * time.Millisecond)

	for range 5 {
		sampler.WithSampling(func(l logrus.FieldLogger) {
			l.Infof("hello")
		})
	}
	require.Len(t, hook.AllEntries(), 3)
}
