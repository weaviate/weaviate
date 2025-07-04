//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFreeTcpPort(t *testing.T) {
	port := MustGetFreeTCPPort()
	assert.Greater(t, port, 0)
}
