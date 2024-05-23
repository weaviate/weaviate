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

package resolver

import (
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRPCResolver(t *testing.T) {
	rpcPort := 8081
	_, err := NewRpc(false, rpcPort).Address("localhost123")
	addErr := &net.AddrError{}
	assert.ErrorAs(t, err, &addErr)

	rAddr, err := NewRpc(false, rpcPort).Address("localhost:123")
	assert.Nil(t, err)
	assert.Equal(t, rAddr, fmt.Sprintf("localhost:%d", rpcPort))

	_, err = NewRpc(true, rpcPort).Address("localhost:not_a_port")
	numErr := &strconv.NumError{}
	assert.ErrorAs(t, err, &numErr)

	rAddr, err = NewRpc(true, rpcPort).Address("localhost:123")
	assert.Nil(t, err)
	assert.Equal(t, "localhost:124", rAddr)
}
