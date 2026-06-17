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

func TestRPCResolverIPv6(t *testing.T) {
	rpcPort := 8081

	t.Run("non-local cluster with IPv6 raft address", func(t *testing.T) {
		rAddr, err := NewRpc(false, rpcPort).Address("[2001:db8::1]:8300")
		assert.NoError(t, err)
		assert.Equal(t, "[2001:db8::1]:8081", rAddr)
	})

	t.Run("local cluster with IPv6 raft address", func(t *testing.T) {
		rAddr, err := NewRpc(true, rpcPort).Address("[::1]:8300")
		assert.NoError(t, err)
		assert.Equal(t, "[::1]:8301", rAddr)
	})

	t.Run("bare IPv6 without brackets is invalid", func(t *testing.T) {
		_, err := NewRpc(false, rpcPort).Address("2001:db8::1:8300")
		assert.Error(t, err)
	})
}
