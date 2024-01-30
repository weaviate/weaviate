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

package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

func TestRecovery(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		With3NodeCluster().
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)

	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	container1Ip := compose.ContainerURI(1)
	container2Ip := compose.ContainerURI(2)
	container3Ip := compose.ContainerURI(3)

	<-time.After(2 * time.Second) // wait for memberlist

	// restart cluster with different IPs
	require.Nil(t, compose.StopAt(ctx, 1, nil))
	require.Nil(t, compose.StopAt(ctx, 2, nil))
	require.Nil(t, compose.StopAt(ctx, 3, nil))

	require.Nil(t, compose.StartAt(ctx, 1))
	require.Nil(t, compose.StartAt(ctx, 2))
	require.Nil(t, compose.StartAt(ctx, 3))

	// ips shouldn't be equal
	require.NotEqual(t, container1Ip, compose.ContainerURI(1))
	require.NotEqual(t, container2Ip, compose.ContainerURI(2))
	require.NotEqual(t, container3Ip, compose.ContainerURI(3))
}
