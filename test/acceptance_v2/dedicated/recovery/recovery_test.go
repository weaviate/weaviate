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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/acceptance_v2/dedicated"
	"github.com/weaviate/weaviate/test/docker"
	"golang.org/x/sync/errgroup"
)

// TODO-RAFT current tests doesn't force containers to change their IPs
// we need to add test were the actual container ip changes on stop if possible with testcontainer
// if not we need to terminate the whole container to pick up new IP and copy the old container filesystem
// to the new one to force recovery
func TestRaftRecoveryRestarts(t *testing.T) {
	t.Parallel()

	compose := dedicated.SetupDedicated(t, func(t *testing.T) *docker.Compose {
		return docker.New().With3NodeCluster()
	})
	ctx := t.Context()
	container1Ip := compose.ContainerURI(0)
	container2Ip := compose.ContainerURI(1)
	container3Ip := compose.ContainerURI(2)

	<-time.After(3 * time.Second) // wait for memberlist

	eg := errgroup.Group{}
	for idx := 0; idx < 3; idx++ {
		require.Nil(t, compose.StopAt(ctx, idx, nil))
		i := idx // catch idx for eg
		if i > 1 {
			time.Sleep(2 * time.Second)
		}
		eg.Go(func() error {
			require.Nil(t, compose.StartAt(ctx, i))
			return nil
		})
	}

	eg.Wait()
	// ips shouldn't be equal
	require.NotEqual(t, container1Ip, compose.ContainerURI(0))
	require.NotEqual(t, container2Ip, compose.ContainerURI(1))
	require.NotEqual(t, container3Ip, compose.ContainerURI(2))
}
