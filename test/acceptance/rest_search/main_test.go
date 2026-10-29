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

package rest_search

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
)

var (
	restSearchOnce    sync.Once
	restSearchCompose *docker.DockerCompose
	restSearchErr     error
)

// restSearchServerURI returns the package's Weaviate, starting it on first
// use. The tests share it and use distinct class names.
func restSearchServerURI(t *testing.T) string {
	t.Helper()
	restSearchOnce.Do(func() {
		restSearchCompose, restSearchErr = docker.New().
			WithWeaviate().
			WithText2VecModel2Vec().
			Start(context.Background())
	})
	require.NoError(t, restSearchErr)
	return restSearchCompose.GetWeaviate().URI()
}

func TestMain(m *testing.M) {
	code := m.Run()
	if restSearchCompose != nil {
		if err := restSearchCompose.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "failed to terminate rest search server: %v\n", err)
		}
	}
	os.Exit(code)
}
