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

package properties

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

func checkFolderExistence(t *testing.T,
	compose *docker.DockerCompose, className, shardName, directory string,
) bool {
	weaviateContainer := compose.GetWeaviate().Container()
	path := fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(className), shardName)
	code, reader, err := weaviateContainer.Exec(context.TODO(), []string{"ls", "-1", path})
	require.NoError(t, err)
	require.Equal(t, 0, code)

	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	require.NoError(t, err)
	output := buf.String()

	for dir := range strings.SplitSeq(output, "\n") {
		if directory == dir {
			return true
		}
	}
	return false
}
