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

package tokenizer

import (
	"testing"

	"github.com/go-ego/gse"
	"github.com/stretchr/testify/require"
)

func TestGseDictLoading(t *testing.T) {
	t.Run("Japanese Dict", func(t *testing.T) {
		_, err := gse.New("ja")
		require.NoError(t, err, "Failed to load Japanese dictionary")
	})

	t.Run("Chinese Dict", func(t *testing.T) {
		_, err := gse.New("zh")
		require.NoError(t, err, "Failed to load Chinese dictionary")
	})
}
