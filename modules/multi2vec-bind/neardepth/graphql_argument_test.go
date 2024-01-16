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

package neardepth

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func TestNearDepthGraphQLArgument(t *testing.T) {
	t.Run("should generate nearDepth argument properly", func(t *testing.T) {
		// given
		prefix := "Prefix"
		classname := "Class"
		// when
		nearDepth := nearDepthArgument(prefix, classname)

		// then
		// the built graphQL field needs to support this structure:
		// nearDepth: {
		//   depth: "base64;encoded,depth_depth",
		//   distance: 0.9
		// }
		assert.NotNil(t, nearDepth)
		assert.Equal(t, "Multi2VecBindPrefixClassNearDepthInpObj", nearDepth.Type.Name())
		answerFields, ok := nearDepth.Type.(*graphql.InputObject)
		assert.True(t, ok)
		assert.NotNil(t, answerFields)
		assert.Equal(t, 3, len(answerFields.Fields()))
		fields := answerFields.Fields()
		depth := fields["depth"]
		depthNonNull, depthNonNullOK := depth.Type.(*graphql.NonNull)
		assert.True(t, depthNonNullOK)
		assert.Equal(t, "String", depthNonNull.OfType.Name())
		assert.NotNil(t, depth)
		assert.NotNil(t, fields["certainty"])
		assert.NotNil(t, fields["distance"])
	})
}
