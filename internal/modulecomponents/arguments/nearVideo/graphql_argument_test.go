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

package nearVideo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func TestNearVideoGraphQLArgument(t *testing.T) {
	t.Run("should generate nearVideo argument properly", func(t *testing.T) {
		// given
		prefix := "Prefix"
		classname := "Class"
		// when
		nearVideo := nearVideoArgument(prefix, classname)

		// then
		// the built graphQL field needs to support this structure:
		// nearVideo: {
		//   video: "base64;encoded,video_file",
		//   distance: 0.9
		//   targetVectors: ["targetVector"]
		// }
		assert.NotNil(t, nearVideo)
		assert.Equal(t, "Multi2VecBindPrefixClassNearVideoInpObj", nearVideo.Type.Name())
		answerFields, ok := nearVideo.Type.(*graphql.InputObject)
		assert.True(t, ok)
		assert.NotNil(t, answerFields)
		assert.Equal(t, 4, len(answerFields.Fields()))
		fields := answerFields.Fields()
		video := fields["video"]
		videoNonNull, videoNonNullOK := video.Type.(*graphql.NonNull)
		assert.True(t, videoNonNullOK)
		assert.Equal(t, "String", videoNonNull.OfType.Name())
		assert.NotNil(t, video)
		assert.NotNil(t, fields["certainty"])
		assert.NotNil(t, fields["distance"])
		targetVectors := fields["targetVectors"]
		targetVectorsList, targetVectorsListOK := targetVectors.Type.(*graphql.List)
		assert.True(t, targetVectorsListOK)
		assert.Equal(t, "String", targetVectorsList.OfType.Name())
		assert.NotNil(t, targetVectors)
	})
}
