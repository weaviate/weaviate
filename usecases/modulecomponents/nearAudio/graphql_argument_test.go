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

package nearAudio

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func TestNearAudioGraphQLArgument(t *testing.T) {
	t.Run("should generate nearAudio argument properly", func(t *testing.T) {
		// given
		prefix := "Prefix"
		classname := "Class"
		// when
		nearAudio := nearAudioArgument(prefix, classname)

		// then
		// the built graphQL field needs to support this structure:
		// nearAudio: {
		//   audio: "base64;encoded,audio",
		//   distance: 0.9
		//   targetVectors: ["targetVector"]
		// }
		assert.NotNil(t, nearAudio)
		assert.Equal(t, "Multi2VecBindPrefixClassNearAudioInpObj", nearAudio.Type.Name())
		nearAudioFields, ok := nearAudio.Type.(*graphql.InputObject)
		assert.True(t, ok)
		assert.NotNil(t, nearAudioFields)
		assert.Equal(t, 4, len(nearAudioFields.Fields()))
		fields := nearAudioFields.Fields()
		audio := fields["audio"]
		audioNonNull, audioNonNullOK := audio.Type.(*graphql.NonNull)
		assert.True(t, audioNonNullOK)
		assert.Equal(t, "String", audioNonNull.OfType.Name())
		assert.NotNil(t, audio)
		assert.NotNil(t, fields["certainty"])
		assert.NotNil(t, fields["distance"])
		targetVectors := fields["targetVectors"]
		targetVectorsList, targetVectorsListOK := targetVectors.Type.(*graphql.List)
		assert.True(t, targetVectorsListOK)
		assert.Equal(t, "String", targetVectorsList.OfType.Name())
		assert.NotNil(t, targetVectors)
	})
}
