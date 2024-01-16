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

package nearImage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func TestNearImageGraphQLArgument(t *testing.T) {
	t.Run("should generate nearImage argument properly", func(t *testing.T) {
		// given
		prefix := "Prefix"
		classname := "Class"
		// when
		nearImage := nearImageArgument(prefix, classname)

		// then
		// the built graphQL field needs to support this structure:
		// nearImage: {
		//   image: "base64;encoded,image",
		//   distance: 0.9
		// }
		assert.NotNil(t, nearImage)
		assert.Equal(t, "Img2VecImagePrefixClassNearImageInpObj", nearImage.Type.Name())
		answerFields, ok := nearImage.Type.(*graphql.InputObject)
		assert.True(t, ok)
		assert.NotNil(t, answerFields)
		assert.Equal(t, 3, len(answerFields.Fields()))
		fields := answerFields.Fields()
		image := fields["image"]
		imageNonNull, imageNonNullOK := image.Type.(*graphql.NonNull)
		assert.True(t, imageNonNullOK)
		assert.Equal(t, "String", imageNonNull.OfType.Name())
		assert.NotNil(t, image)
		assert.NotNil(t, fields["certainty"])
		assert.NotNil(t, fields["distance"])
	})
}
