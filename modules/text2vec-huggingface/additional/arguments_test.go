//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package additional

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
)

func TestFeatureProjectionField(t *testing.T) {
	t.Run("should generate featureProjection argument properly", func(t *testing.T) {
		// given
		classname := "Class"

		// when
		featureProjection := additionalFeatureProjectionField(classname)

		// then
		// the built graphQL field needs to support this structure:
		// Args: {
		//   algorithm: "a",
		//   dimensions: 1,
		//   learningRate: 2,
		//   iterations: 3,
		//   perplexity: 4
		// }
		// Type: {
		//   vector: [0, 1]
		// }
		assert.NotNil(t, featureProjection)
		assert.Equal(t, "ClassAdditionalFeatureProjection", featureProjection.Type.Name())
		assert.NotNil(t, featureProjection.Args)
		assert.Equal(t, 5, len(featureProjection.Args))
		assert.NotNil(t, featureProjection.Args["algorithm"])
		assert.NotNil(t, featureProjection.Args["dimensions"])
		assert.NotNil(t, featureProjection.Args["learningRate"])
		assert.NotNil(t, featureProjection.Args["iterations"])
		assert.NotNil(t, featureProjection.Args["perplexity"])
		featureProjectionObject, featureProjectionObjectOK := featureProjection.Type.(*graphql.Object)
		assert.True(t, featureProjectionObjectOK)
		assert.Equal(t, 1, len(featureProjectionObject.Fields()))
		assert.NotNil(t, featureProjectionObject.Fields()["vector"])
	})
}
