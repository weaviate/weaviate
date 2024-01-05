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

package additional

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func TestNearestNeighborsField(t *testing.T) {
	t.Run("should generate nearestNeighbors argument properly", func(t *testing.T) {
		// given
		classname := "Class"

		// when
		nearestNeighbors := additionalNearestNeighborsField(classname)

		// then
		// the built graphQL field needs to support this structure:
		// Type: {
		//   neighbors: {
		//     concept: "c1",
		//     distance: 0.8
		//   }
		// }
		assert.NotNil(t, nearestNeighbors)
		assert.Equal(t, "ClassAdditionalNearestNeighbors", nearestNeighbors.Type.Name())
		assert.NotNil(t, nearestNeighbors.Type)
		nearestNeighborsObject, nearestNeighborsObjectOK := nearestNeighbors.Type.(*graphql.Object)
		assert.True(t, nearestNeighborsObjectOK)
		assert.Equal(t, 1, len(nearestNeighborsObject.Fields()))
		neighbors, neighborsOK := nearestNeighborsObject.Fields()["neighbors"]
		assert.True(t, neighborsOK)
		neighborsList, neighborsListOK := neighbors.Type.(*graphql.List)
		assert.True(t, neighborsListOK)
		neighborsListObjects, neighborsListObjectsOK := neighborsList.OfType.(*graphql.Object)
		assert.True(t, neighborsListObjectsOK)
		assert.Equal(t, 2, len(neighborsListObjects.Fields()))
		assert.NotNil(t, neighborsListObjects.Fields()["concept"])
		assert.NotNil(t, neighborsListObjects.Fields()["distance"])
	})
}

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

func TestSemanticPathField(t *testing.T) {
	t.Run("should generate semanticPath argument properly", func(t *testing.T) {
		// given
		classname := "Class"

		// when
		semanticPath := additionalSemanticPathField(classname)

		// then
		// the built graphQL field needs to support this structure:
		// Type: {
		//   path: [
		//     {
		//       concept: "c1",
		//       distanceToQuery: 0.1,
		//       distanceToResult: 0.2,
		//       distanceToNext: 0.3,
		//       distanceToPrevious: 0.4,
		//     }
		// }
		assert.NotNil(t, semanticPath)
		assert.Equal(t, "ClassAdditionalSemanticPath", semanticPath.Type.Name())
		semanticPathObject, semanticPathObjectOK := semanticPath.Type.(*graphql.Object)
		assert.True(t, semanticPathObjectOK)
		assert.Equal(t, 1, len(semanticPathObject.Fields()))
		assert.NotNil(t, semanticPathObject.Fields()["path"])
		semanticPathObjectList, semanticPathObjectListOK := semanticPathObject.Fields()["path"].Type.(*graphql.List)
		assert.True(t, semanticPathObjectListOK)
		semanticPathObjectListObjects, semanticPathObjectListOK := semanticPathObjectList.OfType.(*graphql.Object)
		assert.True(t, semanticPathObjectListOK)
		assert.Equal(t, 5, len(semanticPathObjectListObjects.Fields()))
		assert.NotNil(t, semanticPathObjectListObjects.Fields()["concept"])
		assert.NotNil(t, semanticPathObjectListObjects.Fields()["distanceToQuery"])
		assert.NotNil(t, semanticPathObjectListObjects.Fields()["distanceToResult"])
		assert.NotNil(t, semanticPathObjectListObjects.Fields()["distanceToNext"])
		assert.NotNil(t, semanticPathObjectListObjects.Fields()["distanceToPrevious"])
	})
}

func TestNearestInterpretationField(t *testing.T) {
	t.Run("should generate interpretation argument properly", func(t *testing.T) {
		// given
		classname := "Class"

		// when
		interpretation := additionalInterpretationField(classname)

		// then
		// the built graphQL field needs to support this structure:
		// Type: {
		//   source: [
		//     {
		//       concept: "c1",
		//       weight: 0.1,
		//       occurrence: 0.2,
		//     }
		// }
		assert.NotNil(t, interpretation)
		assert.Equal(t, "ClassAdditionalInterpretation", interpretation.Type.Name())
		interpretationObject, interpretationObjectOK := interpretation.Type.(*graphql.Object)
		assert.True(t, interpretationObjectOK)
		assert.Equal(t, 1, len(interpretationObject.Fields()))
		assert.NotNil(t, interpretationObject.Fields()["source"])
		interpretationObjectList, interpretationObjectListOK := interpretationObject.Fields()["source"].Type.(*graphql.List)
		assert.True(t, interpretationObjectListOK)
		interpretationObjectListObjects, interpretationObjectListObjectsOK := interpretationObjectList.OfType.(*graphql.Object)
		assert.True(t, interpretationObjectListObjectsOK)
		assert.Equal(t, 3, len(interpretationObjectListObjects.Fields()))
		assert.NotNil(t, interpretationObjectListObjects.Fields()["concept"])
		assert.NotNil(t, interpretationObjectListObjects.Fields()["weight"])
		assert.NotNil(t, interpretationObjectListObjects.Fields()["occurrence"])
	})
}
