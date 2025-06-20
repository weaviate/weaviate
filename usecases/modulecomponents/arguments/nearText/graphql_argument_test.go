//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package nearText

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func TestNearTextGraphQLArgument(t *testing.T) {
	t.Run("should generate nearText argument properly", func(t *testing.T) {
		// given
		prefix := "Prefix"
		classname := "Class"
		// when
		nearText := New(nil).nearTextArgument(prefix, classname, false)

		// then
		// the built graphQL field needs to support this structure:
		// {
		//   concepts: ["c1", "c2"],
		//   distance: 0.9,
		//   moveTo: {
		//          concepts: ["c1", "c2"],
		//          objects: [
		//               { id: "some-uuid-value"}],
		//               { beacon: "some-beacon-value"}
		//          ],
		//          force: 0.8
		//   }
		//   moveAwayFrom: {
		//          concepts: ["c1", "c2"],
		//          objects: [
		//               { id: "some-uuid-value"}],
		//               { beacon: "some-beacon-value"}
		//          ],
		//          force: 0.8
		//   },
		//   targetVectors: ["targetVector"]
		// }
		assert.NotNil(t, nearText)
		assert.Equal(t, "PrefixClassNearTextInpObj", nearText.Type.Name())
		nearTextFields, ok := nearText.Type.(*graphql.InputObject)
		assert.True(t, ok)
		assert.NotNil(t, nearTextFields)
		assert.Equal(t, 6, len(nearTextFields.Fields()))
		fields := nearTextFields.Fields()
		concepts := fields["concepts"]
		conceptsNonNull, conceptsNonNullOK := concepts.Type.(*graphql.NonNull)
		assert.True(t, conceptsNonNullOK)
		conceptsNonNullList, conceptsNonNullListOK := conceptsNonNull.OfType.(*graphql.List)
		assert.True(t, conceptsNonNullListOK)
		assert.Equal(t, "String", conceptsNonNullList.OfType.Name())
		assert.NotNil(t, concepts)
		conceptsType, conceptsTypeOK := concepts.Type.(*graphql.NonNull)
		assert.True(t, conceptsTypeOK)
		assert.NotNil(t, conceptsType)
		assert.NotNil(t, fields["certainty"])
		assert.NotNil(t, fields["distance"])
		assert.NotNil(t, fields["moveTo"])
		moveTo, moveToOK := fields["moveTo"].Type.(*graphql.InputObject)
		assert.True(t, moveToOK)
		assert.Equal(t, 3, len(moveTo.Fields()))
		assert.NotNil(t, moveTo.Fields()["concepts"])
		moveToConcepts, moveToConceptsOK := moveTo.Fields()["concepts"].Type.(*graphql.List)
		assert.True(t, moveToConceptsOK)
		assert.Equal(t, "String", moveToConcepts.OfType.Name())
		assert.NotNil(t, moveToConcepts)
		assert.NotNil(t, moveTo.Fields()["objects"])
		moveToObjects, moveToObjectsOK := moveTo.Fields()["objects"].Type.(*graphql.List)
		assert.True(t, moveToObjectsOK)
		moveToObjectsObjects, moveToObjectsObjectsOK := moveToObjects.OfType.(*graphql.InputObject)
		assert.True(t, moveToObjectsObjectsOK)
		assert.Equal(t, 2, len(moveToObjectsObjects.Fields()))
		assert.NotNil(t, moveToObjectsObjects.Fields()["id"])
		assert.NotNil(t, moveToObjectsObjects.Fields()["beacon"])
		assert.NotNil(t, moveTo.Fields()["force"])
		_, moveToForceOK := moveTo.Fields()["force"].Type.(*graphql.NonNull)
		assert.True(t, moveToForceOK)
		assert.NotNil(t, fields["moveAwayFrom"])
		moveAwayFrom, moveAwayFromOK := fields["moveAwayFrom"].Type.(*graphql.InputObject)
		assert.True(t, moveAwayFromOK)
		assert.NotNil(t, moveAwayFrom.Fields()["concepts"])
		assert.NotNil(t, moveAwayFrom.Fields()["objects"])
		moveAwayFromObjects, moveAwayFromObjectsOK := moveAwayFrom.Fields()["objects"].Type.(*graphql.List)
		assert.True(t, moveAwayFromObjectsOK)
		moveAwayFromObjectsObjects, moveAwayFromObjectsObjectsOK := moveAwayFromObjects.OfType.(*graphql.InputObject)
		assert.Equal(t, 2, len(moveAwayFromObjectsObjects.Fields()))
		assert.True(t, moveAwayFromObjectsObjectsOK)
		assert.NotNil(t, moveAwayFromObjectsObjects.Fields()["id"])
		assert.NotNil(t, moveAwayFromObjectsObjects.Fields()["beacon"])
		assert.NotNil(t, moveAwayFrom.Fields()["force"])
		_, moveAwayFromForceOK := moveAwayFrom.Fields()["force"].Type.(*graphql.NonNull)
		assert.True(t, moveAwayFromForceOK)
		targetVectors := fields["targetVectors"]
		targetVectorsList, targetVectorsListOK := targetVectors.Type.(*graphql.List)
		assert.True(t, targetVectorsListOK)
		assert.Equal(t, "String", targetVectorsList.OfType.Name())
		assert.NotNil(t, targetVectors)
	})
}

func TestNearTextGraphQLArgumentWithAutocorrect(t *testing.T) {
	t.Run("should generate nearText argument with autocorrect properly", func(t *testing.T) {
		// given
		prefix := "Prefix"
		classname := "Class"
		// when
		nearText := New(&fakeTransformer{}).nearTextArgument(prefix, classname, false)

		// then
		// the built graphQL field needs to support this structure:
		// {
		//   concepts: ["c1", "c2"],
		//   certainty: 0.9,
		//   autocorrect: true,
		//   moveTo: {
		//          concepts: ["c1", "c2"],
		//          objects: [
		//               { id: "some-uuid-value"}],
		//               { beacon: "some-beacon-value"}
		//          ],
		//          force: 0.8
		//   }
		//   moveAwayFrom: {
		//          concepts: ["c1", "c2"],
		//          objects: [
		//               { id: "some-uuid-value"}],
		//               { beacon: "some-beacon-value"}
		//          ],
		//          force: 0.8
		//   },
		//   targetVectors: ["targetVector"],
		// }
		assert.NotNil(t, nearText)
		assert.Equal(t, "PrefixClassNearTextInpObj", nearText.Type.Name())
		nearTextFields, ok := nearText.Type.(*graphql.InputObject)
		assert.True(t, ok)
		assert.NotNil(t, nearTextFields)
		assert.Equal(t, 7, len(nearTextFields.Fields()))
		fields := nearTextFields.Fields()
		concepts := fields["concepts"]
		conceptsNonNull, conceptsNonNullOK := concepts.Type.(*graphql.NonNull)
		assert.True(t, conceptsNonNullOK)
		conceptsNonNullList, conceptsNonNullListOK := conceptsNonNull.OfType.(*graphql.List)
		assert.True(t, conceptsNonNullListOK)
		assert.Equal(t, "String", conceptsNonNullList.OfType.Name())
		assert.NotNil(t, concepts)
		conceptsType, conceptsTypeOK := concepts.Type.(*graphql.NonNull)
		assert.True(t, conceptsTypeOK)
		assert.NotNil(t, conceptsType)
		assert.NotNil(t, fields["certainty"])
		assert.NotNil(t, fields["distance"])
		assert.NotNil(t, fields["autocorrect"])
		assert.NotNil(t, fields["moveTo"])
		moveTo, moveToOK := fields["moveTo"].Type.(*graphql.InputObject)
		assert.True(t, moveToOK)
		assert.Equal(t, 3, len(moveTo.Fields()))
		assert.NotNil(t, moveTo.Fields()["concepts"])
		moveToConcepts, moveToConceptsOK := moveTo.Fields()["concepts"].Type.(*graphql.List)
		assert.True(t, moveToConceptsOK)
		assert.Equal(t, "String", moveToConcepts.OfType.Name())
		assert.NotNil(t, moveToConcepts)
		assert.NotNil(t, moveTo.Fields()["objects"])
		moveToObjects, moveToObjectsOK := moveTo.Fields()["objects"].Type.(*graphql.List)
		assert.True(t, moveToObjectsOK)
		moveToObjectsObjects, moveToObjectsObjectsOK := moveToObjects.OfType.(*graphql.InputObject)
		assert.True(t, moveToObjectsObjectsOK)
		assert.Equal(t, 2, len(moveToObjectsObjects.Fields()))
		assert.NotNil(t, moveToObjectsObjects.Fields()["id"])
		assert.NotNil(t, moveToObjectsObjects.Fields()["beacon"])
		assert.NotNil(t, moveTo.Fields()["force"])
		_, moveToForceOK := moveTo.Fields()["force"].Type.(*graphql.NonNull)
		assert.True(t, moveToForceOK)
		assert.NotNil(t, fields["moveAwayFrom"])
		moveAwayFrom, moveAwayFromOK := fields["moveAwayFrom"].Type.(*graphql.InputObject)
		assert.True(t, moveAwayFromOK)
		assert.NotNil(t, moveAwayFrom.Fields()["concepts"])
		assert.NotNil(t, moveAwayFrom.Fields()["objects"])
		moveAwayFromObjects, moveAwayFromObjectsOK := moveAwayFrom.Fields()["objects"].Type.(*graphql.List)
		assert.True(t, moveAwayFromObjectsOK)
		moveAwayFromObjectsObjects, moveAwayFromObjectsObjectsOK := moveAwayFromObjects.OfType.(*graphql.InputObject)
		assert.Equal(t, 2, len(moveAwayFromObjectsObjects.Fields()))
		assert.True(t, moveAwayFromObjectsObjectsOK)
		assert.NotNil(t, moveAwayFromObjectsObjects.Fields()["id"])
		assert.NotNil(t, moveAwayFromObjectsObjects.Fields()["beacon"])
		assert.NotNil(t, moveAwayFrom.Fields()["force"])
		_, moveAwayFromForceOK := moveAwayFrom.Fields()["force"].Type.(*graphql.NonNull)
		assert.True(t, moveAwayFromForceOK)
		targetVectors := fields["targetVectors"]
		targetVectorsList, targetVectorsListOK := targetVectors.Type.(*graphql.List)
		assert.True(t, targetVectorsListOK)
		assert.NotNil(t, targetVectorsList)
		assert.Equal(t, "String", targetVectorsList.OfType.Name())
		assert.NotNil(t, targetVectors)
	})
}
