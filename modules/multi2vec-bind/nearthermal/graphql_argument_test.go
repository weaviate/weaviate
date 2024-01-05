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

package nearthermal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func TestNearThermalGraphQLArgument(t *testing.T) {
	t.Run("should generate nearThermal argument properly", func(t *testing.T) {
		// given
		prefix := "Prefix"
		classname := "Class"
		// when
		nearThermal := nearThermalArgument(prefix, classname)

		// then
		// the built graphQL field needs to support this structure:
		// nearThermal: {
		//   thermal: "base64;encoded,thermal_image",
		//   distance: 0.9
		// }
		assert.NotNil(t, nearThermal)
		assert.Equal(t, "Multi2VecBindPrefixClassNearThermalInpObj", nearThermal.Type.Name())
		answerFields, ok := nearThermal.Type.(*graphql.InputObject)
		assert.True(t, ok)
		assert.NotNil(t, answerFields)
		assert.Equal(t, 3, len(answerFields.Fields()))
		fields := answerFields.Fields()
		thermal := fields["thermal"]
		thermalNonNull, thermalNonNullOK := thermal.Type.(*graphql.NonNull)
		assert.True(t, thermalNonNullOK)
		assert.Equal(t, "String", thermalNonNull.OfType.Name())
		assert.NotNil(t, thermal)
		assert.NotNil(t, fields["certainty"])
		assert.NotNil(t, fields["distance"])
	})
}
