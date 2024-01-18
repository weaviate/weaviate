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

package nearimu

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func TestNearIMUGraphQLArgument(t *testing.T) {
	t.Run("should generate nearIMU argument properly", func(t *testing.T) {
		// given
		prefix := "Prefix"
		classname := "Class"
		// when
		nearIMU := nearIMUArgument(prefix, classname)

		// then
		// the built graphQL field needs to support this structure:
		// nearIMU: {
		//   imu: "base64;encoded,imu_data",
		//   distance: 0.9
		// }
		assert.NotNil(t, nearIMU)
		assert.Equal(t, "Multi2VecBindPrefixClassNearIMUInpObj", nearIMU.Type.Name())
		answerFields, ok := nearIMU.Type.(*graphql.InputObject)
		assert.True(t, ok)
		assert.NotNil(t, answerFields)
		assert.Equal(t, 3, len(answerFields.Fields()))
		fields := answerFields.Fields()
		imu := fields["imu"]
		imuNonNull, imuNonNullOK := imu.Type.(*graphql.NonNull)
		assert.True(t, imuNonNullOK)
		assert.Equal(t, "String", imuNonNull.OfType.Name())
		assert.NotNil(t, imu)
		assert.NotNil(t, fields["certainty"])
		assert.NotNil(t, fields["distance"])
	})
}
