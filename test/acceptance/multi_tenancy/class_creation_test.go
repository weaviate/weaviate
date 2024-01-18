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

package test

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestClassMultiTenancyDisabled(t *testing.T) {
	testClass := models.Class{
		Class: "ClassDisableMultiTenancy",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: false,
		},
	}
	objUUID := strfmt.UUID("0927a1e0-398e-4e76-91fb-04a7a8f0405c")

	helper.CreateClass(t, &testClass)
	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	helper.CreateObjectsBatch(t, []*models.Object{{
		ID:    objUUID,
		Class: testClass.Class,
	}})

	object, err := helper.GetObject(t, testClass.Class, objUUID)
	require.Nil(t, err)
	require.NotNil(t, object)
	require.Equal(t, objUUID, object.ID)
}

func TestClassMultiTenancyDisabledSchemaPrint(t *testing.T) {
	testClass := models.Class{Class: "ClassDisableMultiTenancy"}
	helper.CreateClass(t, &testClass)
	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	classReturn := helper.GetClass(t, testClass.Class)
	require.NotNil(t, classReturn.MultiTenancyConfig)
}
