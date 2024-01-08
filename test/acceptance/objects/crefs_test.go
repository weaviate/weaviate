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
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/batch"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	testhelper "github.com/weaviate/weaviate/test/helper"
)

const (
	beaconStart = "weaviate://localhost/"
	pathStart   = "/v1/objects/"
)

func TestRefsWithoutToClass(t *testing.T) {
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: "ReferenceTo"})
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
	refToClassName := "ReferenceTo"
	refFromClassName := "ReferenceFrom"
	otherClassMT := "Other"

	// other class has multi-tenancy enabled to make sure that problems trigger an error
	paramsMT := clschema.NewSchemaObjectsCreateParams().WithObjectClass(
		&models.Class{Class: otherClassMT, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
	)
	respMT, err := helper.Client(t).Schema.SchemaObjectsCreate(paramsMT, nil)
	helper.AssertRequestOk(t, respMT, err, nil)

	tenant := "tenant"
	tenants := make([]*models.Tenant, 1)
	for i := range tenants {
		tenants[i] = &models.Tenant{Name: tenant}
	}
	helper.CreateTenants(t, otherClassMT, tenants)

	refFromClass := &models.Class{
		Class: refFromClassName,
		Properties: []*models.Property{
			{
				DataType: []string{refToClassName},
				Name:     "ref",
			},
		},
	}
	params2 := clschema.NewSchemaObjectsCreateParams().WithObjectClass(refFromClass)
	resp2, err := helper.Client(t).Schema.SchemaObjectsCreate(params2, nil)
	helper.AssertRequestOk(t, resp2, err, nil)

	defer deleteObjectClass(t, refToClassName)
	defer deleteObjectClass(t, refFromClassName)
	defer deleteObjectClass(t, otherClassMT)

	refToId := assertCreateObject(t, refToClassName, map[string]interface{}{})
	assertGetObjectWithClass(t, refToId, refToClassName)
	assertCreateObjectWithID(t, otherClassMT, tenant, refToId, map[string]interface{}{})
	refFromId := assertCreateObject(t, refFromClassName, map[string]interface{}{})
	assertGetObjectWithClass(t, refFromId, refFromClassName)

	postRefParams := objects.NewObjectsClassReferencesCreateParams().
		WithID(refFromId).
		WithPropertyName("ref").WithClassName(refFromClass.Class).
		WithBody(&models.SingleRef{
			Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s", refToId.String())),
		})
	postRefResponse, err := helper.Client(t).Objects.ObjectsClassReferencesCreate(postRefParams, nil)
	helper.AssertRequestOk(t, postRefResponse, err, nil)

	// validate that ref was create for the correct class
	objWithRef := func() interface{} {
		obj := assertGetObjectWithClass(t, refFromId, refFromClassName)
		return obj.Properties
	}
	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"ref": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf(beaconStart+"%s/%s", refToClassName, refToId.String()),
				"href":   fmt.Sprintf(pathStart+"%s/%s", refToClassName, refToId.String()),
			},
		},
	}, objWithRef)

	// update prop with multiple references
	updateRefParams := objects.NewObjectsClassReferencesPutParams().
		WithID(refFromId).
		WithPropertyName("ref").WithClassName(refFromClass.Class).
		WithBody(models.MultipleRef{
			{Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s", refToId.String()))},
			{Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s/%s", refToClassName, refToId.String()))},
		})
	updateRefResponse, err := helper.Client(t).Objects.ObjectsClassReferencesPut(updateRefParams, nil)
	helper.AssertRequestOk(t, updateRefResponse, err, nil)

	objWithTwoRef := func() interface{} {
		obj := assertGetObjectWithClass(t, refFromId, refFromClassName)
		return obj.Properties
	}
	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"ref": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf(beaconStart+"%s/%s", refToClassName, refToId.String()),
				"href":   fmt.Sprintf(pathStart+"%s/%s", refToClassName, refToId.String()),
			},
			map[string]interface{}{
				"beacon": fmt.Sprintf(beaconStart+"%s/%s", refToClassName, refToId.String()),
				"href":   fmt.Sprintf(pathStart+"%s/%s", refToClassName, refToId.String()),
			},
		},
	}, objWithTwoRef)

	// delete reference without class
	deleteRefParams := objects.NewObjectsClassReferencesDeleteParams().
		WithID(refFromId).
		WithPropertyName("ref").WithClassName(refFromClass.Class).
		WithBody(&models.SingleRef{
			Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s", refToId.String())),
		})
	deleteRefResponse, err := helper.Client(t).Objects.ObjectsClassReferencesDelete(deleteRefParams, nil)
	helper.AssertRequestOk(t, deleteRefResponse, err, nil)
	objWithoutRef := func() interface{} {
		obj := assertGetObjectWithClass(t, refFromId, refFromClassName)
		return obj.Properties
	}
	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"ref": []interface{}{},
	}, objWithoutRef)
}

func TestRefsMultiTarget(t *testing.T) {
	refToClassName := "ReferenceTo"
	refFromClassName := "ReferenceFrom"
	defer deleteObjectClass(t, refToClassName)
	defer deleteObjectClass(t, refFromClassName)

	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: refToClassName})
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	refFromClass := &models.Class{
		Class: refFromClassName,
		Properties: []*models.Property{
			{
				DataType: []string{refToClassName, refFromClassName},
				Name:     "ref",
			},
		},
	}
	params2 := clschema.NewSchemaObjectsCreateParams().WithObjectClass(refFromClass)
	resp2, err := helper.Client(t).Schema.SchemaObjectsCreate(params2, nil)
	helper.AssertRequestOk(t, resp2, err, nil)

	refToId := assertCreateObject(t, refToClassName, map[string]interface{}{})
	assertGetObjectEventually(t, refToId)
	refFromId := assertCreateObject(t, refFromClassName, map[string]interface{}{})
	assertGetObjectEventually(t, refFromId)

	cases := []struct {
		classRef string
		id       string
	}{
		{classRef: "", id: refToId.String()},
		{classRef: refToClassName + "/", id: refToId.String()},
		{classRef: refFromClassName + "/", id: refFromId.String()},
	}
	for _, tt := range cases {
		postRefParams := objects.NewObjectsClassReferencesCreateParams().
			WithID(refFromId).
			WithPropertyName("ref").WithClassName(refFromClass.Class).
			WithBody(&models.SingleRef{
				Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s%s", tt.classRef, tt.id)),
			})
		postRefResponse, err := helper.Client(t).Objects.ObjectsClassReferencesCreate(postRefParams, nil)
		helper.AssertRequestOk(t, postRefResponse, err, nil)

		// validate that ref was create for the correct class
		objWithRef := func() interface{} {
			obj := assertGetObjectWithClass(t, refFromId, refFromClassName)
			return obj.Properties
		}
		testhelper.AssertEventuallyEqual(t, map[string]interface{}{
			"ref": []interface{}{
				map[string]interface{}{
					"beacon": fmt.Sprintf(beaconStart+"%s%s", tt.classRef, tt.id),
					"href":   fmt.Sprintf(pathStart+"%s%s", tt.classRef, tt.id),
				},
			},
		}, objWithRef)

		// delete refs
		updateRefParams := objects.NewObjectsClassReferencesPutParams().
			WithID(refFromId).
			WithPropertyName("ref").WithClassName(refFromClass.Class).
			WithBody(models.MultipleRef{})
		updateRefResponse, err := helper.Client(t).Objects.ObjectsClassReferencesPut(updateRefParams, nil)
		helper.AssertRequestOk(t, updateRefResponse, err, nil)
	}
}

func TestBatchRefsMultiTarget(t *testing.T) {
	refToClassName := "ReferenceTo"
	refFromClassName := "ReferenceFrom"
	defer deleteObjectClass(t, refToClassName)
	defer deleteObjectClass(t, refFromClassName)

	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: refToClassName})
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	refFromClass := &models.Class{
		Class: refFromClassName,
		Properties: []*models.Property{
			{
				DataType: []string{refToClassName, refFromClassName},
				Name:     "ref",
			},
		},
	}
	params2 := clschema.NewSchemaObjectsCreateParams().WithObjectClass(refFromClass)
	resp2, err := helper.Client(t).Schema.SchemaObjectsCreate(params2, nil)
	helper.AssertRequestOk(t, resp2, err, nil)

	uuidsTo := make([]strfmt.UUID, 10)
	uuidsFrom := make([]strfmt.UUID, 10)
	for i := 0; i < 10; i++ {
		uuidsTo[i] = assertCreateObject(t, refToClassName, map[string]interface{}{})
		assertGetObjectEventually(t, uuidsTo[i])
		uuidsFrom[i] = assertCreateObject(t, refFromClassName, map[string]interface{}{})
		assertGetObjectEventually(t, uuidsFrom[i])
	}

	// add refs without toClass
	var batchRefs []*models.BatchReference
	for i := range uuidsFrom[:2] {
		from := beaconStart + "ReferenceFrom/" + uuidsFrom[i] + "/ref"
		to := beaconStart + uuidsTo[i]
		batchRefs = append(batchRefs, &models.BatchReference{From: strfmt.URI(from), To: strfmt.URI(to)})
	}

	// add refs with toClass target 1
	for i := range uuidsFrom[2:5] {
		j := i + 2
		from := beaconStart + "ReferenceFrom/" + uuidsFrom[j] + "/ref"
		to := beaconStart + "ReferenceTo/" + uuidsTo[j]
		batchRefs = append(batchRefs, &models.BatchReference{From: strfmt.URI(from), To: strfmt.URI(to)})
	}

	// add refs with toClass target 2
	for i := range uuidsFrom[5:] {
		j := i + 5
		from := beaconStart + "ReferenceFrom/" + uuidsFrom[j] + "/ref"
		to := beaconStart + "ReferenceFrom/" + uuidsTo[j]
		batchRefs = append(batchRefs, &models.BatchReference{From: strfmt.URI(from), To: strfmt.URI(to)})
	}

	postRefParams := batch.NewBatchReferencesCreateParams().WithBody(batchRefs)
	postRefResponse, err := helper.Client(t).Batch.BatchReferencesCreate(postRefParams, nil)
	helper.AssertRequestOk(t, postRefResponse, err, nil)

	// no autodetect for multi-target
	for i := range uuidsFrom[:2] {
		objWithRef := func() interface{} {
			obj := assertGetObjectWithClass(t, uuidsFrom[i], refFromClassName)
			return obj.Properties
		}
		testhelper.AssertEventuallyEqual(t, map[string]interface{}{
			"ref": []interface{}{
				map[string]interface{}{
					"beacon": fmt.Sprintf(beaconStart+"%s", uuidsTo[i].String()),
					"href":   fmt.Sprintf(pathStart+"%s", uuidsTo[i].String()),
				},
			},
		}, objWithRef)
	}

	// refs for target 1
	for i := range uuidsFrom[2:5] {
		j := i + 2
		objWithRef := func() interface{} {
			obj := assertGetObjectWithClass(t, uuidsFrom[j], refFromClassName)
			return obj.Properties
		}
		testhelper.AssertEventuallyEqual(t, map[string]interface{}{
			"ref": []interface{}{
				map[string]interface{}{
					"beacon": fmt.Sprintf(beaconStart+"%s/%s", refToClassName, uuidsTo[j].String()),
					"href":   fmt.Sprintf(pathStart+"%s/%s", refToClassName, uuidsTo[j].String()),
				},
			},
		}, objWithRef)
	}

	// refs for target 2
	for i := range uuidsFrom[5:] {
		j := i + 5
		objWithRef := func() interface{} {
			obj := assertGetObjectWithClass(t, uuidsFrom[j], refFromClassName)
			return obj.Properties
		}
		testhelper.AssertEventuallyEqual(t, map[string]interface{}{
			"ref": []interface{}{
				map[string]interface{}{
					"beacon": fmt.Sprintf(beaconStart+"%s/%s", refFromClassName, uuidsTo[j].String()),
					"href":   fmt.Sprintf(pathStart+"%s/%s", refFromClassName, uuidsTo[j].String()),
				},
			},
		}, objWithRef)
	}
}

func TestBatchRefsWithoutFromAndToClass(t *testing.T) {
	refToClassName := "ReferenceTo"
	refFromClassName := "ReferenceFrom"

	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: refToClassName})
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	refFromClass := &models.Class{
		Class: refFromClassName,
		Properties: []*models.Property{
			{
				DataType: []string{refToClassName},
				Name:     "ref",
			},
		},
	}
	params2 := clschema.NewSchemaObjectsCreateParams().WithObjectClass(refFromClass)
	resp2, err := helper.Client(t).Schema.SchemaObjectsCreate(params2, nil)
	helper.AssertRequestOk(t, resp2, err, nil)

	defer deleteObjectClass(t, refToClassName)
	defer deleteObjectClass(t, refFromClassName)

	uuidsTo := make([]strfmt.UUID, 10)
	uuidsFrom := make([]strfmt.UUID, 10)
	for i := 0; i < 10; i++ {
		uuidsTo[i] = assertCreateObject(t, refToClassName, map[string]interface{}{})
		assertGetObjectWithClass(t, uuidsTo[i], refToClassName)

		uuidsFrom[i] = assertCreateObject(t, refFromClassName, map[string]interface{}{})
		assertGetObjectWithClass(t, uuidsFrom[i], refFromClassName)
	}

	// cannot do from urls without class
	var batchRefs []*models.BatchReference
	for i := range uuidsFrom {
		from := beaconStart + uuidsFrom[i] + "/ref"
		to := beaconStart + uuidsTo[i]
		batchRefs = append(batchRefs, &models.BatchReference{From: strfmt.URI(from), To: strfmt.URI(to)})
	}

	postRefParams := batch.NewBatchReferencesCreateParams().WithBody(batchRefs)
	resp3, err := helper.Client(t).Batch.BatchReferencesCreate(postRefParams, nil)
	require.Nil(t, err)
	require.NotNil(t, resp3)
	for i := range resp3.Payload {
		require.NotNil(t, resp3.Payload[i].Result.Errors)
	}
}

func TestBatchRefWithErrors(t *testing.T) {
	refToClassName := "ReferenceTo"
	refFromClassName := "ReferenceFrom"

	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: refToClassName})
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	refFromClass := &models.Class{
		Class: refFromClassName,
		Properties: []*models.Property{
			{
				DataType: []string{refToClassName},
				Name:     "ref",
			},
		},
	}
	params2 := clschema.NewSchemaObjectsCreateParams().WithObjectClass(refFromClass)
	resp2, err := helper.Client(t).Schema.SchemaObjectsCreate(params2, nil)
	helper.AssertRequestOk(t, resp2, err, nil)

	defer deleteObjectClass(t, refToClassName)
	defer deleteObjectClass(t, refFromClassName)

	uuidsTo := make([]strfmt.UUID, 2)
	uuidsFrom := make([]strfmt.UUID, 2)
	for i := 0; i < 2; i++ {
		uuidsTo[i] = assertCreateObject(t, refToClassName, map[string]interface{}{})
		assertGetObjectWithClass(t, uuidsTo[i], refToClassName)

		uuidsFrom[i] = assertCreateObject(t, refFromClassName, map[string]interface{}{})
		assertGetObjectWithClass(t, uuidsFrom[i], refFromClassName)
	}

	var batchRefs []*models.BatchReference
	for i := range uuidsFrom {
		from := beaconStart + "ReferenceFrom/" + uuidsFrom[i] + "/ref"
		to := beaconStart + uuidsTo[i]
		batchRefs = append(batchRefs, &models.BatchReference{From: strfmt.URI(from), To: strfmt.URI(to)})
	}

	// append one entry with a non-existent class
	batchRefs = append(batchRefs, &models.BatchReference{From: strfmt.URI(beaconStart + "DoesNotExist/" + uuidsFrom[0] + "/ref"), To: strfmt.URI(beaconStart + uuidsTo[0])})

	// append one entry with a non-existent property for existing class
	batchRefs = append(batchRefs, &models.BatchReference{From: strfmt.URI(beaconStart + "ReferenceFrom/" + uuidsFrom[0] + "/doesNotExist"), To: strfmt.URI(beaconStart + uuidsTo[0])})

	postRefParams := batch.NewBatchReferencesCreateParams().WithBody(batchRefs)
	postRefResponse, err := helper.Client(t).Batch.BatchReferencesCreate(postRefParams, nil)
	helper.AssertRequestOk(t, postRefResponse, err, nil)

	require.NotNil(t, postRefResponse.Payload[2].Result.Errors)
	require.Contains(t, postRefResponse.Payload[2].Result.Errors.Error[0].Message, "class DoesNotExist does not exist")

	require.NotNil(t, postRefResponse.Payload[3].Result.Errors)
	require.Contains(t, postRefResponse.Payload[3].Result.Errors.Error[0].Message, "property doesNotExist does not exist for class ReferenceFrom")
}

func TestBatchRefsWithoutToClass(t *testing.T) {
	refToClassName := "ReferenceTo"
	refFromClassName := "ReferenceFrom"
	otherClassMT := "Other"

	// other class has multi-tenancy enabled to make sure that problems trigger an error
	paramsMT := clschema.NewSchemaObjectsCreateParams().WithObjectClass(
		&models.Class{Class: otherClassMT, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
	)
	respMT, err := helper.Client(t).Schema.SchemaObjectsCreate(paramsMT, nil)
	helper.AssertRequestOk(t, respMT, err, nil)

	tenant := "tenant"
	tenants := make([]*models.Tenant, 1)
	for i := range tenants {
		tenants[i] = &models.Tenant{Name: tenant}
	}
	helper.CreateTenants(t, otherClassMT, tenants)

	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: refToClassName})
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	refFromClass := &models.Class{
		Class: refFromClassName,
		Properties: []*models.Property{
			{
				DataType: []string{refToClassName},
				Name:     "ref",
			},
		},
	}
	params2 := clschema.NewSchemaObjectsCreateParams().WithObjectClass(refFromClass)
	resp2, err := helper.Client(t).Schema.SchemaObjectsCreate(params2, nil)
	helper.AssertRequestOk(t, resp2, err, nil)

	defer deleteObjectClass(t, refToClassName)
	defer deleteObjectClass(t, refFromClassName)
	defer deleteObjectClass(t, otherClassMT)

	uuidsTo := make([]strfmt.UUID, 10)
	uuidsFrom := make([]strfmt.UUID, 10)
	for i := 0; i < 10; i++ {
		uuidsTo[i] = assertCreateObject(t, refToClassName, map[string]interface{}{})
		assertGetObjectWithClass(t, uuidsTo[i], refToClassName)

		// create object with same id in MT class
		assertCreateObjectWithID(t, otherClassMT, tenant, uuidsTo[i], map[string]interface{}{})

		uuidsFrom[i] = assertCreateObject(t, refFromClassName, map[string]interface{}{})
		assertGetObjectWithClass(t, uuidsFrom[i], refFromClassName)
	}

	var batchRefs []*models.BatchReference
	for i := range uuidsFrom {
		from := beaconStart + "ReferenceFrom/" + uuidsFrom[i] + "/ref"
		to := beaconStart + uuidsTo[i]
		batchRefs = append(batchRefs, &models.BatchReference{From: strfmt.URI(from), To: strfmt.URI(to)})
	}

	postRefParams := batch.NewBatchReferencesCreateParams().WithBody(batchRefs)
	postRefResponse, err := helper.Client(t).Batch.BatchReferencesCreate(postRefParams, nil)
	helper.AssertRequestOk(t, postRefResponse, err, nil)

	for i := range uuidsFrom {
		// validate that ref was create for the correct class
		objWithRef := func() interface{} {
			obj := assertGetObjectWithClass(t, uuidsFrom[i], refFromClassName)
			return obj.Properties
		}
		testhelper.AssertEventuallyEqual(t, map[string]interface{}{
			"ref": []interface{}{
				map[string]interface{}{
					"beacon": fmt.Sprintf(beaconStart+"%s/%s", refToClassName, uuidsTo[i].String()),
					"href":   fmt.Sprintf(pathStart+"%s/%s", refToClassName, uuidsTo[i].String()),
				},
			},
		}, objWithRef)
	}
}

func TestObjectBatchToClassDetection(t *testing.T) {
	// uses same code path as normal object add
	refToClassName := "ReferenceTo"
	refFromClassName := "ReferenceFrom"
	defer deleteObjectClass(t, refToClassName)
	defer deleteObjectClass(t, refFromClassName)

	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: refToClassName})
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	refFromClass := &models.Class{
		Class: refFromClassName,
		Properties: []*models.Property{
			{
				DataType: []string{refToClassName},
				Name:     "ref",
			},
		},
	}
	params2 := clschema.NewSchemaObjectsCreateParams().WithObjectClass(refFromClass)
	resp2, err := helper.Client(t).Schema.SchemaObjectsCreate(params2, nil)
	helper.AssertRequestOk(t, resp2, err, nil)

	refs := make([]interface{}, 10)
	uuidsTo := make([]strfmt.UUID, 10)

	for i := 0; i < 10; i++ {
		uuidTo := assertCreateObject(t, refToClassName, map[string]interface{}{})
		uuidsTo[i] = uuidTo
		assertGetObjectEventually(t, uuidTo)
		refs[i] = map[string]interface{}{
			"beacon": beaconStart + uuidTo,
		}
	}

	fromBatch := make([]*models.Object, 10)
	for i := 0; i < 10; i++ {
		fromBatch[i] = &models.Object{
			Class: refFromClassName,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"ref": refs[i : i+1],
			},
		}
	}
	paramsBatch := batch.NewBatchObjectsCreateParams().WithBody(
		batch.BatchObjectsCreateBody{
			Objects: fromBatch,
		},
	)
	res, err := helper.Client(t).Batch.BatchObjectsCreate(paramsBatch, nil)
	require.Nil(t, err)
	for _, elem := range res.Payload {
		assert.Nil(t, elem.Result.Errors)
	}

	for i := range fromBatch {
		// validate that ref was create for the correct class
		objWithRef := func() interface{} {
			obj := assertGetObjectWithClass(t, fromBatch[i].ID, refFromClassName)
			return obj.Properties
		}
		testhelper.AssertEventuallyEqual(t, map[string]interface{}{
			"ref": []interface{}{
				map[string]interface{}{
					"beacon": fmt.Sprintf(beaconStart+"%s/%s", refToClassName, uuidsTo[i].String()),
					"href":   fmt.Sprintf(pathStart+"%s/%s", refToClassName, uuidsTo[i].String()),
				},
			},
		}, objWithRef)
	}
}

func TestObjectCrefWithoutToClass(t *testing.T) {
	refToClassName := "ReferenceTo"
	refFromClassName := "ReferenceFrom"
	otherClassMT := "Other"

	// other class has multi-tenancy enabled to make sure that problems trigger an error
	paramsMT := clschema.NewSchemaObjectsCreateParams().WithObjectClass(
		&models.Class{Class: otherClassMT, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
	)
	respMT, err := helper.Client(t).Schema.SchemaObjectsCreate(paramsMT, nil)
	helper.AssertRequestOk(t, respMT, err, nil)

	tenant := "tenant"
	tenants := make([]*models.Tenant, 1)
	for i := range tenants {
		tenants[i] = &models.Tenant{Name: tenant}
	}
	helper.CreateTenants(t, otherClassMT, tenants)

	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: refToClassName})
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	refFromClass := &models.Class{
		Class: refFromClassName,
		Properties: []*models.Property{
			{
				DataType: []string{refToClassName},
				Name:     "ref",
			},
		},
	}
	params2 := clschema.NewSchemaObjectsCreateParams().WithObjectClass(refFromClass)
	resp2, err := helper.Client(t).Schema.SchemaObjectsCreate(params2, nil)
	helper.AssertRequestOk(t, resp2, err, nil)

	defer deleteObjectClass(t, refToClassName)
	defer deleteObjectClass(t, refFromClassName)
	defer deleteObjectClass(t, otherClassMT)

	refs := make([]interface{}, 10)
	uuids := make([]strfmt.UUID, 10)
	for i := 0; i < 10; i++ {
		uuidTo := assertCreateObject(t, refToClassName, map[string]interface{}{})
		assertGetObjectWithClass(t, uuidTo, refToClassName)

		// create object with same id in MT class
		assertCreateObjectWithID(t, otherClassMT, tenant, uuidTo, map[string]interface{}{})

		refs[i] = map[string]interface{}{
			"beacon": beaconStart + uuidTo,
		}
		uuids[i] = uuidTo
	}

	uuidFrom := assertCreateObject(t, refFromClassName, map[string]interface{}{"ref": refs})
	assertGetObjectWithClass(t, uuidFrom, refFromClassName)

	objWithRef := assertGetObjectWithClass(t, uuidFrom, refFromClassName)
	assert.NotNil(t, objWithRef.Properties)
	refsReturned := objWithRef.Properties.(map[string]interface{})["ref"].([]interface{})
	for i := range refsReturned {
		require.Equal(t, refsReturned[i].(map[string]interface{})["beacon"], string(beaconStart+"ReferenceTo/"+uuids[i]))
	}
}

// This test suite is meant to prevent a regression on
// https://github.com/weaviate/weaviate/issues/868, hence it tries to
// reproduce the steps outlined in there as closely as possible
func Test_CREFWithCardinalityMany_UsingPatch(t *testing.T) {
	defer func() {
		// clean up so we can run this test multiple times in a row
		delCityParams := clschema.NewSchemaObjectsDeleteParams().WithClassName("ReferenceTestCity")
		dresp, err := helper.Client(t).Schema.SchemaObjectsDelete(delCityParams, nil)
		t.Logf("clean up - delete city \n%v\n %v", dresp, err)

		delPlaceParams := clschema.NewSchemaObjectsDeleteParams().WithClassName("ReferenceTestPlace")
		dresp, err = helper.Client(t).Schema.SchemaObjectsDelete(delPlaceParams, nil)
		t.Logf("clean up - delete place \n%v\n %v", dresp, err)
	}()

	t.Log("1. create ReferenceTestPlace class")
	placeClass := &models.Class{
		Class: "ReferenceTestPlace",
		Properties: []*models.Property{
			{
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				Name:         "name",
			},
		},
	}
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(placeClass)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("2. create ReferenceTestCity class with HasPlaces (many) cross-ref")
	cityClass := &models.Class{
		Class: "ReferenceTestCity",
		Properties: []*models.Property{
			{
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				Name:         "name",
			},
			{
				DataType: []string{"ReferenceTestPlace"},
				Name:     "HasPlaces",
			},
		},
	}
	params = clschema.NewSchemaObjectsCreateParams().WithObjectClass(cityClass)
	resp, err = helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("3. add two places and save their IDs")
	place1ID := assertCreateObject(t, "ReferenceTestPlace", map[string]interface{}{
		"name": "Place 1",
	})
	place2ID := assertCreateObject(t, "ReferenceTestPlace", map[string]interface{}{
		"name": "Place 2",
	})
	assertGetObjectEventually(t, place1ID)
	assertGetObjectEventually(t, place2ID)

	t.Log("4. add one city")
	cityID := assertCreateObject(t, "ReferenceTestCity", map[string]interface{}{
		"name": "My City",
	})
	assertGetObjectEventually(t, cityID)

	t.Log("5. patch city to point to the first place")
	patchParams := objects.NewObjectsPatchParams().
		WithID(cityID).
		WithBody(&models.Object{
			Class: "ReferenceTestCity",
			Properties: map[string]interface{}{
				"hasPlaces": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", place1ID.String()),
					},
				},
			},
		})
	patchResp, err := helper.Client(t).Objects.ObjectsPatch(patchParams, nil)
	helper.AssertRequestOk(t, patchResp, err, nil)

	t.Log("6. verify first cross ref was added")

	actualThunk := func() interface{} {
		cityAfterFirstPatch := assertGetObject(t, cityID)
		return cityAfterFirstPatch.Properties
	}

	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"name": "My City",
		"hasPlaces": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/%s/%s", placeClass.Class, place1ID.String()),
				"href":   fmt.Sprintf("/v1/objects/%s/%s", placeClass.Class, place1ID.String()),
			},
		},
	}, actualThunk)

	t.Log("7. patch city to point to the second place")
	patchParams = objects.NewObjectsPatchParams().
		WithID(cityID).
		WithBody(&models.Object{
			Class: "ReferenceTestCity",
			Properties: map[string]interface{}{
				"hasPlaces": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", place2ID.String()),
					},
				},
			},
		})
	patchResp, err = helper.Client(t).Objects.ObjectsPatch(patchParams, nil)
	helper.AssertRequestOk(t, patchResp, err, nil)

	actualThunk = func() interface{} {
		city := assertGetObject(t, cityID)
		return city.Properties.(map[string]interface{})["hasPlaces"].([]interface{})
	}

	t.Log("9. verify both cross refs are present")
	expectedRefs := []interface{}{
		map[string]interface{}{
			"beacon": fmt.Sprintf("weaviate://localhost/%s/%s", placeClass.Class, place1ID.String()),
			"href":   fmt.Sprintf("/v1/objects/%s/%s", placeClass.Class, place1ID.String()),
		},
		map[string]interface{}{
			"beacon": fmt.Sprintf("weaviate://localhost/%s/%s", placeClass.Class, place2ID.String()),
			"href":   fmt.Sprintf("/v1/objects/%s/%s", placeClass.Class, place2ID.String()),
		},
	}

	testhelper.AssertEventuallyEqual(t, expectedRefs, actualThunk)
}

// This test suite is meant to prevent a regression on
// https://github.com/weaviate/weaviate/issues/868, hence it tries to
// reproduce the steps outlined in there as closely as possible
func Test_CREFWithCardinalityMany_UsingPostReference(t *testing.T) {
	defer func() {
		// clean up so we can run this test multiple times in a row
		delCityParams := clschema.NewSchemaObjectsDeleteParams().WithClassName("ReferenceTestCity")
		dresp, err := helper.Client(t).Schema.SchemaObjectsDelete(delCityParams, nil)
		t.Logf("clean up - delete city \n%v\n %v", dresp, err)

		delPlaceParams := clschema.NewSchemaObjectsDeleteParams().WithClassName("ReferenceTestPlace")
		dresp, err = helper.Client(t).Schema.SchemaObjectsDelete(delPlaceParams, nil)
		t.Logf("clean up - delete place \n%v\n %v", dresp, err)
	}()

	t.Log("1. create ReferenceTestPlace class")
	placeClass := &models.Class{
		Class: "ReferenceTestPlace",
		Properties: []*models.Property{
			{
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				Name:         "name",
			},
		},
	}
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(placeClass)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("2. create ReferenceTestCity class with HasPlaces (many) cross-ref")
	cityClass := &models.Class{
		Class: "ReferenceTestCity",
		Properties: []*models.Property{
			{
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				Name:         "name",
			},
			{
				DataType: []string{"ReferenceTestPlace"},
				Name:     "HasPlaces",
			},
		},
	}
	params = clschema.NewSchemaObjectsCreateParams().WithObjectClass(cityClass)
	resp, err = helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("3. add two places and save their IDs")
	place1ID := assertCreateObject(t, "ReferenceTestPlace", map[string]interface{}{
		"name": "Place 1",
	})
	place2ID := assertCreateObject(t, "ReferenceTestPlace", map[string]interface{}{
		"name": "Place 2",
	})
	assertGetObjectEventually(t, place1ID)
	assertGetObjectEventually(t, place2ID)
	t.Logf("Place 1 ID: %s", place1ID)
	t.Logf("Place 2 ID: %s", place2ID)

	t.Log("4. add one city")
	cityID := assertCreateObject(t, "ReferenceTestCity", map[string]interface{}{
		"name": "My City",
	})
	assertGetObjectEventually(t, cityID)

	t.Log("5. POST /references/ for place 1")
	postRefParams := objects.NewObjectsReferencesCreateParams().
		WithID(cityID).
		WithPropertyName("hasPlaces").
		WithBody(&models.SingleRef{
			Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", place1ID.String())),
		})
	postRefResponse, err := helper.Client(t).Objects.ObjectsReferencesCreate(postRefParams, nil)
	helper.AssertRequestOk(t, postRefResponse, err, nil)

	actualThunk := func() interface{} {
		city := assertGetObject(t, cityID)
		return city.Properties
	}
	t.Log("7. verify first cross ref was added")
	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"name": "My City",
		"hasPlaces": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/%s/%s", placeClass.Class, place1ID.String()),
				"href":   fmt.Sprintf("/v1/objects/%s/%s", placeClass.Class, place1ID.String()),
			},
		},
	}, actualThunk)

	t.Log("8. POST /references/ for place 2")
	postRefParams = objects.NewObjectsReferencesCreateParams().
		WithID(cityID).
		WithPropertyName("hasPlaces").
		WithBody(&models.SingleRef{
			Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", place2ID.String())),
		})
	postRefResponse, err = helper.Client(t).Objects.ObjectsReferencesCreate(postRefParams, nil)
	helper.AssertRequestOk(t, postRefResponse, err, nil)

	t.Log("9. verify both cross refs are present")
	actualThunk = func() interface{} {
		city := assertGetObject(t, cityID)
		return city.Properties.(map[string]interface{})["hasPlaces"].([]interface{})
	}

	expectedRefs := []interface{}{
		map[string]interface{}{
			"beacon": fmt.Sprintf("weaviate://localhost/%s/%s", placeClass.Class, place1ID.String()),
			"href":   fmt.Sprintf("/v1/objects/%s/%s", placeClass.Class, place1ID.String()),
		},
		map[string]interface{}{
			"beacon": fmt.Sprintf("weaviate://localhost/%s/%s", placeClass.Class, place2ID.String()),
			"href":   fmt.Sprintf("/v1/objects/%s/%s", placeClass.Class, place2ID.String()),
		},
	}

	testhelper.AssertEventuallyEqual(t, expectedRefs, actualThunk)
}
