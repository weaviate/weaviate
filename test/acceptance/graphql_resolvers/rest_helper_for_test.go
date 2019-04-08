package test

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/client/things"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/go-openapi/strfmt"
)

func assertGetThing(t *testing.T, uuid strfmt.UUID) *models.ThingGetResponse {
	getResp, err := helper.Client(t).Things.WeaviateThingsGet(things.NewWeaviateThingsGetParams().WithThingID(uuid), nil)

	var thing *models.ThingGetResponse

	helper.AssertRequestOk(t, getResp, err, func() {
		thing = getResp.Payload
	})

	return thing
}
