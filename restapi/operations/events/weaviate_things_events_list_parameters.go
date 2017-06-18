/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package events




import (
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/swag"

	strfmt "github.com/go-openapi/strfmt"
)

// NewWeaviateThingsEventsListParams creates a new WeaviateThingsEventsListParams object
// with the default values initialized.
func NewWeaviateThingsEventsListParams() WeaviateThingsEventsListParams {
	var ()
	return WeaviateThingsEventsListParams{}
}

// WeaviateThingsEventsListParams contains all the bound params for the weaviate things events list operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.things.events.list
type WeaviateThingsEventsListParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request

	/*The maximum number of items to be returned per page.
	  In: query
	*/
	MaxResults *int64
	/*The page number of the items to be returned.
	  In: query
	*/
	Page *int64
	/*Unique ID of the thing.
	  Required: true
	  In: path
	*/
	ThingID strfmt.UUID
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *WeaviateThingsEventsListParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	qs := runtime.Values(r.URL.Query())

	qMaxResults, qhkMaxResults, _ := qs.GetOK("maxResults")
	if err := o.bindMaxResults(qMaxResults, qhkMaxResults, route.Formats); err != nil {
		res = append(res, err)
	}

	qPage, qhkPage, _ := qs.GetOK("page")
	if err := o.bindPage(qPage, qhkPage, route.Formats); err != nil {
		res = append(res, err)
	}

	rThingID, rhkThingID, _ := route.Params.GetOK("thingId")
	if err := o.bindThingID(rThingID, rhkThingID, route.Formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateThingsEventsListParams) bindMaxResults(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		return nil
	}

	value, err := swag.ConvertInt64(raw)
	if err != nil {
		return errors.InvalidType("maxResults", "query", "int64", raw)
	}
	o.MaxResults = &value

	return nil
}

func (o *WeaviateThingsEventsListParams) bindPage(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		return nil
	}

	value, err := swag.ConvertInt64(raw)
	if err != nil {
		return errors.InvalidType("page", "query", "int64", raw)
	}
	o.Page = &value

	return nil
}

func (o *WeaviateThingsEventsListParams) bindThingID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	value, err := formats.Parse("uuid", raw)
	if err != nil {
		return errors.InvalidType("thingId", "path", "strfmt.UUID", raw)
	}
	o.ThingID = *(value.(*strfmt.UUID))

	return nil
}
