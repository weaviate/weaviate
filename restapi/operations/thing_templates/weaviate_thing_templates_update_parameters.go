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
  package thing_templates

 
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"io"
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/models"
)

// NewWeaviateThingTemplatesUpdateParams creates a new WeaviateThingTemplatesUpdateParams object
// with the default values initialized.
func NewWeaviateThingTemplatesUpdateParams() WeaviateThingTemplatesUpdateParams {
	var ()
	return WeaviateThingTemplatesUpdateParams{}
}

// WeaviateThingTemplatesUpdateParams contains all the bound params for the weaviate thing templates update operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.thingTemplates.update
type WeaviateThingTemplatesUpdateParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request

	/*
	  Required: true
	  In: body
	*/
	Body *models.ThingTemplate
	/*Unique ID of the thing template.
	  Required: true
	  In: path
	*/
	ThingTemplateID string
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *WeaviateThingTemplatesUpdateParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	if runtime.HasBody(r) {
		defer r.Body.Close()
		var body models.ThingTemplate
		if err := route.Consumer.Consume(r.Body, &body); err != nil {
			if err == io.EOF {
				res = append(res, errors.Required("body", "body"))
			} else {
				res = append(res, errors.NewParseError("body", "body", "", err))
			}

		} else {
			if err := body.Validate(route.Formats); err != nil {
				res = append(res, err)
			}

			if len(res) == 0 {
				o.Body = &body
			}
		}

	} else {
		res = append(res, errors.Required("body", "body"))
	}

	rThingTemplateID, rhkThingTemplateID, _ := route.Params.GetOK("thingTemplateId")
	if err := o.bindThingTemplateID(rThingTemplateID, rhkThingTemplateID, route.Formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateThingTemplatesUpdateParams) bindThingTemplateID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	o.ThingTemplateID = raw

	return nil
}
