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
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"

	strfmt "github.com/go-openapi/strfmt"
)

// NewWeaviateThingTemplatesGetParams creates a new WeaviateThingTemplatesGetParams object
// with the default values initialized.
func NewWeaviateThingTemplatesGetParams() WeaviateThingTemplatesGetParams {
	var ()
	return WeaviateThingTemplatesGetParams{}
}

// WeaviateThingTemplatesGetParams contains all the bound params for the weaviate thing templates get operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.thingTemplates.get
type WeaviateThingTemplatesGetParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request

	/*Unique ID of the thing template.
	  Required: true
	  In: path
	*/
	ThingTemplateID string
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *WeaviateThingTemplatesGetParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	rThingTemplateID, rhkThingTemplateID, _ := route.Params.GetOK("thingTemplateId")
	if err := o.bindThingTemplateID(rThingTemplateID, rhkThingTemplateID, route.Formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateThingTemplatesGetParams) bindThingTemplateID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	o.ThingTemplateID = raw

	return nil
}
