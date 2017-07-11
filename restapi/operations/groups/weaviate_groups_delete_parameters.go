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
  package groups

 
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"

	strfmt "github.com/go-openapi/strfmt"
)

// NewWeaviateGroupsDeleteParams creates a new WeaviateGroupsDeleteParams object
// with the default values initialized.
func NewWeaviateGroupsDeleteParams() WeaviateGroupsDeleteParams {
	var ()
	return WeaviateGroupsDeleteParams{}
}

// WeaviateGroupsDeleteParams contains all the bound params for the weaviate groups delete operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.groups.delete
type WeaviateGroupsDeleteParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request

	/*
	  Required: true
	  In: path
	*/
	GroupID strfmt.UUID
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *WeaviateGroupsDeleteParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	rGroupID, rhkGroupID, _ := route.Params.GetOK("groupId")
	if err := o.bindGroupID(rGroupID, rhkGroupID, route.Formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateGroupsDeleteParams) bindGroupID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	value, err := formats.Parse("uuid", raw)
	if err != nil {
		return errors.InvalidType("groupId", "path", "strfmt.UUID", raw)
	}
	o.GroupID = *(value.(*strfmt.UUID))

	return nil
}
