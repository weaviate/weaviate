/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */

package actions

import (
	"io"
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/creativesoftwarefdn/weaviate/models"
)

// NewWeaviateActionsPatchParams creates a new WeaviateActionsPatchParams object
// with the default values initialized.
func NewWeaviateActionsPatchParams() WeaviateActionsPatchParams {
	var ()
	return WeaviateActionsPatchParams{}
}

// WeaviateActionsPatchParams contains all the bound params for the weaviate actions patch operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.actions.patch
type WeaviateActionsPatchParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request

	/*Unique ID of the action.
	  Required: true
	  In: path
	*/
	ActionID strfmt.UUID
	/*JSONPatch document as defined by RFC 6902.
	  Required: true
	  In: body
	*/
	Body []*models.PatchDocument
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *WeaviateActionsPatchParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	rActionID, rhkActionID, _ := route.Params.GetOK("actionId")
	if err := o.bindActionID(rActionID, rhkActionID, route.Formats); err != nil {
		res = append(res, err)
	}

	if runtime.HasBody(r) {
		defer r.Body.Close()
		var body []*models.PatchDocument
		if err := route.Consumer.Consume(r.Body, &body); err != nil {
			if err == io.EOF {
				res = append(res, errors.Required("body", "body"))
			} else {
				res = append(res, errors.NewParseError("body", "body", "", err))
			}

		} else {
			for _, io := range body {
				if err := io.Validate(route.Formats); err != nil {
					res = append(res, err)
					break
				}
			}

			if len(res) == 0 {
				o.Body = body
			}
		}

	} else {
		res = append(res, errors.Required("body", "body"))
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateActionsPatchParams) bindActionID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	value, err := formats.Parse("uuid", raw)
	if err != nil {
		return errors.InvalidType("actionId", "path", "strfmt.UUID", raw)
	}
	o.ActionID = *(value.(*strfmt.UUID))

	return nil
}
