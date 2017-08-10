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
   

package keys

 
 

import (
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"

	strfmt "github.com/go-openapi/strfmt"
)

// NewWeaviateKeysGetParams creates a new WeaviateKeysGetParams object
// with the default values initialized.
func NewWeaviateKeysGetParams() WeaviateKeysGetParams {
	var ()
	return WeaviateKeysGetParams{}
}

// WeaviateKeysGetParams contains all the bound params for the weaviate keys get operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.keys.get
type WeaviateKeysGetParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request

	/*Unique ID of the key.
	  Required: true
	  In: path
	*/
	KeyID strfmt.UUID
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *WeaviateKeysGetParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	rKeyID, rhkKeyID, _ := route.Params.GetOK("keyId")
	if err := o.bindKeyID(rKeyID, rhkKeyID, route.Formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateKeysGetParams) bindKeyID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	value, err := formats.Parse("uuid", raw)
	if err != nil {
		return errors.InvalidType("keyId", "path", "strfmt.UUID", raw)
	}
	o.KeyID = *(value.(*strfmt.UUID))

	return nil
}
