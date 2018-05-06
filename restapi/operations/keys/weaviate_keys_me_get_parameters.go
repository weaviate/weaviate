/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package keys

import (
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"
)

// NewWeaviateKeysMeGetParams creates a new WeaviateKeysMeGetParams object
// with the default values initialized.
func NewWeaviateKeysMeGetParams() WeaviateKeysMeGetParams {
	var ()
	return WeaviateKeysMeGetParams{}
}

// WeaviateKeysMeGetParams contains all the bound params for the weaviate keys me get operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.keys.me.get
type WeaviateKeysMeGetParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *WeaviateKeysMeGetParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
