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
 package locations




import (
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"
)

// NewWeaviateLocationsListParams creates a new WeaviateLocationsListParams object
// with the default values initialized.
func NewWeaviateLocationsListParams() WeaviateLocationsListParams {
	var ()
	return WeaviateLocationsListParams{}
}

// WeaviateLocationsListParams contains all the bound params for the weaviate locations list operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.locations.list
type WeaviateLocationsListParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *WeaviateLocationsListParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
