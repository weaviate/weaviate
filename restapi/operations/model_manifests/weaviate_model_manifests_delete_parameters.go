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
 package model_manifests




import (
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"

	strfmt "github.com/go-openapi/strfmt"
)

// NewWeaviateModelManifestsDeleteParams creates a new WeaviateModelManifestsDeleteParams object
// with the default values initialized.
func NewWeaviateModelManifestsDeleteParams() WeaviateModelManifestsDeleteParams {
	var ()
	return WeaviateModelManifestsDeleteParams{}
}

// WeaviateModelManifestsDeleteParams contains all the bound params for the weaviate model manifests delete operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.modelManifests.delete
type WeaviateModelManifestsDeleteParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request

	/*Unique ID of the model manifest.
	  Required: true
	  In: path
	*/
	ModelManifestID string
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *WeaviateModelManifestsDeleteParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	rModelManifestID, rhkModelManifestID, _ := route.Params.GetOK("modelManifestId")
	if err := o.bindModelManifestID(rModelManifestID, rhkModelManifestID, route.Formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateModelManifestsDeleteParams) bindModelManifestID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	o.ModelManifestID = raw

	return nil
}
