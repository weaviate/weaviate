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
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"

	strfmt "github.com/go-openapi/strfmt"
)

// NewWeaviateModelManifestsGetParams creates a new WeaviateModelManifestsGetParams object
// with the default values initialized.
func NewWeaviateModelManifestsGetParams() WeaviateModelManifestsGetParams {
	var ()
	return WeaviateModelManifestsGetParams{}
}

// WeaviateModelManifestsGetParams contains all the bound params for the weaviate model manifests get operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.modelManifests.get
type WeaviateModelManifestsGetParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request

	/*Specifies the language code that should be used for text values in the API response.
	  In: query
	*/
	Hl *string
	/*Unique ID of the model manifest.
	  Required: true
	  In: path
	*/
	ModelManifestID string
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *WeaviateModelManifestsGetParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	qs := runtime.Values(r.URL.Query())

	qHl, qhkHl, _ := qs.GetOK("hl")
	if err := o.bindHl(qHl, qhkHl, route.Formats); err != nil {
		res = append(res, err)
	}

	rModelManifestID, rhkModelManifestID, _ := route.Params.GetOK("modelManifestId")
	if err := o.bindModelManifestID(rModelManifestID, rhkModelManifestID, route.Formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateModelManifestsGetParams) bindHl(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		return nil
	}

	o.Hl = &raw

	return nil
}

func (o *WeaviateModelManifestsGetParams) bindModelManifestID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	o.ModelManifestID = raw

	return nil
}
