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

package p2_p

import (
	"io"
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/creativesoftwarefdn/weaviate/models"
)

// NewWeaviatePeersAnswersCreateParams creates a new WeaviatePeersAnswersCreateParams object
// with the default values initialized.
func NewWeaviatePeersAnswersCreateParams() WeaviatePeersAnswersCreateParams {
	var ()
	return WeaviatePeersAnswersCreateParams{}
}

// WeaviatePeersAnswersCreateParams contains all the bound params for the weaviate peers answers create operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.peers.answers.create
type WeaviatePeersAnswersCreateParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request

	/*The Uuid of the answer.
	  Required: true
	  In: path
	*/
	AnswerID strfmt.UUID
	/*The answer.
	  Required: true
	  In: body
	*/
	Body models.Schema
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *WeaviatePeersAnswersCreateParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	rAnswerID, rhkAnswerID, _ := route.Params.GetOK("answerId")
	if err := o.bindAnswerID(rAnswerID, rhkAnswerID, route.Formats); err != nil {
		res = append(res, err)
	}

	if runtime.HasBody(r) {
		defer r.Body.Close()
		var body models.Schema
		if err := route.Consumer.Consume(r.Body, &body); err != nil {
			if err == io.EOF {
				res = append(res, errors.Required("body", "body"))
			} else {
				res = append(res, errors.NewParseError("body", "body", "", err))
			}

		} else {

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

func (o *WeaviatePeersAnswersCreateParams) bindAnswerID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	value, err := formats.Parse("uuid", raw)
	if err != nil {
		return errors.InvalidType("answerId", "path", "strfmt.UUID", raw)
	}
	o.AnswerID = *(value.(*strfmt.UUID))

	return nil
}
