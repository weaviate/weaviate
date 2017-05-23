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
 package things




import (
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"

	strfmt "github.com/go-openapi/strfmt"
)

// NewWeaviateThingsGetParams creates a new WeaviateThingsGetParams object
// with the default values initialized.
func NewWeaviateThingsGetParams() WeaviateThingsGetParams {
	var (
		altDefault         = string("json")
		prettyPrintDefault = bool(true)
	)
	return WeaviateThingsGetParams{
		Alt: &altDefault,

		PrettyPrint: &prettyPrintDefault,
	}
}

// WeaviateThingsGetParams contains all the bound params for the weaviate things get operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.things.get
type WeaviateThingsGetParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request

	/*Data format for the response.
	  In: query
	  Default: "json"
	*/
	Alt *string
	/*Selector specifying which fields to include in a partial response.
	  In: query
	*/
	Fields *string
	/*Specifies the language code that should be used for text values in the API response.
	  In: query
	*/
	Hl *string
	/*API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token.
	  In: query
	*/
	Key *string
	/*OAuth 2.0 token for the current user.
	  In: query
	*/
	OauthToken *string
	/*Returns response with indentations and line breaks.
	  In: query
	  Default: true
	*/
	PrettyPrint *bool
	/*Projection controls which fields of the Thing resource are returned.
	  In: query
	*/
	Projection *string
	/*Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided.
	  In: query
	*/
	QuotaUser *string
	/*Unique ID of the thing.
	  Required: true
	  In: path
	*/
	ThingID string
	/*IP address of the site where the request originates. Use this if you want to enforce per-user limits.
	  In: query
	*/
	UserIP *string
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *WeaviateThingsGetParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	qs := runtime.Values(r.URL.Query())

	qAlt, qhkAlt, _ := qs.GetOK("alt")
	if err := o.bindAlt(qAlt, qhkAlt, route.Formats); err != nil {
		res = append(res, err)
	}

	qFields, qhkFields, _ := qs.GetOK("fields")
	if err := o.bindFields(qFields, qhkFields, route.Formats); err != nil {
		res = append(res, err)
	}

	qHl, qhkHl, _ := qs.GetOK("hl")
	if err := o.bindHl(qHl, qhkHl, route.Formats); err != nil {
		res = append(res, err)
	}

	qKey, qhkKey, _ := qs.GetOK("key")
	if err := o.bindKey(qKey, qhkKey, route.Formats); err != nil {
		res = append(res, err)
	}

	qOauthToken, qhkOauthToken, _ := qs.GetOK("oauth_token")
	if err := o.bindOauthToken(qOauthToken, qhkOauthToken, route.Formats); err != nil {
		res = append(res, err)
	}

	qPrettyPrint, qhkPrettyPrint, _ := qs.GetOK("prettyPrint")
	if err := o.bindPrettyPrint(qPrettyPrint, qhkPrettyPrint, route.Formats); err != nil {
		res = append(res, err)
	}

	qProjection, qhkProjection, _ := qs.GetOK("projection")
	if err := o.bindProjection(qProjection, qhkProjection, route.Formats); err != nil {
		res = append(res, err)
	}

	qQuotaUser, qhkQuotaUser, _ := qs.GetOK("quotaUser")
	if err := o.bindQuotaUser(qQuotaUser, qhkQuotaUser, route.Formats); err != nil {
		res = append(res, err)
	}

	rThingID, rhkThingID, _ := route.Params.GetOK("thingId")
	if err := o.bindThingID(rThingID, rhkThingID, route.Formats); err != nil {
		res = append(res, err)
	}

	qUserIP, qhkUserIP, _ := qs.GetOK("userIp")
	if err := o.bindUserIP(qUserIP, qhkUserIP, route.Formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateThingsGetParams) bindAlt(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		var altDefault string = string("json")
		o.Alt = &altDefault
		return nil
	}

	o.Alt = &raw

	if err := o.validateAlt(formats); err != nil {
		return err
	}

	return nil
}

func (o *WeaviateThingsGetParams) validateAlt(formats strfmt.Registry) error {

	if err := validate.Enum("alt", "query", *o.Alt, []interface{}{"json"}); err != nil {
		return err
	}

	return nil
}

func (o *WeaviateThingsGetParams) bindFields(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		return nil
	}

	o.Fields = &raw

	return nil
}

func (o *WeaviateThingsGetParams) bindHl(rawData []string, hasKey bool, formats strfmt.Registry) error {
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

func (o *WeaviateThingsGetParams) bindKey(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		return nil
	}

	o.Key = &raw

	return nil
}

func (o *WeaviateThingsGetParams) bindOauthToken(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		return nil
	}

	o.OauthToken = &raw

	return nil
}

func (o *WeaviateThingsGetParams) bindPrettyPrint(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		var prettyPrintDefault bool = bool(true)
		o.PrettyPrint = &prettyPrintDefault
		return nil
	}

	value, err := swag.ConvertBool(raw)
	if err != nil {
		return errors.InvalidType("prettyPrint", "query", "bool", raw)
	}
	o.PrettyPrint = &value

	return nil
}

func (o *WeaviateThingsGetParams) bindProjection(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		return nil
	}

	o.Projection = &raw

	if err := o.validateProjection(formats); err != nil {
		return err
	}

	return nil
}

func (o *WeaviateThingsGetParams) validateProjection(formats strfmt.Registry) error {

	if err := validate.Enum("projection", "query", *o.Projection, []interface{}{"full", "noUserInfo"}); err != nil {
		return err
	}

	return nil
}

func (o *WeaviateThingsGetParams) bindQuotaUser(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		return nil
	}

	o.QuotaUser = &raw

	return nil
}

func (o *WeaviateThingsGetParams) bindThingID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	o.ThingID = raw

	return nil
}

func (o *WeaviateThingsGetParams) bindUserIP(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		return nil
	}

	o.UserIP = &raw

	return nil
}
