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
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package devices




import (
	"errors"
	"net/url"

	"github.com/go-openapi/swag"
)

// WeaveDevicesListURL generates an URL for the weave devices list operation
type WeaveDevicesListURL struct {
	Alt                  *string
	DescriptionSubstring *string
	DeviceKind           *string
	DisplayNameSubstring *string
	Fields               *string
	Hl                   *string
	Key                  *string
	MaxResults           *int64
	ModelManifestID      *string
	NameSubstring        *string
	OauthToken           *string
	PlaceID              *string
	PrettyPrint          *bool
	QuotaUser            *string
	Role                 *string
	RoomID               *string
	StartIndex           *int64
	SystemNameSubstring  *string
	Token                *string
	UserIP               *string

	// avoid unkeyed usage
	_ struct{}
}

// Build a url path and query string
func (o *WeaveDevicesListURL) Build() (*url.URL, error) {
	var result url.URL

	var _path = "/devices"

	result.Path = _path

	qs := make(url.Values)

	var alt string
	if o.Alt != nil {
		alt = *o.Alt
	}
	if alt != "" {
		qs.Set("alt", alt)
	}

	var descriptionSubstring string
	if o.DescriptionSubstring != nil {
		descriptionSubstring = *o.DescriptionSubstring
	}
	if descriptionSubstring != "" {
		qs.Set("descriptionSubstring", descriptionSubstring)
	}

	var deviceKind string
	if o.DeviceKind != nil {
		deviceKind = *o.DeviceKind
	}
	if deviceKind != "" {
		qs.Set("deviceKind", deviceKind)
	}

	var displayNameSubstring string
	if o.DisplayNameSubstring != nil {
		displayNameSubstring = *o.DisplayNameSubstring
	}
	if displayNameSubstring != "" {
		qs.Set("displayNameSubstring", displayNameSubstring)
	}

	var fields string
	if o.Fields != nil {
		fields = *o.Fields
	}
	if fields != "" {
		qs.Set("fields", fields)
	}

	var hl string
	if o.Hl != nil {
		hl = *o.Hl
	}
	if hl != "" {
		qs.Set("hl", hl)
	}

	var key string
	if o.Key != nil {
		key = *o.Key
	}
	if key != "" {
		qs.Set("key", key)
	}

	var maxResults string
	if o.MaxResults != nil {
		maxResults = swag.FormatInt64(*o.MaxResults)
	}
	if maxResults != "" {
		qs.Set("maxResults", maxResults)
	}

	var modelManifestID string
	if o.ModelManifestID != nil {
		modelManifestID = *o.ModelManifestID
	}
	if modelManifestID != "" {
		qs.Set("modelManifestId", modelManifestID)
	}

	var nameSubstring string
	if o.NameSubstring != nil {
		nameSubstring = *o.NameSubstring
	}
	if nameSubstring != "" {
		qs.Set("nameSubstring", nameSubstring)
	}

	var oauthToken string
	if o.OauthToken != nil {
		oauthToken = *o.OauthToken
	}
	if oauthToken != "" {
		qs.Set("oauth_token", oauthToken)
	}

	var placeID string
	if o.PlaceID != nil {
		placeID = *o.PlaceID
	}
	if placeID != "" {
		qs.Set("placeId", placeID)
	}

	var prettyPrint string
	if o.PrettyPrint != nil {
		prettyPrint = swag.FormatBool(*o.PrettyPrint)
	}
	if prettyPrint != "" {
		qs.Set("prettyPrint", prettyPrint)
	}

	var quotaUser string
	if o.QuotaUser != nil {
		quotaUser = *o.QuotaUser
	}
	if quotaUser != "" {
		qs.Set("quotaUser", quotaUser)
	}

	var role string
	if o.Role != nil {
		role = *o.Role
	}
	if role != "" {
		qs.Set("role", role)
	}

	var roomID string
	if o.RoomID != nil {
		roomID = *o.RoomID
	}
	if roomID != "" {
		qs.Set("roomId", roomID)
	}

	var startIndex string
	if o.StartIndex != nil {
		startIndex = swag.FormatInt64(*o.StartIndex)
	}
	if startIndex != "" {
		qs.Set("startIndex", startIndex)
	}

	var systemNameSubstring string
	if o.SystemNameSubstring != nil {
		systemNameSubstring = *o.SystemNameSubstring
	}
	if systemNameSubstring != "" {
		qs.Set("systemNameSubstring", systemNameSubstring)
	}

	var token string
	if o.Token != nil {
		token = *o.Token
	}
	if token != "" {
		qs.Set("token", token)
	}

	var userIP string
	if o.UserIP != nil {
		userIP = *o.UserIP
	}
	if userIP != "" {
		qs.Set("userIp", userIP)
	}

	result.RawQuery = qs.Encode()

	return &result, nil
}

// Must is a helper function to panic when the url builder returns an error
func (o *WeaveDevicesListURL) Must(u *url.URL, err error) *url.URL {
	if err != nil {
		panic(err)
	}
	if u == nil {
		panic("url can't be nil")
	}
	return u
}

// String returns the string representation of the path with query string
func (o *WeaveDevicesListURL) String() string {
	return o.Must(o.Build()).String()
}

// BuildFull builds a full url with scheme, host, path and query string
func (o *WeaveDevicesListURL) BuildFull(scheme, host string) (*url.URL, error) {
	if scheme == "" {
		return nil, errors.New("scheme is required for a full url on WeaveDevicesListURL")
	}
	if host == "" {
		return nil, errors.New("host is required for a full url on WeaveDevicesListURL")
	}

	base, err := o.Build()
	if err != nil {
		return nil, err
	}

	base.Scheme = scheme
	base.Host = host
	return base, nil
}

// StringFull returns the string representation of a complete url
func (o *WeaveDevicesListURL) StringFull(scheme, host string) string {
	return o.Must(o.BuildFull(scheme, host)).String()
}
