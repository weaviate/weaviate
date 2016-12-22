package personalized_infos


// Editing this file might prove futile when you re-run the generate command

import (
	"errors"
	"net/url"
	"strings"

	"github.com/go-openapi/swag"
)

// WeavePersonalizedInfosPatchURL generates an URL for the weave personalized infos patch operation
type WeavePersonalizedInfosPatchURL struct {
	DeviceID           string
	PersonalizedInfoID string

	Alt         *string
	Fields      *string
	Hl          *string
	Key         *string
	OauthToken  *string
	PrettyPrint *bool
	QuotaUser   *string
	UserIP      *string

	// avoid unkeyed usage
	_ struct{}
}

// Build a url path and query string
func (o *WeavePersonalizedInfosPatchURL) Build() (*url.URL, error) {
	var result url.URL

	var _path = "/devices/{deviceId}/personalizedInfos/{personalizedInfoId}"

	deviceID := o.DeviceID
	if deviceID != "" {
		_path = strings.Replace(_path, "{deviceId}", deviceID, -1)
	} else {
		return nil, errors.New("DeviceID is required on WeavePersonalizedInfosPatchURL")
	}
	personalizedInfoID := o.PersonalizedInfoID
	if personalizedInfoID != "" {
		_path = strings.Replace(_path, "{personalizedInfoId}", personalizedInfoID, -1)
	} else {
		return nil, errors.New("PersonalizedInfoID is required on WeavePersonalizedInfosPatchURL")
	}
	result.Path = _path

	qs := make(url.Values)

	var alt string
	if o.Alt != nil {
		alt = *o.Alt
	}
	if alt != "" {
		qs.Set("alt", alt)
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

	var oauthToken string
	if o.OauthToken != nil {
		oauthToken = *o.OauthToken
	}
	if oauthToken != "" {
		qs.Set("oauth_token", oauthToken)
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
func (o *WeavePersonalizedInfosPatchURL) Must(u *url.URL, err error) *url.URL {
	if err != nil {
		panic(err)
	}
	if u == nil {
		panic("url can't be nil")
	}
	return u
}

// String returns the string representation of the path with query string
func (o *WeavePersonalizedInfosPatchURL) String() string {
	return o.Must(o.Build()).String()
}

// BuildFull builds a full url with scheme, host, path and query string
func (o *WeavePersonalizedInfosPatchURL) BuildFull(scheme, host string) (*url.URL, error) {
	if scheme == "" {
		return nil, errors.New("scheme is required for a full url on WeavePersonalizedInfosPatchURL")
	}
	if host == "" {
		return nil, errors.New("host is required for a full url on WeavePersonalizedInfosPatchURL")
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
func (o *WeavePersonalizedInfosPatchURL) StringFull(scheme, host string) string {
	return o.Must(o.BuildFull(scheme, host)).String()
}
