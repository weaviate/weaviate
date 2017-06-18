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
 package events


// Editing this file might prove futile when you re-run the generate command

import (
	"errors"
	"net/url"
	golangswaggerpaths "path"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// WeaviateGroupsEventsListURL generates an URL for the weaviate groups events list operation
type WeaviateGroupsEventsListURL struct {
	GroupID strfmt.UUID

	MaxResults *int64
	Page       *int64

	_basePath string
	// avoid unkeyed usage
	_ struct{}
}

// WithBasePath sets the base path for this url builder, only required when it's different from the
// base path specified in the swagger spec.
// When the value of the base path is an empty string
func (o *WeaviateGroupsEventsListURL) WithBasePath(bp string) *WeaviateGroupsEventsListURL {
	o.SetBasePath(bp)
	return o
}

// SetBasePath sets the base path for this url builder, only required when it's different from the
// base path specified in the swagger spec.
// When the value of the base path is an empty string
func (o *WeaviateGroupsEventsListURL) SetBasePath(bp string) {
	o._basePath = bp
}

// Build a url path and query string
func (o *WeaviateGroupsEventsListURL) Build() (*url.URL, error) {
	var result url.URL

	var _path = "/groups/{groupId}/events"

	groupID := o.GroupID.String()
	if groupID != "" {
		_path = strings.Replace(_path, "{groupId}", groupID, -1)
	} else {
		return nil, errors.New("GroupID is required on WeaviateGroupsEventsListURL")
	}
	_basePath := o._basePath
	if _basePath == "" {
		_basePath = "/weaviate/v1"
	}
	result.Path = golangswaggerpaths.Join(_basePath, _path)

	qs := make(url.Values)

	var maxResults string
	if o.MaxResults != nil {
		maxResults = swag.FormatInt64(*o.MaxResults)
	}
	if maxResults != "" {
		qs.Set("maxResults", maxResults)
	}

	var page string
	if o.Page != nil {
		page = swag.FormatInt64(*o.Page)
	}
	if page != "" {
		qs.Set("page", page)
	}

	result.RawQuery = qs.Encode()

	return &result, nil
}

// Must is a helper function to panic when the url builder returns an error
func (o *WeaviateGroupsEventsListURL) Must(u *url.URL, err error) *url.URL {
	if err != nil {
		panic(err)
	}
	if u == nil {
		panic("url can't be nil")
	}
	return u
}

// String returns the string representation of the path with query string
func (o *WeaviateGroupsEventsListURL) String() string {
	return o.Must(o.Build()).String()
}

// BuildFull builds a full url with scheme, host, path and query string
func (o *WeaviateGroupsEventsListURL) BuildFull(scheme, host string) (*url.URL, error) {
	if scheme == "" {
		return nil, errors.New("scheme is required for a full url on WeaviateGroupsEventsListURL")
	}
	if host == "" {
		return nil, errors.New("host is required for a full url on WeaviateGroupsEventsListURL")
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
func (o *WeaviateGroupsEventsListURL) StringFull(scheme, host string) string {
	return o.Must(o.BuildFull(scheme, host)).String()
}
