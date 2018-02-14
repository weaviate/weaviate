/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */

package keys

// Editing this file might prove futile when you re-run the generate command

import (
	"errors"
	"net/url"
	golangswaggerpaths "path"
	"strings"

	"github.com/go-openapi/strfmt"
)

// WeaviateKeysChildrenGetURL generates an URL for the weaviate keys children get operation
type WeaviateKeysChildrenGetURL struct {
	KeyID strfmt.UUID

	_basePath string
	// avoid unkeyed usage
	_ struct{}
}

// WithBasePath sets the base path for this url builder, only required when it's different from the
// base path specified in the swagger spec.
// When the value of the base path is an empty string
func (o *WeaviateKeysChildrenGetURL) WithBasePath(bp string) *WeaviateKeysChildrenGetURL {
	o.SetBasePath(bp)
	return o
}

// SetBasePath sets the base path for this url builder, only required when it's different from the
// base path specified in the swagger spec.
// When the value of the base path is an empty string
func (o *WeaviateKeysChildrenGetURL) SetBasePath(bp string) {
	o._basePath = bp
}

// Build a url path and query string
func (o *WeaviateKeysChildrenGetURL) Build() (*url.URL, error) {
	var result url.URL

	var _path = "/keys/{keyId}/children"

	keyID := o.KeyID.String()
	if keyID != "" {
		_path = strings.Replace(_path, "{keyId}", keyID, -1)
	} else {
		return nil, errors.New("KeyID is required on WeaviateKeysChildrenGetURL")
	}
	_basePath := o._basePath
	if _basePath == "" {
		_basePath = "/weaviate/v1"
	}
	result.Path = golangswaggerpaths.Join(_basePath, _path)

	return &result, nil
}

// Must is a helper function to panic when the url builder returns an error
func (o *WeaviateKeysChildrenGetURL) Must(u *url.URL, err error) *url.URL {
	if err != nil {
		panic(err)
	}
	if u == nil {
		panic("url can't be nil")
	}
	return u
}

// String returns the string representation of the path with query string
func (o *WeaviateKeysChildrenGetURL) String() string {
	return o.Must(o.Build()).String()
}

// BuildFull builds a full url with scheme, host, path and query string
func (o *WeaviateKeysChildrenGetURL) BuildFull(scheme, host string) (*url.URL, error) {
	if scheme == "" {
		return nil, errors.New("scheme is required for a full url on WeaviateKeysChildrenGetURL")
	}
	if host == "" {
		return nil, errors.New("host is required for a full url on WeaviateKeysChildrenGetURL")
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
func (o *WeaviateKeysChildrenGetURL) StringFull(scheme, host string) string {
	return o.Must(o.BuildFull(scheme, host)).String()
}
