//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"errors"
	"net/url"
	golangswaggerpaths "path"
	"strings"

	"github.com/go-openapi/strfmt"
)

// ObjectsClassReferencesPutURL generates an URL for the objects class references put operation
type ObjectsClassReferencesPutURL struct {
	ClassName    string
	ID           strfmt.UUID
	PropertyName string

	ConsistencyLevel *string
	TenantKey        *string

	_basePath string
	// avoid unkeyed usage
	_ struct{}
}

// WithBasePath sets the base path for this url builder, only required when it's different from the
// base path specified in the swagger spec.
// When the value of the base path is an empty string
func (o *ObjectsClassReferencesPutURL) WithBasePath(bp string) *ObjectsClassReferencesPutURL {
	o.SetBasePath(bp)
	return o
}

// SetBasePath sets the base path for this url builder, only required when it's different from the
// base path specified in the swagger spec.
// When the value of the base path is an empty string
func (o *ObjectsClassReferencesPutURL) SetBasePath(bp string) {
	o._basePath = bp
}

// Build a url path and query string
func (o *ObjectsClassReferencesPutURL) Build() (*url.URL, error) {
	var _result url.URL

	var _path = "/objects/{className}/{id}/references/{propertyName}"

	className := o.ClassName
	if className != "" {
		_path = strings.Replace(_path, "{className}", className, -1)
	} else {
		return nil, errors.New("className is required on ObjectsClassReferencesPutURL")
	}

	id := o.ID.String()
	if id != "" {
		_path = strings.Replace(_path, "{id}", id, -1)
	} else {
		return nil, errors.New("id is required on ObjectsClassReferencesPutURL")
	}

	propertyName := o.PropertyName
	if propertyName != "" {
		_path = strings.Replace(_path, "{propertyName}", propertyName, -1)
	} else {
		return nil, errors.New("propertyName is required on ObjectsClassReferencesPutURL")
	}

	_basePath := o._basePath
	if _basePath == "" {
		_basePath = "/v1"
	}
	_result.Path = golangswaggerpaths.Join(_basePath, _path)

	qs := make(url.Values)

	var consistencyLevelQ string
	if o.ConsistencyLevel != nil {
		consistencyLevelQ = *o.ConsistencyLevel
	}
	if consistencyLevelQ != "" {
		qs.Set("consistency_level", consistencyLevelQ)
	}

	var tenantKeyQ string
	if o.TenantKey != nil {
		tenantKeyQ = *o.TenantKey
	}
	if tenantKeyQ != "" {
		qs.Set("tenant_key", tenantKeyQ)
	}

	_result.RawQuery = qs.Encode()

	return &_result, nil
}

// Must is a helper function to panic when the url builder returns an error
func (o *ObjectsClassReferencesPutURL) Must(u *url.URL, err error) *url.URL {
	if err != nil {
		panic(err)
	}
	if u == nil {
		panic("url can't be nil")
	}
	return u
}

// String returns the string representation of the path with query string
func (o *ObjectsClassReferencesPutURL) String() string {
	return o.Must(o.Build()).String()
}

// BuildFull builds a full url with scheme, host, path and query string
func (o *ObjectsClassReferencesPutURL) BuildFull(scheme, host string) (*url.URL, error) {
	if scheme == "" {
		return nil, errors.New("scheme is required for a full url on ObjectsClassReferencesPutURL")
	}
	if host == "" {
		return nil, errors.New("host is required for a full url on ObjectsClassReferencesPutURL")
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
func (o *ObjectsClassReferencesPutURL) StringFull(scheme, host string) string {
	return o.Must(o.BuildFull(scheme, host)).String()
}
