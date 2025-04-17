//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package vectorization

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"errors"
	"net/url"
	golangswaggerpaths "path"
	"strings"
)

// VectorizationGetStatusURL generates an URL for the vectorization get status operation
type VectorizationGetStatusURL struct {
	CollectionName string
	TargetVector   string

	_basePath string
	// avoid unkeyed usage
	_ struct{}
}

// WithBasePath sets the base path for this url builder, only required when it's different from the
// base path specified in the swagger spec.
// When the value of the base path is an empty string
func (o *VectorizationGetStatusURL) WithBasePath(bp string) *VectorizationGetStatusURL {
	o.SetBasePath(bp)
	return o
}

// SetBasePath sets the base path for this url builder, only required when it's different from the
// base path specified in the swagger spec.
// When the value of the base path is an empty string
func (o *VectorizationGetStatusURL) SetBasePath(bp string) {
	o._basePath = bp
}

// Build a url path and query string
func (o *VectorizationGetStatusURL) Build() (*url.URL, error) {
	var _result url.URL

	var _path = "/schema/{collectionName}/vectorize/{targetVector}"

	collectionName := o.CollectionName
	if collectionName != "" {
		_path = strings.Replace(_path, "{collectionName}", collectionName, -1)
	} else {
		return nil, errors.New("collectionName is required on VectorizationGetStatusURL")
	}

	targetVector := o.TargetVector
	if targetVector != "" {
		_path = strings.Replace(_path, "{targetVector}", targetVector, -1)
	} else {
		return nil, errors.New("targetVector is required on VectorizationGetStatusURL")
	}

	_basePath := o._basePath
	if _basePath == "" {
		_basePath = "/v1"
	}
	_result.Path = golangswaggerpaths.Join(_basePath, _path)

	return &_result, nil
}

// Must is a helper function to panic when the url builder returns an error
func (o *VectorizationGetStatusURL) Must(u *url.URL, err error) *url.URL {
	if err != nil {
		panic(err)
	}
	if u == nil {
		panic("url can't be nil")
	}
	return u
}

// String returns the string representation of the path with query string
func (o *VectorizationGetStatusURL) String() string {
	return o.Must(o.Build()).String()
}

// BuildFull builds a full url with scheme, host, path and query string
func (o *VectorizationGetStatusURL) BuildFull(scheme, host string) (*url.URL, error) {
	if scheme == "" {
		return nil, errors.New("scheme is required for a full url on VectorizationGetStatusURL")
	}
	if host == "" {
		return nil, errors.New("host is required for a full url on VectorizationGetStatusURL")
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
func (o *VectorizationGetStatusURL) StringFull(scheme, host string) string {
	return o.Must(o.BuildFull(scheme, host)).String()
}
