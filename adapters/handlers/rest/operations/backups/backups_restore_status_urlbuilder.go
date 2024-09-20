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

package backups

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"errors"
	"net/url"
	golangswaggerpaths "path"
	"strings"
)

// BackupsRestoreStatusURL generates an URL for the backups restore status operation
type BackupsRestoreStatusURL struct {
	Backend string
	ID      string

	S3bucket *string
	S3path   *string

	_basePath string
	// avoid unkeyed usage
	_ struct{}
}

// WithBasePath sets the base path for this url builder, only required when it's different from the
// base path specified in the swagger spec.
// When the value of the base path is an empty string
func (o *BackupsRestoreStatusURL) WithBasePath(bp string) *BackupsRestoreStatusURL {
	o.SetBasePath(bp)
	return o
}

// SetBasePath sets the base path for this url builder, only required when it's different from the
// base path specified in the swagger spec.
// When the value of the base path is an empty string
func (o *BackupsRestoreStatusURL) SetBasePath(bp string) {
	o._basePath = bp
}

// Build a url path and query string
func (o *BackupsRestoreStatusURL) Build() (*url.URL, error) {
	var _result url.URL

	var _path = "/backups/{backend}/{id}/restore"

	backend := o.Backend
	if backend != "" {
		_path = strings.Replace(_path, "{backend}", backend, -1)
	} else {
		return nil, errors.New("backend is required on BackupsRestoreStatusURL")
	}

	id := o.ID
	if id != "" {
		_path = strings.Replace(_path, "{id}", id, -1)
	} else {
		return nil, errors.New("id is required on BackupsRestoreStatusURL")
	}

	_basePath := o._basePath
	if _basePath == "" {
		_basePath = "/v1"
	}
	_result.Path = golangswaggerpaths.Join(_basePath, _path)

	qs := make(url.Values)

	var s3bucketQ string
	if o.S3bucket != nil {
		s3bucketQ = *o.S3bucket
	}
	if s3bucketQ != "" {
		qs.Set("s3bucket", s3bucketQ)
	}

	var s3pathQ string
	if o.S3path != nil {
		s3pathQ = *o.S3path
	}
	if s3pathQ != "" {
		qs.Set("s3path", s3pathQ)
	}

	_result.RawQuery = qs.Encode()

	return &_result, nil
}

// Must is a helper function to panic when the url builder returns an error
func (o *BackupsRestoreStatusURL) Must(u *url.URL, err error) *url.URL {
	if err != nil {
		panic(err)
	}
	if u == nil {
		panic("url can't be nil")
	}
	return u
}

// String returns the string representation of the path with query string
func (o *BackupsRestoreStatusURL) String() string {
	return o.Must(o.Build()).String()
}

// BuildFull builds a full url with scheme, host, path and query string
func (o *BackupsRestoreStatusURL) BuildFull(scheme, host string) (*url.URL, error) {
	if scheme == "" {
		return nil, errors.New("scheme is required for a full url on BackupsRestoreStatusURL")
	}
	if host == "" {
		return nil, errors.New("host is required for a full url on BackupsRestoreStatusURL")
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
func (o *BackupsRestoreStatusURL) StringFull(scheme, host string) string {
	return o.Must(o.BuildFull(scheme, host)).String()
}
