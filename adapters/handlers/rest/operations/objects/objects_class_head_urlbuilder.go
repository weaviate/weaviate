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

// ObjectsClassHeadURL generates an URL for the objects class head operation
type ObjectsClassHeadURL struct {
	ClassName string
	ID        strfmt.UUID

	_basePath string
	// avoid unkeyed usage
	_ struct{}
}

// WithBasePath sets the base path for this url builder, only required when it's different from the
// base path specified in the swagger spec.
// When the value of the base path is an empty string
func (o *ObjectsClassHeadURL) WithBasePath(bp string) *ObjectsClassHeadURL {
	o.SetBasePath(bp)
	return o
}

// SetBasePath sets the base path for this url builder, only required when it's different from the
// base path specified in the swagger spec.
// When the value of the base path is an empty string
func (o *ObjectsClassHeadURL) SetBasePath(bp string) {
	o._basePath = bp
}

// Build a url path and query string
func (o *ObjectsClassHeadURL) Build() (*url.URL, error) {
	var _result url.URL

	var _path = "/objects/{className}/{id}"

	className := o.ClassName
	if className != "" {
		_path = strings.Replace(_path, "{className}", className, -1)
	} else {
		return nil, errors.New("className is required on ObjectsClassHeadURL")
	}

	id := o.ID.String()
	if id != "" {
		_path = strings.Replace(_path, "{id}", id, -1)
	} else {
		return nil, errors.New("id is required on ObjectsClassHeadURL")
	}

	_basePath := o._basePath
	if _basePath == "" {
		_basePath = "/v1"
	}
	_result.Path = golangswaggerpaths.Join(_basePath, _path)

	return &_result, nil
}

// Must is a helper function to panic when the url builder returns an error
func (o *ObjectsClassHeadURL) Must(u *url.URL, err error) *url.URL {
	if err != nil {
		panic(err)
	}
	if u == nil {
		panic("url can't be nil")
	}
	return u
}

// String returns the string representation of the path with query string
func (o *ObjectsClassHeadURL) String() string {
	return o.Must(o.Build()).String()
}

// BuildFull builds a full url with scheme, host, path and query string
func (o *ObjectsClassHeadURL) BuildFull(scheme, host string) (*url.URL, error) {
	if scheme == "" {
		return nil, errors.New("scheme is required for a full url on ObjectsClassHeadURL")
	}
	if host == "" {
		return nil, errors.New("host is required for a full url on ObjectsClassHeadURL")
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
func (o *ObjectsClassHeadURL) StringFull(scheme, host string) string {
	return o.Must(o.BuildFull(scheme, host)).String()
}
