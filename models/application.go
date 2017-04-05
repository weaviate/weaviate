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
 package models




import (
	"encoding/json"

	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/validate"
)

// Application Contains information about a recommended application for a device model.
// swagger:model Application
type Application struct {

	// User readable application description.
	Description string `json:"description,omitempty"`

	// Application icon URL.
	IconURL string `json:"iconUrl,omitempty"`

	// Unique application ID.
	ID string `json:"id,omitempty"`

	// User readable application name.
	Name string `json:"name,omitempty"`

	// Price of the application.
	Price float64 `json:"price,omitempty"`

	// User readable publisher name.
	PublisherName string `json:"publisherName,omitempty"`

	// Application type.
	Type string `json:"type,omitempty"`

	// Application install URL.
	URL string `json:"url,omitempty"`
}

// Validate validates this application
func (m *Application) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateType(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var applicationTypeTypePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["android","chrome","ios","web"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		applicationTypeTypePropEnum = append(applicationTypeTypePropEnum, v)
	}
}

const (
	applicationTypeAndroid string = "android"
	applicationTypeChrome  string = "chrome"
	applicationTypeIos     string = "ios"
	applicationTypeWeb     string = "web"
)

// prop value enum
func (m *Application) validateTypeEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, applicationTypeTypePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *Application) validateType(formats strfmt.Registry) error {

	if swag.IsZero(m.Type) { // not required
		return nil
	}

	// value enum
	if err := m.validateTypeEnum("type", "body", m.Type); err != nil {
		return err
	}

	return nil
}
