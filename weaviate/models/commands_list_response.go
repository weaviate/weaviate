package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// CommandsListResponse List of commands.
// swagger:model CommandsListResponse
type CommandsListResponse struct {

	// The actual list of commands.
	Commands []*Command `json:"commands"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#commandsListResponse".
	Kind *string `json:"kind,omitempty"`

	// Token for the next page of commands.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// The total number of commands for the query. The number of items in a response may be smaller due to paging.
	TotalResults int32 `json:"totalResults,omitempty"`
}

// Validate validates this commands list response
func (m *CommandsListResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateCommands(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *CommandsListResponse) validateCommands(formats strfmt.Registry) error {

	if swag.IsZero(m.Commands) { // not required
		return nil
	}

	for i := 0; i < len(m.Commands); i++ {

		if swag.IsZero(m.Commands[i]) { // not required
			continue
		}

		if m.Commands[i] != nil {

			if err := m.Commands[i].Validate(formats); err != nil {
				return err
			}
		}

	}

	return nil
}
