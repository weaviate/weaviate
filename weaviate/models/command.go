package models




import (
	"encoding/json"

	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/validate"
)

// Command command
// swagger:model Command
type Command struct {

	// Blob parameters list.
	BlobParameters JSONObject `json:"blobParameters,omitempty"`

	// Blob results list.
	BlobResults JSONObject `json:"blobResults,omitempty"`

	// Component name paths separated by '/'.
	Component string `json:"component,omitempty"`

	// Timestamp since epoch of a creation of a command.
	CreationTimeMs int64 `json:"creationTimeMs,omitempty"`

	// User that created the command (not applicable if the user is deleted).
	CreatorEmail string `json:"creatorEmail,omitempty"`

	// Device ID that this command belongs to.
	DeviceID string `json:"deviceId,omitempty"`

	// error
	Error *CommandError `json:"error,omitempty"`

	// Timestamp since epoch of command expiration.
	ExpirationTimeMs int64 `json:"expirationTimeMs,omitempty"`

	// Expiration timeout for the command since its creation, 10 seconds min, 30 days max.
	ExpirationTimeoutMs int64 `json:"expirationTimeoutMs,omitempty"`

	// Unique command ID.
	ID string `json:"id,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#command".
	Kind *string `json:"kind,omitempty"`

	// Full command name, including trait.
	Name string `json:"name,omitempty"`

	// Parameters list.
	Parameters JSONObject `json:"parameters,omitempty"`

	// Command-specific progress descriptor.
	Progress JSONObject `json:"progress,omitempty"`

	// Results list.
	Results JSONObject `json:"results,omitempty"`

	// Current command state.
	State string `json:"state,omitempty"`

	// Pending command state that is not acknowledged by the device yet.
	UserAction string `json:"userAction,omitempty"`
}

// Validate validates this command
func (m *Command) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateError(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateState(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Command) validateError(formats strfmt.Registry) error {

	if swag.IsZero(m.Error) { // not required
		return nil
	}

	if m.Error != nil {

		if err := m.Error.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}

var commandTypeStatePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["aborted","cancelled","done","error","expired","inProgress","queued"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		commandTypeStatePropEnum = append(commandTypeStatePropEnum, v)
	}
}

const (
	commandStateAborted    string = "aborted"
	commandStateCancelled  string = "cancelled"
	commandStateDone       string = "done"
	commandStateError      string = "error"
	commandStateExpired    string = "expired"
	commandStateInProgress string = "inProgress"
	commandStateQueued     string = "queued"
)

// prop value enum
func (m *Command) validateStateEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, commandTypeStatePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *Command) validateState(formats strfmt.Registry) error {

	if swag.IsZero(m.State) { // not required
		return nil
	}

	// value enum
	if err := m.validateStateEnum("state", "body", m.State); err != nil {
		return err
	}

	return nil
}

// CommandError Error descriptor.
// swagger:model CommandError
type CommandError struct {

	// Positional error arguments used for error message formatting.
	Arguments []string `json:"arguments"`

	// Error code.
	Code string `json:"code,omitempty"`

	// User-visible error message populated by the cloud based on command name and error code.
	Message string `json:"message,omitempty"`
}

// Validate validates this command error
func (m *CommandError) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateArguments(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *CommandError) validateArguments(formats strfmt.Registry) error {

	if swag.IsZero(m.Arguments) { // not required
		return nil
	}

	return nil
}
