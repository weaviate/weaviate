package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"encoding/json"
	"strconv"

	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/validate"
)

// ACLEntry Acl entry
// swagger:model AclEntry
type ACLEntry struct {

	// Indicates whether the AclEntry has been revoked from the cloud and the user has no cloud access, but they still might have local auth tokens that are valid and can access the device and execute commands locally. See localAccessInfo for local auth details.
	CloudAccessRevoked bool `json:"cloudAccessRevoked,omitempty"`

	// User who created this entry. At the moment it is populated only when pending == true.
	CreatorEmail string `json:"creatorEmail,omitempty"`

	// User on behalf of whom the access is granted to the application.
	Delegator string `json:"delegator,omitempty"`

	// Unique ACL entry ID.
	ID string `json:"id,omitempty"`

	// Public access key value. Set only when scopeType is PUBLIC.
	Key int64 `json:"key,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#aclEntry".
	Kind *string `json:"kind,omitempty"`

	// Information about local auth tokens timestamps.
	LocalAccessInfo *LocalAccessInfo `json:"localAccessInfo,omitempty"`

	// Whether this ACL entry is pending for user reply to accept/reject it.
	Pending bool `json:"pending,omitempty"`

	// Set of access privileges granted for this scope.
	//
	// Valid values are:
	// - "modifyAcl"
	// - "viewAllEvents"
	Privileges []string `json:"privileges"`

	// Time in milliseconds since Unix Epoch indicating when the AclEntry was revoked.
	RevocationTimeMs int64 `json:"revocationTimeMs,omitempty"`

	// Access role granted to this scope.
	Role string `json:"role,omitempty"`

	// Email address if scope type is user or group, domain name if scope type is a domain.
	ScopeID string `json:"scopeId,omitempty"`

	// Type of membership the user has in the scope.
	ScopeMembership string `json:"scopeMembership,omitempty"`

	// Displayable scope name.
	ScopeName string `json:"scopeName,omitempty"`

	// URL of this scope displayable photo.
	ScopePhotoURL string `json:"scopePhotoUrl,omitempty"`

	// Type of the access scope.
	ScopeType string `json:"scopeType,omitempty"`
}

// Validate validates this Acl entry
func (m *ACLEntry) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateLocalAccessInfo(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validatePrivileges(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateRole(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateScopeMembership(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateScopeType(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *ACLEntry) validateLocalAccessInfo(formats strfmt.Registry) error {

	if swag.IsZero(m.LocalAccessInfo) { // not required
		return nil
	}

	if m.LocalAccessInfo != nil {

		if err := m.LocalAccessInfo.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}

var aclEntryPrivilegesItemsEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["modifyAcl","viewAllEvents"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		aclEntryPrivilegesItemsEnum = append(aclEntryPrivilegesItemsEnum, v)
	}
}

func (m *ACLEntry) validatePrivilegesItemsEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, aclEntryPrivilegesItemsEnum); err != nil {
		return err
	}
	return nil
}

func (m *ACLEntry) validatePrivileges(formats strfmt.Registry) error {

	if swag.IsZero(m.Privileges) { // not required
		return nil
	}

	for i := 0; i < len(m.Privileges); i++ {

		// value enum
		if err := m.validatePrivilegesItemsEnum("privileges"+"."+strconv.Itoa(i), "body", m.Privileges[i]); err != nil {
			return err
		}

	}

	return nil
}

var aclEntryTypeRolePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["manager","owner","robot","user","viewer"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		aclEntryTypeRolePropEnum = append(aclEntryTypeRolePropEnum, v)
	}
}

const (
	aclEntryRoleManager string = "manager"
	aclEntryRoleOwner   string = "owner"
	aclEntryRoleRobot   string = "robot"
	aclEntryRoleUser    string = "user"
	aclEntryRoleViewer  string = "viewer"
)

// prop value enum
func (m *ACLEntry) validateRoleEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, aclEntryTypeRolePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *ACLEntry) validateRole(formats strfmt.Registry) error {

	if swag.IsZero(m.Role) { // not required
		return nil
	}

	// value enum
	if err := m.validateRoleEnum("role", "body", m.Role); err != nil {
		return err
	}

	return nil
}

var aclEntryTypeScopeMembershipPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["delegator","manager","member","none"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		aclEntryTypeScopeMembershipPropEnum = append(aclEntryTypeScopeMembershipPropEnum, v)
	}
}

const (
	aclEntryScopeMembershipDelegator string = "delegator"
	aclEntryScopeMembershipManager   string = "manager"
	aclEntryScopeMembershipMember    string = "member"
	aclEntryScopeMembershipNone      string = "none"
)

// prop value enum
func (m *ACLEntry) validateScopeMembershipEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, aclEntryTypeScopeMembershipPropEnum); err != nil {
		return err
	}
	return nil
}

func (m *ACLEntry) validateScopeMembership(formats strfmt.Registry) error {

	if swag.IsZero(m.ScopeMembership) { // not required
		return nil
	}

	// value enum
	if err := m.validateScopeMembershipEnum("scopeMembership", "body", m.ScopeMembership); err != nil {
		return err
	}

	return nil
}

var aclEntryTypeScopeTypePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["application","domain","group","public","user"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		aclEntryTypeScopeTypePropEnum = append(aclEntryTypeScopeTypePropEnum, v)
	}
}

const (
	aclEntryScopeTypeApplication string = "application"
	aclEntryScopeTypeDomain      string = "domain"
	aclEntryScopeTypeGroup       string = "group"
	aclEntryScopeTypePublic      string = "public"
	aclEntryScopeTypeUser        string = "user"
)

// prop value enum
func (m *ACLEntry) validateScopeTypeEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, aclEntryTypeScopeTypePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *ACLEntry) validateScopeType(formats strfmt.Registry) error {

	if swag.IsZero(m.ScopeType) { // not required
		return nil
	}

	// value enum
	if err := m.validateScopeTypeEnum("scopeType", "body", m.ScopeType); err != nil {
		return err
	}

	return nil
}
