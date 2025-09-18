// Package multitenancy provides tenant validation strategies for Weaviate collections
// based on their multi-tenancy configuration. It ensures proper tenant handling
// for both single-tenant and multi-tenant collections.
package multitenancy

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/objects"
)

// schemaReader abstracts schema operations required for tenant validation.
// It provides access to tenant status information and class metadata
// needed to validate tenant requests against the current schema state.
type schemaReader interface {
	// TenantsShards returns a map of tenant names to their activity status
	// for the specified class. Used for bulk tenant validation.
	TenantsShards(ctx context.Context, className string, tenants ...string) (map[string]string, error)

	// ReadOnlyClass returns the class definition for the given class name.
	// Returns nil if the class does not exist in the schema.
	ReadOnlyClass(className string) *models.Class
}

// singleTenantValidator validates requests for collections with multi-tenancy disabled.
// It ensures that no tenant parameters are provided in requests to single-tenant
// collections, maintaining strict separation between single and multi-tenant modes.
type singleTenantValidator struct {
	className string
}

// newSingleTenantValidator creates a validator for single-tenant collections.
// The returned validator will reject any requests that include tenant parameters.
//
// Parameters:
//   - className: the name of the class this validator will validate against
//
// Returns a configured singleTenantValidator.
func newSingleTenantValidator(className string) *singleTenantValidator {
	return &singleTenantValidator{
		className: className,
	}
}

// ValidateTenants validates tenant parameters for single-tenant collections.
//
// For single-tenant collections, any non-empty tenant parameter is invalid and
// will result in a multi-tenancy error. This method accepts variadic tenant
// parameters for flexible usage.
//
// Parameters:
//   - ctx: request context (unused but maintained for interface compatibility)
//   - tenants: variadic tenant names to validate
//
// Returns an error if any tenant is non-empty, nil if all tenants are empty.
func (v *singleTenantValidator) ValidateTenants(ctx context.Context, tenants ...string) error {
	for _, tenant := range tenants {
		if tenant != "" {
			return objects.NewErrMultiTenancy(
				fmt.Errorf("class %s has multi-tenancy disabled, but request was with tenant", v.className),
			)
		}
	}
	return nil
}

// multiTenantValidator validates requests for collections with multi-tenancy enabled.
// It ensures tenant parameters are provided and validates that tenants exist in the
// schema and are in active (HOT) status. Inactive tenants are rejected to prevent
// operations on cold or frozen tenant data.
type multiTenantValidator struct {
	className    string
	schemaReader schemaReader
}

// newMultiTenantValidator creates a validator for multi-tenant collections.
// The returned validator will check the tenant presence, existence, and activity status.
//
// Parameters:
//   - className: the name of the class this validator will validate against
//   - schemaReader: provides access to schema and tenant status information
//
// Returns a configured multiTenantValidator.
func newMultiTenantValidator(className string, schemaReader schemaReader) *multiTenantValidator {
	return &multiTenantValidator{
		className:    className,
		schemaReader: schemaReader,
	}
}

// ValidateTenants validates tenant parameters for multi-tenant collections.
//
// This method performs a complete validation chain for all provided tenants:
// 1. Verifies all tenant parameters are non-empty
// 2. Fetches tenant status from the schema in a single bulk operation
// 3. Validates each tenant exists and is in active (HOT) status
//
// The method handles status fetching internally to optimize performance for batch
// operations while maintaining a clean interface for callers.
//
// Parameters:
//   - ctx: request context for schema operations
//   - tenants: variadic tenant names to validate
//
// Returns an error on the first validation failure, nil if all tenants are valid.
func (v *multiTenantValidator) ValidateTenants(ctx context.Context, tenants ...string) error {
	if len(tenants) == 0 {
		return objects.NewErrMultiTenancy(fmt.Errorf(
			"class %s has multi-tenancy enabled, but request was without tenant", v.className,
		))
	}
	for _, tenant := range tenants {
		if tenant == "" {
			return objects.NewErrMultiTenancy(fmt.Errorf(
				"class %s has multi-tenancy enabled, but request was without tenant", v.className,
			))
		}
	}

	tenants = deduplicateTenants(tenants)

	statusMap, err := v.schemaReader.TenantsShards(ctx, v.className, tenants...)
	if err != nil {
		return fmt.Errorf("fetch tenant status for class %q: %w", v.className, err)
	}

	for _, tenant := range tenants {
		status, ok := statusMap[tenant]
		if !ok {
			// if no status is found, the tenant doesn't exist' and is handled as a missing tenant
			status = ""
		}
		if err := v.validateTenantStatus(tenant, status); err != nil {
			return err
		}
	}
	return nil
}

// validateTenantStatus validates a tenant's status and existence.
//
// This internal method handles the business logic for tenant status validation:
// - If status is empty, the tenant doesn't exist in the schema
// - If status is not HOT, the tenant is inactive and unavailable
//
// For missing tenants, it checks if the class itself exists to provide
// more specific error messages (preserving compatibility with legacy code and backward	compatibility).
//
// Parameters:
//   - tenant: the tenant name being validated
//   - status: the tenant's current activity status from the schema
//
// Returns a multi-tenancy error for invalid tenants, nil for valid tenants.
func (v *multiTenantValidator) validateTenantStatus(tenant, status string) error {
	if status == "" {
		class := v.schemaReader.ReadOnlyClass(v.className)
		if class == nil {
			return fmt.Errorf("class %q not found in schema", v.className)
		}
		return objects.NewErrMultiTenancy(fmt.Errorf("%w: %q", errors.ErrTenantNotFound, tenant))
	}

	if status != models.TenantActivityStatusHOT {
		return objects.NewErrMultiTenancy(fmt.Errorf("%w: '%s'", errors.ErrTenantNotActive, tenant))
	}
	return nil
}

// deduplicateTenants returns tenants with duplicates removed.
func deduplicateTenants(tenants []string) []string {
	if len(tenants) <= 1 {
		return tenants
	}
	seen := make(map[string]struct{}, len(tenants))
	uniqueTenants := make([]string, 0, len(tenants))
	for _, tenant := range tenants {
		if _, ok := seen[tenant]; ok {
			continue
		}
		seen[tenant] = struct{}{}
		uniqueTenants = append(uniqueTenants, tenant)
	}
	return uniqueTenants
}

// TenantValidator provides a unified interface for tenant validation regardless
// of the collection's multi-tenancy configuration. It delegates to the appropriate
// validation strategy based on the collection's settings.
type TenantValidator struct {
	validateTenants func(ctx context.Context, tenants ...string) error
}

// ValidateTenants validates the provided tenant parameters according to the
// collection's multi-tenancy configuration. This method provides a consistent
// interface for both single-tenant and multi-tenant validation strategies.
//
// Parameters:
//   - ctx: request context for validation operations
//   - tenants: variadic tenant names to validate
//
// Returns an error if validation fails, nil if all tenants are valid.
func (v *TenantValidator) ValidateTenants(ctx context.Context, tenants ...string) error {
	return v.validateTenants(ctx, tenants...)
}

// Builder constructs TenantValidator instances based on collection multi-tenancy settings.
// It selects the appropriate validation strategy (single-tenant or multi-tenant) and
// encapsulates the strategy selection logic.
type Builder struct {
	className           string
	multiTenancyEnabled bool
	schemaReader        schemaReader
}

// NewBuilder creates a new Builder for constructing TenantValidator instances.
//
// Parameters:
//   - className: the name of the class to validate tenants for
//   - multiTenancyEnabled: whether the class has multi-tenancy enabled
//   - schemaReader: provides access to schema and tenant information
//
// Returns a configured Builder.
func NewBuilder(className string, multiTenancyEnabled bool, schemaReader schemaReader) *Builder {
	return &Builder{
		className:           className,
		multiTenancyEnabled: multiTenancyEnabled,
		schemaReader:        schemaReader,
	}
}

// Build constructs a TenantValidator with the appropriate validation strategy
// based on the collection's multi-tenancy configuration.
//
// Returns a configured TenantValidator that uses either single-tenant or
// multi-tenant validation strategy as appropriate.
func (b *Builder) Build() *TenantValidator {
	if b.multiTenancyEnabled {
		validator := newMultiTenantValidator(b.className, b.schemaReader)
		return &TenantValidator{
			validateTenants: validator.ValidateTenants,
		}
	}
	validator := newSingleTenantValidator(b.className)
	return &TenantValidator{
		validateTenants: validator.ValidateTenants,
	}
}

// validatorStrategy defines the interface that all tenant validation strategies must implement.
// This interface ensures consistency across different validation approaches.
type validatorStrategy interface {
	ValidateTenants(ctx context.Context, tenants ...string) error
}

// Interface compliance checks at compile time.
var (
	_ validatorStrategy = (*singleTenantValidator)(nil)
	_ validatorStrategy = (*multiTenantValidator)(nil)
)
