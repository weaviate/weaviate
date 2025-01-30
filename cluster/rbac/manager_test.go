package rbac

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestUpsertRolesPermissions(t *testing.T) {
	tests := []struct {
		name    string
		request *cmd.ApplyRequest
		setup   func(*mocks.Controller)
		wantErr bool
	}{
		{
			name: "V0: Successfully update schema domain permissions",
			request: &cmd.ApplyRequest{
				SubCommand: mustMarshal(cmd.CreateRolesRequest{
					Version: cmd.RBACCommandPolicyVersionV0,
					Roles: map[string][]authorization.Policy{
						"admin": {
							{
								Domain:   authorization.SchemaDomain,
								Resource: "schema/collections/ABC/shards/*", // old
								Verb:     authorization.READ,
							},
						},
					},
				}),
			},
			setup: func(m *mocks.Controller) {
				m.On("RemovePermissions", "admin", mock.Anything).Return(nil)
				m.On("UpsertRolesPermissions", map[string][]authorization.Policy{
					"admin": {
						{
							Domain:   authorization.SchemaDomain,
							Resource: "schema/collections/ABC/shards/#", // new
							Verb:     authorization.READ,
						},
					},
				}).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "V1: Successfully update roles domain permissions",
			request: &cmd.ApplyRequest{
				SubCommand: mustMarshal(cmd.CreateRolesRequest{
					Version: cmd.RBACCommandPolicyVersionV1,
					Roles: map[string][]authorization.Policy{
						"admin": {
							{
								Domain: authorization.RolesDomain,
								Verb:   authorization.ROLE_SCOPE_ALL, // old
							},
						},
					},
				}),
			},
			setup: func(m *mocks.Controller) {
				m.On("RemovePermissions", "admin", mock.Anything).Return(nil)
				m.On("UpsertRolesPermissions", map[string][]authorization.Policy{
					"admin": {
						{
							Domain: authorization.RolesDomain,
							Verb:   authorization.ROLE_SCOPE_MATCH, // new
						},
					},
				}).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "VLatest: Successfully update roles domain permissions",
			request: &cmd.ApplyRequest{
				SubCommand: mustMarshal(cmd.CreateRolesRequest{
					Version: cmd.RBACLatestCommandPolicyVersion,
					Roles: map[string][]authorization.Policy{
						"admin": {
							{
								Domain: authorization.RolesDomain,
								Verb:   authorization.ROLE_SCOPE_ALL, // shall not be affected
							},
						},
					},
				}),
			},
			setup: func(m *mocks.Controller) {
				m.On("UpsertRolesPermissions", map[string][]authorization.Policy{
					"admin": {
						{
							Domain: authorization.RolesDomain,
							Verb:   authorization.ROLE_SCOPE_ALL, // new
						},
					},
				}).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "Invalid schema path",
			request: &cmd.ApplyRequest{
				SubCommand: mustMarshal(cmd.CreateRolesRequest{
					Version: cmd.RBACCommandPolicyVersionV0,
					Roles: map[string][]authorization.Policy{
						"admin": {
							{
								Domain:   authorization.SchemaDomain,
								Resource: "invalid/path",
								Verb:     authorization.READ,
							},
						},
					},
				}),
			},
			setup: func(m *mocks.Controller) {
				m.On("RemovePermissions", "admin", mock.Anything).Return(nil)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := mocks.NewController(t)
			if tt.setup != nil {
				tt.setup(controller)
			}

			manager := NewManager(controller, nil)
			err := manager.UpsertRolesPermissions(tt.request)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			controller.AssertExpectations(t)
		})
	}
}

func mustMarshal(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
