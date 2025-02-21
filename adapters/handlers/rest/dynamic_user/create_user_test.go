package dynamic_user

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/mocks"
	authzMocks "github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestBadRequest(t *testing.T) {
	principal := &models.Principal{}
	tests := []struct {
		name   string
		userId string
	}{
		{name: "too long", userId: strings.Repeat("A", 100)},
		{name: "invalid characters", userId: "#a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authzMocks.NewAuthorizer(t)
			dynUser := mocks.NewDynamicUser(t)

			h := dynUserHandler{
				dynamicUser: dynUser,
				authorizer:  authorizer,
			}

			res := h.createUser(users.CreateUserParams{UserID: tt.userId}, principal)
			parsed, ok := res.(*users.CreateUserBadRequest)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestInternalServerError(t *testing.T) {
	principal := &models.Principal{}
	tests := []struct {
		name             string
		GetUserReturn    error
		CreateUserReturn error
	}{
		{name: "get user error", GetUserReturn: errors.New("some error")},
		{name: "create user error", GetUserReturn: nil, CreateUserReturn: errors.New("some error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authzMocks.NewAuthorizer(t)
			dynUser := mocks.NewDynamicUser(t)
			dynUser.On("GetUsers", "user").Return(nil, tt.GetUserReturn)
			if tt.GetUserReturn == nil {
				dynUser.On("CreateUser", "user", mock.Anything, mock.Anything).Return(tt.CreateUserReturn)
			}

			h := dynUserHandler{
				dynamicUser: dynUser,
				authorizer:  authorizer,
			}

			res := h.createUser(users.CreateUserParams{UserID: "user"}, principal)
			parsed, ok := res.(*users.CreateUserInternalServerError)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestConflict(t *testing.T) {
	principal := &models.Principal{}

	authorizer := authzMocks.NewAuthorizer(t)
	dynUser := mocks.NewDynamicUser(t)
	dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{"user": {}}, nil)

	h := dynUserHandler{
		dynamicUser: dynUser,
		authorizer:  authorizer,
	}

	res := h.createUser(users.CreateUserParams{UserID: "user"}, principal)
	parsed, ok := res.(*users.CreateUserConflict)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestSuccess(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	dynUser := mocks.NewDynamicUser(t)
	dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{}, nil)
	dynUser.On("CreateUser", "user", mock.Anything, mock.Anything).Return(nil)

	h := dynUserHandler{
		dynamicUser: dynUser,
		authorizer:  authorizer,
	}

	res := h.createUser(users.CreateUserParams{UserID: "user"}, principal)
	parsed, ok := res.(*users.CreateUserCreated)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}
