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

package dynusers

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
)

var ErrBadRequest = errors.New("bad request")

type Manager struct {
	dynUser apikey.DynamicUser
	logger  logrus.FieldLogger
}

func NewManager(dynUser apikey.DynamicUser, logger logrus.FieldLogger) *Manager {
	return &Manager{dynUser: dynUser, logger: logger}
}

func (m *Manager) CreateUser(c *cmd.ApplyRequest) error {
	if m.dynUser == nil {
		return nil
	}
	req := &cmd.CreateUsersRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.dynUser.CreateUser(req.UserId, req.SecureHash, req.UserIdentifier)
}

func (m *Manager) GetUsers(req *cmd.QueryRequest) ([]byte, error) {
	if m.dynUser == nil {
		payload, _ := json.Marshal(cmd.QueryGetUsersRequest{})
		return payload, nil
	}
	subCommand := cmd.QueryGetUsersRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	users, err := m.dynUser.GetUsers(subCommand.UserIds...)
	if err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	response := cmd.QueryGetUsersResponse{Users: users}
	payload, err := json.Marshal(response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}
