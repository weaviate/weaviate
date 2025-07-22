//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"encoding/json"
	"fmt"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	gproto "google.golang.org/protobuf/proto"
)

func (s *SchemaManager) CreateAlias(cmd *command.ApplyRequest) error {
	req := &command.CreateAliasRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return s.apply(
		applyOp{
			op:                   cmd.GetType().String(),
			updateSchema:         func() error { return s.schema.createAlias(req.Collection, req.Alias) },
			updateStore:          func() error { return nil /* nothing do to here */ },
			enableSchemaCallback: true,
		},
	)
}

func (s *SchemaManager) ReplaceAlias(cmd *command.ApplyRequest) error {
	req := &command.ReplaceAliasRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return s.apply(
		applyOp{
			op:                   cmd.GetType().String(),
			updateSchema:         func() error { return s.schema.replaceAlias(req.Collection, req.Alias) },
			updateStore:          func() error { return nil /* nothing do to here */ },
			enableSchemaCallback: true,
		},
	)
}

func (s *SchemaManager) DeleteAlias(cmd *command.ApplyRequest) error {
	req := &command.DeleteAliasRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return s.apply(
		applyOp{
			op:                   cmd.GetType().String(),
			updateSchema:         func() error { return s.schema.deleteAlias(req.Alias) },
			updateStore:          func() error { return nil /* nothing do to here */ },
			enableSchemaCallback: true,
		},
	)
}

func (s *SchemaManager) ResolveAlias(req *command.QueryRequest) ([]byte, error) {
	subCommand := command.QueryResolveAliasRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	rootClass := s.schema.ResolveAlias(subCommand.Alias)
	if rootClass == "" {
		return nil, fmt.Errorf("resolve alias: alias %q not found", subCommand.Alias)
	}

	response := command.QueryResolveAliasResponse{
		Class: rootClass,
	}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal resolve alias response: %w", err)
	}
	return payload, nil
}

func (s *SchemaManager) GetAliases(req *command.QueryRequest) ([]byte, error) {
	subCommand := command.QueryGetAliasesRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	response := command.QueryGetAliasesResponse{
		Aliases: s.schema.getAliases(subCommand.Alias, subCommand.Class),
	}
	payload, err := json.Marshal(&response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal get aliases response: %w", err)
	}
	return payload, nil
}
