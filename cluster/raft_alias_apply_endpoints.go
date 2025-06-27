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

package cluster

import (
	"context"
	"fmt"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	gproto "google.golang.org/protobuf/proto"
)

func (s *Raft) CreateAlias(ctx context.Context, alias string, class *models.Class) (uint64, error) {
	if class == nil || class.Class == "" {
		return 0, fmt.Errorf("nil class or empty class name: %w", schema.ErrBadRequest)
	}
	if alias == "" {
		return 0, fmt.Errorf("empty alias name: %w", schema.ErrBadRequest)
	}

	req := cmd.CreateAliasRequest{Collection: class.Class, Alias: alias}
	subCommand, err := gproto.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_CREATE_ALIAS,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) ReplaceAlias(ctx context.Context, alias *models.Alias, newClass *models.Class) (uint64, error) {
	if alias == nil {
		return 0, fmt.Errorf("empty alias: %w", schema.ErrBadRequest)
	}
	if newClass == nil || newClass.Class == "" {
		return 0, fmt.Errorf("nil new class or empty class name: %w", schema.ErrBadRequest)
	}

	req := cmd.ReplaceAliasRequest{Collection: newClass.Class, Alias: alias.Alias}
	subCommand, err := gproto.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_REPLACE_ALIAS,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) DeleteAlias(ctx context.Context, alias string) (uint64, error) {
	if alias == "" {
		return 0, fmt.Errorf("empty alias name: %w", schema.ErrBadRequest)
	}

	req := cmd.DeleteAliasRequest{Alias: alias}
	subCommand, err := gproto.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DELETE_ALIAS,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}
