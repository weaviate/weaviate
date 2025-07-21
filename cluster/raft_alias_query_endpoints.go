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
	"encoding/json"
	"fmt"

	"github.com/getsentry/sentry-go"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	entSentry "github.com/weaviate/weaviate/entities/sentry"
)

func (s *Raft) GetAliases(ctx context.Context, alias string, class *models.Class) ([]*models.Alias, error) {
	if entSentry.Enabled() {
		transaction := sentry.StartSpan(ctx, "grpc.client",
			sentry.WithTransactionName("raft.query.aliases"),
			sentry.WithDescription("Query the aliases"),
		)
		ctx = transaction.Context()
		defer transaction.Finish()
	}
	req := cmd.QueryGetAliasesRequest{}
	if alias != "" {
		req.Alias = alias
	}
	if class != nil {
		req.Class = class.Class
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_ALIASES,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Unmarshal the response
	resp := cmd.QueryGetAliasesResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}
	aliases := []*models.Alias{}
	for alias, className := range resp.Aliases {
		aliases = append(aliases, &models.Alias{Alias: alias, Class: className})
	}
	return aliases, nil
}
