//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"encoding/json"
	"fmt"
	"slices"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// ReindexFlip needs no shard work: every tenant already serves the generation it makes live.
func (s *SchemaManager) ReindexFlip(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := command.ReindexFlipRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	return s.apply(applyOp{
		op:           cmd.GetType().String(),
		updateSchema: func() error { return s.schema.applyReindexFlip(cmd.Class, cmd.Version, &req) },
		updateStore:  func() error { return nil },
		schemaOnly:   schemaOnly,
	})
}

func (s *schema) applyReindexFlip(class string, v uint64, req *command.ReindexFlipRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta := s.unsafeResolveClass(class)
	if meta == nil {
		return ErrClassNotFound
	}
	meta.Lock()
	defer meta.Unlock()
	live, changed, err := applyReindexFlip(&meta.Class, meta.Live, req)
	if err != nil || !changed {
		return err
	}
	meta.Live, meta.ClassVersion = live, v
	return nil
}

// applyReindexFlip replaces the property, since readers may hold the old one.
// Every node proposes the flip, so one already applied changes nothing.
func applyReindexFlip(class *models.Class, cur map[string]map[string]uint64, req *command.ReindexFlipRequest,
) (map[string]map[string]uint64, bool, error) {
	i := slices.IndexFunc(class.Properties, func(p *models.Property) bool { return p.Name == req.Prop })
	if i < 0 {
		return nil, false, fmt.Errorf("%w: flip: property %q not found", ErrBadRequest, req.Prop)
	}
	if !slices.ContainsFunc(req.IndexTypes, func(t string) bool { return cur[req.Prop][t] < req.Gen }) {
		return cur, false, nil
	}
	p, on := *class.Properties[i], true
	for _, t := range req.IndexTypes {
		switch t {
		case command.ReindexFilterable:
			if req.Enable {
				p.IndexFilterable = &on
			}
		case command.ReindexSearchable:
			if req.Enable {
				p.IndexSearchable = &on
			}
			if req.Blockmax {
				p.SearchableBlockmax = &on
			}
		case command.ReindexRangeable:
			if req.Enable {
				p.IndexRangeFilters = &on
			}
		default:
			return nil, false, fmt.Errorf("%w: flip: unknown index type %q", ErrBadRequest, t)
		}
	}
	if req.Tokenization != "" {
		p.Tokenization = req.Tokenization
	}
	live := cloneLive(cur)
	if live == nil {
		live = map[string]map[string]uint64{}
	}
	if live[req.Prop] == nil {
		live[req.Prop] = map[string]uint64{}
	}
	for _, t := range req.IndexTypes {
		live[req.Prop][t] = req.Gen
	}
	props := slices.Clone(class.Properties)
	props[i] = &p
	class.Properties = props
	if req.Blockmax && allSearchableStamped(props) {
		cfg := models.InvertedIndexConfig{}
		if class.InvertedIndexConfig != nil {
			cfg = *class.InvertedIndexConfig
		}
		cfg.UsingBlockMaxWAND = true
		class.InvertedIndexConfig = &cfg
	}
	return live, true, nil
}

func allSearchableStamped(props []*models.Property) bool {
	return !slices.ContainsFunc(props, func(p *models.Property) bool {
		return searchparams.HasSearchableIndex(p) && (p.SearchableBlockmax == nil || !*p.SearchableBlockmax)
	})
}
