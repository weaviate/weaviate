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

package telemetry

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/config"
)

type nodesStatusGetter interface {
	LocalNodeStatus(className, output string) *models.NodeStatus
}

type modulesProvider interface {
	GetMeta() (map[string]interface{}, error)
}

// Telemeter is responsible for managing
type Telemeter struct {
	MachineID         strfmt.UUID `json:"machineId"`
	nodesStatusGetter nodesStatusGetter
	modulesProvider   modulesProvider
}

// New creates a new Telemetry instance
func New(nodesStatusGetter nodesStatusGetter, modulesProvider modulesProvider) *Telemeter {
	return &Telemeter{
		MachineID:         strfmt.UUID(uuid.NewString()),
		nodesStatusGetter: nodesStatusGetter,
		modulesProvider:   modulesProvider,
	}
}

// Push aggregates the telemetry metrics and sends them to transport layer
func (tel *Telemeter) Push(ctx context.Context, payloadType string) (*Payload, error) {
	mods, err := tel.getEnabledModules()
	if err != nil {
		return nil, fmt.Errorf("get enabled modules: %w", err)
	}
	objs, err := tel.getObjectCount()
	if err != nil {
		return nil, fmt.Errorf("get object count: %w", err)
	}
	return &Payload{
		MachineID:  tel.MachineID,
		Type:       payloadType,
		Version:    config.ServerVersion,
		Modules:    mods,
		NumObjects: objs,
	}, nil
}

func (tel *Telemeter) getEnabledModules() (string, error) {
	modMeta, err := tel.modulesProvider.GetMeta()
	if err != nil {
		return "", fmt.Errorf("meta from modules provider: %w", err)
	}
	if len(modMeta) == 0 {
		return "", nil
	}
	mods, i := make([]string, len(modMeta)), 0
	for name := range modMeta {
		mods[i], i = name, i+1
	}
	sort.Strings(mods)
	return strings.Join(mods, ","), nil
}

func (tel *Telemeter) getObjectCount() (int64, error) {
	status := tel.nodesStatusGetter.LocalNodeStatus("", verbosity.OutputMinimal)
	if status == nil || status.Stats == nil {
		return 0, fmt.Errorf("received nil node stats")
	}
	return status.Stats.ObjectCount, nil
}
