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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/config"
)

const (
	consumerURL  = "https://us-central1-semi-production.cloudfunctions.net/weaviate-telemetry"
	pushInterval = 2*time.Hour + 30*time.Minute
)

type nodesStatusGetter interface {
	LocalNodeStatus(className, output string) *models.NodeStatus
}

type modulesProvider interface {
	GetMeta() (map[string]interface{}, error)
}

// Telemeter is responsible for managing
type Telemeter struct {
	MachineID         strfmt.UUID
	nodesStatusGetter nodesStatusGetter
	modulesProvider   modulesProvider
	logger            logrus.FieldLogger
	shutdown          chan struct{}
}

// New creates a new Telemetry instance
func New(nodesStatusGetter nodesStatusGetter, modulesProvider modulesProvider,
	logger logrus.FieldLogger,
) *Telemeter {
	return &Telemeter{
		MachineID:         strfmt.UUID(uuid.NewString()),
		nodesStatusGetter: nodesStatusGetter,
		modulesProvider:   modulesProvider,
		logger:            logger,
		shutdown:          make(chan struct{}),
	}
}

// Start begins telemetry for the node
func (tel *Telemeter) Start(ctx context.Context) error {
	if err := tel.Push(ctx, PayloadType.Init); err != nil {
		return fmt.Errorf("push: %w", err)
	}
	go func() {
		t := time.NewTicker(pushInterval)
		for {
			select {
			case <-tel.shutdown:
				if err := tel.Push(ctx, PayloadType.Terminate); err != nil {
					tel.logger.
						WithField("action", "telemetry_push").
						WithField("type", PayloadType.Terminate).
						WithField("machine_id", tel.MachineID).
						Error(err.Error())
				}
				return
			case <-t.C:
				if err := tel.Push(ctx, PayloadType.Update); err != nil {
					tel.logger.
						WithField("action", "telemetry_push").
						WithField("type", PayloadType.Update).
						WithField("machine_id", tel.MachineID).
						WithField("retry_at", time.Now().Add(pushInterval).Format(time.RFC3339)).
						Error(err.Error())
				}
			}
		}
	}()

	tel.logger.
		WithField("action", "telemetry_start").
		WithField("machine_id", tel.MachineID).
		Info("telemetry started")
	return nil
}

// Stop shuts down the telemeter
func (tel *Telemeter) Stop(ctx context.Context) error {
	waitForShutdown := make(chan struct{})
	go func() {
		tel.shutdown <- struct{}{}
		waitForShutdown <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("shutdown telemetry: %w", ctx.Err())
	case <-waitForShutdown:
		tel.logger.
			WithField("action", "shutdown").
			WithField("machine_id", tel.MachineID).
			Info("telemetry stopped")
		return nil
	}
}

// Push sends telemetry data to the consumer url
func (tel *Telemeter) Push(ctx context.Context, payloadType string) error {
	payload, err := tel.buildPayload(ctx, payloadType)
	if err != nil {
		return fmt.Errorf("build payload: %w", err)
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	resp, err := http.Post(consumerURL, "application/json", bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request unsuccessful, status code: %d, body: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (tel *Telemeter) buildPayload(ctx context.Context, payloadType string) (*Payload, error) {
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
