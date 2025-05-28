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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/config"
)

const (
	defaultConsumer = "aHR0cHM6Ly90ZWxlbWV0cnkud2Vhdmlh" +
		"dGUuaW8vd2VhdmlhdGUtdGVsZW1ldHJ5"
	defaultPushInterval = 24 * time.Hour
)

type nodesStatusGetter interface {
	LocalNodeStatus(ctx context.Context, className, shardName, output string) *models.NodeStatus
}

type schemaManager interface {
	GetSchemaSkipAuth() schema.Schema
}

// Telemeter is responsible for managing the transmission of telemetry data
type Telemeter struct {
	machineID         strfmt.UUID
	nodesStatusGetter nodesStatusGetter
	schemaManager     schemaManager
	logger            logrus.FieldLogger
	shutdown          chan struct{}
	failedToStart     bool
	consumer          string
	pushInterval      time.Duration
}

// New creates a new Telemeter instance
func New(nodesStatusGetter nodesStatusGetter, schemaManager schemaManager,
	logger logrus.FieldLogger,
) *Telemeter {
	tel := &Telemeter{
		machineID:         strfmt.UUID(uuid.NewString()),
		nodesStatusGetter: nodesStatusGetter,
		schemaManager:     schemaManager,
		logger:            logger,
		shutdown:          make(chan struct{}),
		consumer:          defaultConsumer,
		pushInterval:      defaultPushInterval,
	}
	return tel
}

// Start begins telemetry for the node
func (tel *Telemeter) Start(ctx context.Context) error {
	payload, err := tel.push(ctx, PayloadType.Init)
	if err != nil {
		tel.failedToStart = true
		return fmt.Errorf("push: %w", err)
	}
	f := func() {
		t := time.NewTicker(tel.pushInterval)
		defer t.Stop()
		for {
			select {
			case <-tel.shutdown:
				return
			case <-t.C:
				payload, err = tel.push(ctx, PayloadType.Update)
				if err != nil {
					tel.logger.
						WithField("action", "telemetry_push").
						WithField("payload", fmt.Sprintf("%+v", payload)).
						WithField("retry_at", time.Now().Add(tel.pushInterval).Format(time.RFC3339)).
						Error(err.Error())
					continue
				}
				tel.logger.
					WithField("action", "telemetry_push").
					WithField("payload", fmt.Sprintf("%+v", payload)).
					Info("telemetry update")
			}
		}
	}
	enterrors.GoWrapper(f, tel.logger)

	tel.logger.
		WithField("action", "telemetry_push").
		WithField("payload", fmt.Sprintf("%+v", payload)).
		Info("telemetry started")
	return nil
}

// Stop shuts down the telemeter
func (tel *Telemeter) Stop(ctx context.Context) error {
	if tel.failedToStart {
		return nil
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("shutdown telemetry: %w", ctx.Err())
	case tel.shutdown <- struct{}{}:
		payload, err := tel.push(ctx, PayloadType.Terminate)
		if err != nil {
			tel.logger.
				WithField("action", "telemetry_push").
				WithField("payload", fmt.Sprintf("%+v", payload)).
				Error(err.Error())
			return err
		}
		tel.logger.
			WithField("action", "telemetry_push").
			WithField("payload", fmt.Sprintf("%+v", payload)).
			Info("telemetry terminated")
		return nil
	}
}

// push sends telemetry data to the consumer url
func (tel *Telemeter) push(ctx context.Context, payloadType string) (*Payload, error) {
	payload, err := tel.buildPayload(ctx, payloadType)
	if err != nil {
		return nil, fmt.Errorf("build payload: %w", err)
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	url, err := base64.StdEncoding.DecodeString(tel.consumer)
	if err != nil {
		return nil, fmt.Errorf("decode url: %w", err)
	}

	resp, err := http.Post(string(url), "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("request unsuccessful, status code: %d, body: %s", resp.StatusCode, string(body))
	}
	return payload, nil
}

func (tel *Telemeter) buildPayload(ctx context.Context, payloadType string) (*Payload, error) {
	usedMods, err := tel.getUsedModules()
	if err != nil {
		return nil, fmt.Errorf("get used modules: %w", err)
	}

	var objs int64
	// The first payload should not include object count,
	// because all the shards may not be loaded yet. We
	// don't want to force load for telemetry alone
	if payloadType != PayloadType.Init {
		objs, err = tel.getObjectCount(ctx)
		if err != nil {
			return nil, fmt.Errorf("get object count: %w", err)
		}
	}

	cols, err := tel.getCollectionsCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("get collections count: %w", err)
	}

	return &Payload{
		MachineID:        tel.machineID,
		Type:             payloadType,
		Version:          config.ServerVersion,
		ObjectsCount:     objs,
		OS:               runtime.GOOS,
		Arch:             runtime.GOARCH,
		UsedModules:      usedMods,
		CollectionsCount: cols,
	}, nil
}

func (tel *Telemeter) getUsedModules() ([]string, error) {
	sch := tel.schemaManager.GetSchemaSkipAuth()
	usedModulesMap := map[string]struct{}{}

	if sch.Objects != nil {
		for _, class := range sch.Objects.Classes {
			if modCfg, ok := class.ModuleConfig.(map[string]interface{}); ok {
				for name, cfg := range modCfg {
					usedModulesMap[tel.determineModule(name, cfg)] = struct{}{}
				}
			}
			for _, vectorConfig := range class.VectorConfig {
				if modCfg, ok := vectorConfig.Vectorizer.(map[string]interface{}); ok {
					for name, cfg := range modCfg {
						usedModulesMap[tel.determineModule(name, cfg)] = struct{}{}
					}
				}
			}
		}
	}

	var usedModules []string
	for modName := range usedModulesMap {
		usedModules = append(usedModules, modName)
	}
	sort.Strings(usedModules)
	return usedModules, nil
}

func (tel *Telemeter) determineModule(name string, cfg interface{}) string {
	if strings.Contains(name, "palm") || strings.Contains(name, "google") {
		if settings, ok := cfg.(map[string]interface{}); ok {
			if apiEndpoint, ok := settings["apiEndpoint"]; ok {
				if apiEndpointStr, ok := apiEndpoint.(string); ok && apiEndpointStr == "generativelanguage.googleapis.com" {
					return fmt.Sprintf("%s-ai-studio", strings.Replace(name, "palm", "google", 1))
				}
			}
		}
		return fmt.Sprintf("%s-vertex-ai", strings.Replace(name, "palm", "google", 1))
	}
	return name
}

func (tel *Telemeter) getObjectCount(ctx context.Context) (int64, error) {
	status := tel.nodesStatusGetter.LocalNodeStatus(ctx, "", "", verbosity.OutputVerbose)
	if status == nil || status.Stats == nil {
		return 0, fmt.Errorf("received nil node stats")
	}
	return status.Stats.ObjectCount, nil
}

func (tel *Telemeter) getCollectionsCount(context.Context) (int, error) {
	sch := tel.schemaManager.GetSchemaSkipAuth()
	if sch.Objects == nil {
		return 0, nil
	}
	return len(sch.Objects.Classes), nil
}
